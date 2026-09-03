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

void bind_f05_repeated_boolean_cooldown(py::module_& m) {
    m.attr("F05_REPEATED_BOOLEAN_COOLDOWN_ABI_VERSION") =
        std::string(kF05RepeatedBooleanCooldownAbi);

    py::enum_<F05TriState>(m, "F05TriState")
        .value("UNOBSERVED", F05TriState::Unobserved)
        .value("FALSE", F05TriState::False)
        .value("TRUE", F05TriState::True);
    py::enum_<F05CooldownFillRole>(m, "F05CooldownFillRole")
        .value("OPENER", F05CooldownFillRole::Opener)
        .value("ADD", F05CooldownFillRole::Add)
        .value("REDUCING", F05CooldownFillRole::Reducing);
    py::enum_<F05PredicateMetric>(m, "F05PredicateMetric")
        .value("CAMPAIGN_AGE_GT_CONTROL", F05PredicateMetric::CampaignAgeGtControl)
        .value("POSITIVE_ORDERING", F05PredicateMetric::PositiveOrdering)
        .value("LAST_CROSS_POSITIVE", F05PredicateMetric::LastCrossPositive)
        .value("EXPANDING", F05PredicateMetric::Expanding)
        .value("CONVERGING", F05PredicateMetric::Converging)
        .value("ABS_DISTANCE", F05PredicateMetric::AbsDistance)
        .value("CROSS_AGE_S", F05PredicateMetric::CrossAgeS)
        .value(
            "ARRANGEMENT_PERSISTENCE_S",
            F05PredicateMetric::ArrangementPersistenceS)
        .value("SIGNED_DISTANCE", F05PredicateMetric::SignedDistance)
        .value(
            "SIGNED_DISTANCE_VELOCITY",
            F05PredicateMetric::SignedDistanceVelocity)
        .value(
            "SIGNED_DISTANCE_ACCELERATION",
            F05PredicateMetric::SignedDistanceAcceleration);

    py::class_<F05BooleanLiteral>(m, "F05BooleanLiteral")
        .def(py::init<>())
        .def_readwrite("predicate_index", &F05BooleanLiteral::predicate_index)
        .def_readwrite("negated", &F05BooleanLiteral::negated);
    py::class_<F05BooleanClause>(m, "F05BooleanClause")
        .def(py::init<>())
        .def_readwrite("literals", &F05BooleanClause::literals);
    py::class_<F05BooleanRule>(m, "F05BooleanRule")
        .def(py::init<>())
        .def_readwrite("action_id", &F05BooleanRule::action_id)
        .def_readwrite("duration_ms", &F05BooleanRule::duration_ms)
        .def_readwrite("clauses", &F05BooleanRule::clauses);
    py::class_<F05PredicatePair>(m, "F05PredicatePair")
        .def(py::init<>())
        .def_readwrite("fast_ema_index", &F05PredicatePair::fast_ema_index)
        .def_readwrite("slow_ema_index", &F05PredicatePair::slow_ema_index);
    py::class_<F05PredicateDefinition>(m, "F05PredicateDefinition")
        .def(py::init<>())
        .def_readwrite(
            "predicate_index", &F05PredicateDefinition::predicate_index)
        .def_readwrite("metric", &F05PredicateDefinition::metric)
        .def_readwrite("pair_index", &F05PredicateDefinition::pair_index)
        .def_readwrite(
            "threshold_enabled", &F05PredicateDefinition::threshold_enabled)
        .def_readwrite("threshold", &F05PredicateDefinition::threshold);
    py::class_<F05BooleanPolicy>(m, "F05BooleanPolicy")
        .def(py::init<>())
        .def_readwrite("policy_sha256", &F05BooleanPolicy::policy_sha256)
        .def_readwrite(
            "predicate_bundle_sha256",
            &F05BooleanPolicy::predicate_bundle_sha256)
        .def_readwrite(
            "predicate_columns",
            &F05BooleanPolicy::predicate_columns)
        .def_readwrite("rules", &F05BooleanPolicy::rules)
        .def_readwrite(
            "ema_half_lives_s", &F05BooleanPolicy::ema_half_lives_s)
        .def_readwrite(
            "predicate_pairs", &F05BooleanPolicy::predicate_pairs)
        .def_readwrite(
            "predicate_definitions", &F05BooleanPolicy::predicate_definitions)
        .def_readwrite("default_action", &F05BooleanPolicy::default_action);
    py::class_<F05RepeatedBooleanCooldownConfig>(
        m,
        "F05RepeatedBooleanCooldownConfig")
        .def(py::init<>())
        .def_readwrite(
            "parity_qualified",
            &F05RepeatedBooleanCooldownConfig::parity_qualified)
        .def_readwrite(
            "qualification_under_test",
            &F05RepeatedBooleanCooldownConfig::qualification_under_test)
        .def_readwrite(
            "parity_qualification_sha256",
            &F05RepeatedBooleanCooldownConfig::parity_qualification_sha256)
        .def_readwrite(
            "qualification_scope",
            &F05RepeatedBooleanCooldownConfig::qualification_scope)
        .def_readwrite(
            "feature_clock_semantics",
            &F05RepeatedBooleanCooldownConfig::feature_clock_semantics)
        .def_readwrite("warmup_s", &F05RepeatedBooleanCooldownConfig::warmup_s)
        .def_readwrite(
            "max_feature_age_s",
            &F05RepeatedBooleanCooldownConfig::max_feature_age_s)
        .def_readwrite("policy", &F05RepeatedBooleanCooldownConfig::policy)
        .def_readwrite(
            "buy_policy", &F05RepeatedBooleanCooldownConfig::buy_policy);
    py::class_<F05CooldownWindowObservation>(
        m,
        "F05CooldownWindowObservation")
        .def(py::init<>())
        .def_readwrite("left_ts_ns", &F05CooldownWindowObservation::left_ts_ns)
        .def_readwrite(
            "right_ts_ns",
            &F05CooldownWindowObservation::right_ts_ns)
        .def_readwrite(
            "feature_ready_ts_ns",
            &F05CooldownWindowObservation::feature_ready_ts_ns)
        .def_readwrite(
            "market_generation",
            &F05CooldownWindowObservation::market_generation)
        .def_readwrite(
            "depth_generation",
            &F05CooldownWindowObservation::depth_generation)
        .def_readwrite(
            "mid_usdc_per_btc",
            &F05CooldownWindowObservation::mid_usdc_per_btc)
        .def_readwrite(
            "reset_feature_state",
            &F05CooldownWindowObservation::reset_feature_state)
        .def_readwrite("source_gap", &F05CooldownWindowObservation::source_gap)
        .def_readwrite(
            "source_stale",
            &F05CooldownWindowObservation::source_stale)
        .def_readwrite(
            "warmup_admitted",
            &F05CooldownWindowObservation::warmup_admitted)
        .def_readwrite(
            "channel_support_valid",
            &F05CooldownWindowObservation::channel_support_valid);
    py::class_<F05CooldownWindowTape, std::shared_ptr<F05CooldownWindowTape>>(
        m,
        "F05CooldownWindowTape")
        .def(py::init<>())
        .def_readwrite(
            "content_sha256",
            &F05CooldownWindowTape::content_sha256)
        .def_property_readonly(
            "size",
            [](const F05CooldownWindowTape& self) {
              return self.observations.size();
            });
    py::class_<F05CooldownFillInput>(m, "F05CooldownFillInput")
        .def(py::init<>())
        .def_readwrite("snapshot_id", &F05CooldownFillInput::snapshot_id)
        .def_readwrite("side", &F05CooldownFillInput::side)
        .def_readwrite("role", &F05CooldownFillInput::role)
        .def_readwrite("fill_ts_ms", &F05CooldownFillInput::fill_ts_ms)
        .def_readwrite("decision_ts_ns", &F05CooldownFillInput::decision_ts_ns)
        .def_readwrite("campaign_id", &F05CooldownFillInput::campaign_id)
        .def_readwrite("campaign_age_s", &F05CooldownFillInput::campaign_age_s)
        .def_readwrite(
            "inventory_before_fill_btc",
            &F05CooldownFillInput::inventory_before_fill_btc)
        .def_readwrite(
            "inventory_after_fill_btc",
            &F05CooldownFillInput::inventory_after_fill_btc)
        .def_readwrite(
            "consecutive_units_after",
            &F05CooldownFillInput::consecutive_units_after)
        .def_readwrite(
            "baseline_duration_ms",
            &F05CooldownFillInput::baseline_duration_ms)
        .def_readwrite(
            "policy_input_valid",
            &F05CooldownFillInput::policy_input_valid)
        .def_readwrite("support_valid", &F05CooldownFillInput::support_valid)
        .def_readwrite(
            "channel_support_valid",
            &F05CooldownFillInput::channel_support_valid)
        .def_readwrite(
            "snapshot_fallback_reason",
            &F05CooldownFillInput::snapshot_fallback_reason)
        .def_readwrite(
            "predicate_values",
            &F05CooldownFillInput::predicate_values);
    py::class_<F05CooldownPredicateRow>(m, "F05CooldownPredicateRow")
        .def(py::init<>())
        .def_readwrite(
            "exposure_fill_ordinal",
            &F05CooldownPredicateRow::exposure_fill_ordinal)
        .def_readwrite("fill_ts_ms", &F05CooldownPredicateRow::fill_ts_ms)
        .def_readwrite("side", &F05CooldownPredicateRow::side)
        .def_readwrite("campaign_id", &F05CooldownPredicateRow::campaign_id)
        .def_readwrite("snapshot_id", &F05CooldownPredicateRow::snapshot_id)
        .def_readwrite(
            "policy_input_valid",
            &F05CooldownPredicateRow::policy_input_valid)
        .def_readwrite("support_valid", &F05CooldownPredicateRow::support_valid)
        .def_readwrite(
            "channel_support_valid",
            &F05CooldownPredicateRow::channel_support_valid)
        .def_readwrite(
            "snapshot_fallback_reason",
            &F05CooldownPredicateRow::snapshot_fallback_reason)
        .def_readwrite(
            "predicate_values",
            &F05CooldownPredicateRow::predicate_values);
    m.def(
        "validate_f05_cooldown_predicate_rows",
        &validate_f05_cooldown_predicate_rows,
        py::arg("config"),
        py::arg("rows"));
    py::class_<F05CooldownDecision>(m, "F05CooldownDecision")
        .def(py::init<>())
        .def_readwrite("snapshot_id", &F05CooldownDecision::snapshot_id)
        .def_readwrite("side", &F05CooldownDecision::side)
        .def_readwrite("role", &F05CooldownDecision::role)
        .def_readwrite(
            "exposure_fill_ordinal",
            &F05CooldownDecision::exposure_fill_ordinal)
        .def_readwrite("fill_ts_ms", &F05CooldownDecision::fill_ts_ms)
        .def_readwrite("campaign_id", &F05CooldownDecision::campaign_id)
        .def_readwrite(
            "consecutive_units_after",
            &F05CooldownDecision::consecutive_units_after)
        .def_readwrite("action_id", &F05CooldownDecision::action_id)
        .def_readwrite(
            "baseline_duration_ms",
            &F05CooldownDecision::baseline_duration_ms)
        .def_readwrite("duration_ms", &F05CooldownDecision::duration_ms)
        .def_readwrite("deadline_ts_ms", &F05CooldownDecision::deadline_ts_ms)
        .def_readwrite(
            "lineage_revision",
            &F05CooldownDecision::lineage_revision)
        .def_readwrite(
            "matched_rule_index",
            &F05CooldownDecision::matched_rule_index)
        .def_readwrite("support_valid", &F05CooldownDecision::support_valid)
        .def_readwrite("lineage_applied", &F05CooldownDecision::lineage_applied)
        .def_readwrite(
            "coverage_reason_code",
            &F05CooldownDecision::coverage_reason_code)
        .def_readwrite("fallback_reason", &F05CooldownDecision::fallback_reason)
        .def_readwrite("policy_sha256", &F05CooldownDecision::policy_sha256)
        .def_readwrite(
            "predicate_bundle_sha256",
            &F05CooldownDecision::predicate_bundle_sha256)
        .def_readwrite(
            "feature_ready_ts_ns",
            &F05CooldownDecision::feature_ready_ts_ns)
        .def_readwrite("feature_age_ms", &F05CooldownDecision::feature_age_ms);
    py::class_<F05CooldownPairState>(m, "F05CooldownPairState")
        .def(py::init<>())
        .def_readwrite("effective_sign", &F05CooldownPairState::effective_sign)
        .def_readwrite(
            "arrangement_start_ts_ns",
            &F05CooldownPairState::arrangement_start_ts_ns)
        .def_readwrite(
            "last_cross_ts_ns",
            &F05CooldownPairState::last_cross_ts_ns)
        .def_readwrite(
            "last_cross_direction",
            &F05CooldownPairState::last_cross_direction);
    py::class_<F05CooldownLineageState>(m, "F05CooldownLineageState")
        .def(py::init<>())
        .def_readwrite("active", &F05CooldownLineageState::active)
        .def_readwrite("side", &F05CooldownLineageState::side)
        .def_readwrite("revision", &F05CooldownLineageState::revision)
        .def_readwrite("campaign_id", &F05CooldownLineageState::campaign_id)
        .def_readwrite("fill_ts_ms", &F05CooldownLineageState::fill_ts_ms)
        .def_readwrite("deadline_ts_ms", &F05CooldownLineageState::deadline_ts_ms)
        .def_readwrite(
            "consecutive_units_after",
            &F05CooldownLineageState::consecutive_units_after)
        .def_readwrite("duration_ms", &F05CooldownLineageState::duration_ms)
        .def_readwrite("action_id", &F05CooldownLineageState::action_id)
        .def_readwrite(
            "coverage_reason_code",
            &F05CooldownLineageState::coverage_reason_code);
    py::class_<F05CooldownRuntimeAudit>(m, "F05CooldownRuntimeAudit")
        .def(py::init<>())
        .def_readwrite("window_count", &F05CooldownRuntimeAudit::window_count)
        .def_readwrite(
            "gap_window_count",
            &F05CooldownRuntimeAudit::gap_window_count)
        .def_readwrite(
            "feature_state_reset_count",
            &F05CooldownRuntimeAudit::feature_state_reset_count)
        .def_readwrite(
            "evaluation_count",
            &F05CooldownRuntimeAudit::evaluation_count)
        .def_readwrite(
            "supported_count",
            &F05CooldownRuntimeAudit::supported_count)
        .def_readwrite("fallback_count", &F05CooldownRuntimeAudit::fallback_count)
        .def_readwrite(
            "nonbaseline_count",
            &F05CooldownRuntimeAudit::nonbaseline_count)
        .def_readwrite(
            "buy_control_count",
            &F05CooldownRuntimeAudit::buy_control_count)
        .def_readwrite(
            "reducing_bypass_count",
            &F05CooldownRuntimeAudit::reducing_bypass_count)
        .def_readwrite("lineage_count", &F05CooldownRuntimeAudit::lineage_count)
        .def_readwrite(
            "lineage_clear_count",
            &F05CooldownRuntimeAudit::lineage_clear_count);
    py::class_<F05RepeatedBooleanCooldownCheckpoint>(
        m,
        "F05RepeatedBooleanCooldownCheckpoint")
        .def(py::init<>())
        .def_readwrite("abi_version", &F05RepeatedBooleanCooldownCheckpoint::abi_version)
        .def_readwrite(
            "qualification_under_test",
            &F05RepeatedBooleanCooldownCheckpoint::qualification_under_test)
        .def_readwrite(
            "parity_qualification_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::parity_qualification_sha256)
        .def_readwrite(
            "qualification_scope",
            &F05RepeatedBooleanCooldownCheckpoint::qualification_scope)
        .def_readwrite(
            "feature_clock_semantics",
            &F05RepeatedBooleanCooldownCheckpoint::feature_clock_semantics)
        .def_readwrite(
            "policy_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::policy_sha256)
        .def_readwrite(
            "predicate_bundle_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::predicate_bundle_sha256)
        .def_readwrite(
            "buy_policy_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::buy_policy_sha256)
        .def_readwrite(
            "buy_predicate_bundle_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::buy_predicate_bundle_sha256)
        .def_readwrite("warmup_s", &F05RepeatedBooleanCooldownCheckpoint::warmup_s)
        .def_readwrite(
            "max_feature_age_s",
            &F05RepeatedBooleanCooldownCheckpoint::max_feature_age_s)
        .def_readwrite(
            "warmup_admitted",
            &F05RepeatedBooleanCooldownCheckpoint::warmup_admitted)
        .def_readwrite(
            "warmup_start_right_ts_ns",
            &F05RepeatedBooleanCooldownCheckpoint::warmup_start_right_ts_ns)
        .def_readwrite(
            "last_right_ts_ns",
            &F05RepeatedBooleanCooldownCheckpoint::last_right_ts_ns)
        .def_readwrite(
            "last_input_ready_ts_ns",
            &F05RepeatedBooleanCooldownCheckpoint::last_input_ready_ts_ns)
        .def_readwrite(
            "last_feature_ready_ts_ns",
            &F05RepeatedBooleanCooldownCheckpoint::last_feature_ready_ts_ns)
        .def_readwrite(
            "last_market_generation",
            &F05RepeatedBooleanCooldownCheckpoint::last_market_generation)
        .def_readwrite(
            "last_depth_generation",
            &F05RepeatedBooleanCooldownCheckpoint::last_depth_generation)
        .def_readwrite(
            "ema_initialized",
            &F05RepeatedBooleanCooldownCheckpoint::ema_initialized)
        .def_readwrite(
            "buy_ema_initialized",
            &F05RepeatedBooleanCooldownCheckpoint::buy_ema_initialized)
        .def_readwrite(
            "current_window_observed",
            &F05RepeatedBooleanCooldownCheckpoint::current_window_observed)
        .def_readwrite(
            "current_channel_support_valid",
            &F05RepeatedBooleanCooldownCheckpoint::current_channel_support_valid)
        .def_readwrite(
            "last_observed_ts_ns",
            &F05RepeatedBooleanCooldownCheckpoint::last_observed_ts_ns)
        .def_readwrite("ema", &F05RepeatedBooleanCooldownCheckpoint::ema)
        .def_readwrite("buy_ema", &F05RepeatedBooleanCooldownCheckpoint::buy_ema)
        .def_readwrite(
            "buy_velocity", &F05RepeatedBooleanCooldownCheckpoint::buy_velocity)
        .def_readwrite(
            "buy_acceleration",
            &F05RepeatedBooleanCooldownCheckpoint::buy_acceleration)
        .def_readwrite(
            "buy_pairs", &F05RepeatedBooleanCooldownCheckpoint::buy_pairs)
        .def_readwrite(
            "short_pair",
            &F05RepeatedBooleanCooldownCheckpoint::short_pair)
        .def_readwrite("long_pair", &F05RepeatedBooleanCooldownCheckpoint::long_pair)
        .def_readwrite(
            "buy_lineage",
            &F05RepeatedBooleanCooldownCheckpoint::buy_lineage)
        .def_readwrite(
            "sell_lineage",
            &F05RepeatedBooleanCooldownCheckpoint::sell_lineage)
        .def_readwrite("audit", &F05RepeatedBooleanCooldownCheckpoint::audit)
        .def_readwrite(
            "canonical_payload",
            &F05RepeatedBooleanCooldownCheckpoint::canonical_payload)
        .def_readwrite(
            "checkpoint_sha256",
            &F05RepeatedBooleanCooldownCheckpoint::checkpoint_sha256);
    py::class_<
        F05RepeatedBooleanCooldownRuntime,
        std::shared_ptr<F05RepeatedBooleanCooldownRuntime>>(
        m,
        "F05RepeatedBooleanCooldownRuntime")
        .def(py::init<F05RepeatedBooleanCooldownConfig>())
        .def("update_window", &F05RepeatedBooleanCooldownRuntime::update_window)
        .def("apply_fill", &F05RepeatedBooleanCooldownRuntime::apply_fill)
        .def(
            "override_active_lineage_duration",
            &F05RepeatedBooleanCooldownRuntime::override_active_lineage_duration)
        .def("advance_time", &F05RepeatedBooleanCooldownRuntime::advance_time)
        .def("add_blocked", &F05RepeatedBooleanCooldownRuntime::add_blocked)
        .def("lineage", &F05RepeatedBooleanCooldownRuntime::lineage)
        .def("audit", &F05RepeatedBooleanCooldownRuntime::audit)
        .def("checkpoint", &F05RepeatedBooleanCooldownRuntime::checkpoint)
        .def("restore", &F05RepeatedBooleanCooldownRuntime::restore)
        .def_property_readonly(
            "parity_qualified",
            &F05RepeatedBooleanCooldownRuntime::parity_qualified)
        .def_property_readonly(
            "qualification_under_test",
            &F05RepeatedBooleanCooldownRuntime::qualification_under_test)
        .def_property_readonly(
            "execution_admitted",
            &F05RepeatedBooleanCooldownRuntime::execution_admitted)
        .def_property_readonly(
            "binding_error",
            &F05RepeatedBooleanCooldownRuntime::binding_error)
        .def_property_readonly(
            "config",
            &F05RepeatedBooleanCooldownRuntime::config,
            py::return_value_policy::reference_internal);
}

void bind_tick_replay(py::module_& m) {
    bind_f05_repeated_boolean_cooldown(m);
    py::class_<TickReplayParams>(m, "TickReplayParams")
        .def(py::init<>())
        .def_readwrite("quote", &TickReplayParams::quote)
        .def_readwrite("order_size", &TickReplayParams::order_size)
        .def_readwrite("max_inventory", &TickReplayParams::max_inventory)
        .def_readwrite("eta", &TickReplayParams::eta)
        .def_readwrite("symmetric_size", &TickReplayParams::symmetric_size)
        .def_readwrite("requote_interval_s", &TickReplayParams::requote_interval_s)
        .def_readwrite("rq_min_s", &TickReplayParams::rq_min_s)
        .def_readwrite("rq_max_s", &TickReplayParams::rq_max_s)
        .def_readwrite("requote_clock_fixed", &TickReplayParams::requote_clock_fixed)
        .def_readwrite("empirical_requote_clock", &TickReplayParams::empirical_requote_clock)
        .def_readwrite("maker_fee", &TickReplayParams::maker_fee)
        .def_readwrite("taker_fee", &TickReplayParams::taker_fee)
        .def_readwrite("queue_base", &TickReplayParams::queue_base)
        .def_readwrite("queue_decay", &TickReplayParams::queue_decay)
        .def_readwrite("queue_price_tolerance", &TickReplayParams::queue_price_tolerance)
        .def_readwrite("maker_fill_prob", &TickReplayParams::maker_fill_prob)
        .def_readwrite("buy_fill_prob", &TickReplayParams::buy_fill_prob)
        .def_readwrite("sell_fill_prob", &TickReplayParams::sell_fill_prob)
        .def_readwrite("queue_ahead_base_mult", &TickReplayParams::queue_ahead_base_mult)
        .def_readwrite("queue_deplete_base_mult", &TickReplayParams::queue_deplete_base_mult)
        .def_readwrite("queue_l2_cancel_ahead_enabled", &TickReplayParams::queue_l2_cancel_ahead_enabled)
        .def_readwrite(
            "native_exchange_book_enabled",
            &TickReplayParams::native_exchange_book_enabled)
        .def_readwrite(
            "native_exchange_book_strict_after_ns",
            &TickReplayParams::native_exchange_book_strict_after_ns)
        .def(
            "set_native_exchange_book_arrays",
            [](TickReplayParams& params,
               CArray<std::int64_t> event_ts_ns,
               CArray<std::uint8_t> event_type,
               CArray<std::int64_t> receive_ts_ns,
               CArray<std::int64_t> event_time_ms,
               CArray<std::int64_t> transaction_time_ms,
               CArray<std::int64_t> first_update_id,
               CArray<std::int64_t> final_update_id,
               CArray<std::int64_t> previous_final_update_id,
               CArray<std::int64_t> last_update_id,
               CArray<std::int64_t> level_offsets,
               CArray<std::uint8_t> level_is_bid,
               CArray<std::int64_t> level_price_tick,
               CArray<double> level_quantity) {
                const auto events = event_ts_ns.size();
                const auto levels = level_price_tick.size();
                const auto require_events = [events](py::ssize_t size,
                                                     const char* name) {
                    if (size != events) {
                        throw std::invalid_argument(
                            std::string(name) + " length mismatch");
                    }
                };
                require_events(event_type.size(), "native event_type");
                require_events(receive_ts_ns.size(), "native receive_ts_ns");
                require_events(event_time_ms.size(), "native event_time_ms");
                require_events(
                    transaction_time_ms.size(),
                    "native transaction_time_ms");
                require_events(first_update_id.size(), "native first_update_id");
                require_events(final_update_id.size(), "native final_update_id");
                require_events(
                    previous_final_update_id.size(),
                    "native previous_final_update_id");
                require_events(last_update_id.size(), "native last_update_id");
                if (level_offsets.size() != events + 1 ||
                    level_is_bid.size() != levels ||
                    level_quantity.size() != levels) {
                    throw std::invalid_argument(
                        "native exchange-book level array shape drifted");
                }
                const auto copy_i64 = [](const CArray<std::int64_t>& value) {
                    if (value.size() == 0) return std::vector<std::int64_t>{};
                    return std::vector<std::int64_t>(
                        value.data(), value.data() + value.size());
                };
                const auto copy_u8 = [](const CArray<std::uint8_t>& value) {
                    if (value.size() == 0) return std::vector<std::uint8_t>{};
                    return std::vector<std::uint8_t>(
                        value.data(), value.data() + value.size());
                };
                const auto copy_f64 = [](const CArray<double>& value) {
                    if (value.size() == 0) return std::vector<double>{};
                    return std::vector<double>(
                        value.data(), value.data() + value.size());
                };
                params.native_book_event_ts_ns = copy_i64(event_ts_ns);
                params.native_book_event_type = copy_u8(event_type);
                params.native_book_receive_ts_ns = copy_i64(receive_ts_ns);
                params.native_book_event_time_ms = copy_i64(event_time_ms);
                params.native_book_transaction_time_ms =
                    copy_i64(transaction_time_ms);
                params.native_book_first_update_id = copy_i64(first_update_id);
                params.native_book_final_update_id = copy_i64(final_update_id);
                params.native_book_previous_final_update_id =
                    copy_i64(previous_final_update_id);
                params.native_book_last_update_id = copy_i64(last_update_id);
                params.native_book_level_offsets = copy_i64(level_offsets);
                params.native_book_level_is_bid = copy_u8(level_is_bid);
                params.native_book_level_price_tick = copy_i64(level_price_tick);
                params.native_book_level_quantity = copy_f64(level_quantity);
                params.native_exchange_book_enabled = events > 0;
                return events;
            },
            py::arg("event_ts_ns"), py::arg("event_type"),
            py::arg("receive_ts_ns"), py::arg("event_time_ms"),
            py::arg("transaction_time_ms"), py::arg("first_update_id"),
            py::arg("final_update_id"),
            py::arg("previous_final_update_id"),
            py::arg("last_update_id"), py::arg("level_offsets"),
            py::arg("level_is_bid"), py::arg("level_price_tick"),
            py::arg("level_quantity"))
        .def_readwrite("queue_ahead_buy_exposure_mult", &TickReplayParams::queue_ahead_buy_exposure_mult)
        .def_readwrite("queue_ahead_buy_reducing_mult", &TickReplayParams::queue_ahead_buy_reducing_mult)
        .def_readwrite("queue_ahead_sell_exposure_mult", &TickReplayParams::queue_ahead_sell_exposure_mult)
        .def_readwrite("queue_ahead_sell_reducing_mult", &TickReplayParams::queue_ahead_sell_reducing_mult)
        .def_readwrite("queue_regime_distance_edges", &TickReplayParams::queue_regime_distance_edges)
        .def_readwrite("queue_regime_rank_edges", &TickReplayParams::queue_regime_rank_edges)
        .def_readwrite("queue_regime_buy_mult", &TickReplayParams::queue_regime_buy_mult)
        .def_readwrite("queue_regime_sell_mult", &TickReplayParams::queue_regime_sell_mult)
        .def_readwrite("queue_deplete_rank_edges", &TickReplayParams::queue_deplete_rank_edges)
        .def_readwrite("queue_deplete_buy_mult", &TickReplayParams::queue_deplete_buy_mult)
        .def_readwrite("queue_deplete_sell_mult", &TickReplayParams::queue_deplete_sell_mult)
        .def_readwrite("queue_mo_edges", &TickReplayParams::queue_mo_edges)
        .def_readwrite("queue_mo_buy_mult", &TickReplayParams::queue_mo_buy_mult)
        .def_readwrite("queue_mo_sell_mult", &TickReplayParams::queue_mo_sell_mult)
        .def_readwrite("requote_threshold_bps", &TickReplayParams::requote_threshold_bps)
        .def_readwrite("replace_min_price_change_ticks", &TickReplayParams::replace_min_price_change_ticks)
        .def_readwrite("replace_min_price_change_ticks_reducing", &TickReplayParams::replace_min_price_change_ticks_reducing)
        .def_readwrite("replace_min_interval_ms", &TickReplayParams::replace_min_interval_ms)
        .def_readwrite("replace_min_interval_ms_reducing", &TickReplayParams::replace_min_interval_ms_reducing)
        .def_readwrite("replace_pending_coalesce", &TickReplayParams::replace_pending_coalesce)
        .def_readwrite(
            "replace_terminal_continuation",
            &TickReplayParams::replace_terminal_continuation
        )
        .def_readwrite("replace_cancel_first_exposure_increasing", &TickReplayParams::replace_cancel_first_exposure_increasing)
        .def_readwrite("new_order_latency_ms", &TickReplayParams::new_order_latency_ms)
        .def_readwrite("cancel_order_latency_ms", &TickReplayParams::cancel_order_latency_ms)
        .def_readwrite("latency_jitter_ms", &TickReplayParams::latency_jitter_ms)
        .def_readwrite("decision_to_gateway_latency_seed", &TickReplayParams::decision_to_gateway_latency_seed)
        .def_readwrite("decision_to_gateway_latency_samples_ms", &TickReplayParams::decision_to_gateway_latency_samples_ms)
        .def_readwrite("new_order_latency_samples_ms", &TickReplayParams::new_order_latency_samples_ms)
        .def_readwrite("new_order_exchange_effective_latency_samples_ms", &TickReplayParams::new_order_exchange_effective_latency_samples_ms)
        .def_readwrite("cancel_order_latency_samples_ms", &TickReplayParams::cancel_order_latency_samples_ms)
        .def_readwrite("cancel_exchange_effective_latency_samples_ms", &TickReplayParams::cancel_exchange_effective_latency_samples_ms)
        .def_readwrite("cancel_ack_visibility_latency_samples_ms", &TickReplayParams::cancel_ack_visibility_latency_samples_ms)
        .def_readwrite("exec_book_visibility_delay_samples_ms", &TickReplayParams::exec_book_visibility_delay_samples_ms)
        .def_readwrite("exec_book_visibility_delay_mean_ms", &TickReplayParams::exec_book_visibility_delay_mean_ms)
        .def_readwrite("exec_book_visibility_delay_jitter_ms", &TickReplayParams::exec_book_visibility_delay_jitter_ms)
        .def_readwrite("exec_book_visibility_delay_seed", &TickReplayParams::exec_book_visibility_delay_seed)
        .def_readwrite("max_exec_book_age_ms", &TickReplayParams::max_exec_book_age_ms)
        .def_readwrite("ber_guard_thresh", &TickReplayParams::ber_guard_thresh)
        .def_readwrite("ber_exposure_add_only", &TickReplayParams::ber_exposure_add_only)
        .def_readwrite("fill_cooldown_s", &TickReplayParams::fill_cooldown_s)
        .def_readwrite("fill_cooldown_apply_reducing", &TickReplayParams::fill_cooldown_apply_reducing)
        .def_readwrite("fill_cooldown_consecutive_reset_policy", &TickReplayParams::fill_cooldown_consecutive_reset_policy)
        .def_readwrite("fill_cooldown_reset_consec_on_expiry", &TickReplayParams::fill_cooldown_reset_consec_on_expiry)
        .def_readwrite("fill_cooldown_reducing_s", &TickReplayParams::fill_cooldown_reducing_s)
        .def_readwrite("fill_cooldown_reducing_campaign_only", &TickReplayParams::fill_cooldown_reducing_campaign_only)
        .def_readwrite("fill_cooldown_reducing_inv_threshold", &TickReplayParams::fill_cooldown_reducing_inv_threshold)
        .def_readwrite("fill_cooldown_reducing_inv_ratio", &TickReplayParams::fill_cooldown_reducing_inv_ratio)
        .def_readwrite("fill_cooldown_reducing_age_s", &TickReplayParams::fill_cooldown_reducing_age_s)
        .def_readwrite("fill_cooldown_reducing_vol_ref", &TickReplayParams::fill_cooldown_reducing_vol_ref)
        .def_readwrite("fill_cooldown_reducing_vol_min_mult", &TickReplayParams::fill_cooldown_reducing_vol_min_mult)
        .def_readwrite("fill_cooldown_reducing_vol_max_mult", &TickReplayParams::fill_cooldown_reducing_vol_max_mult)
        .def_readwrite("max_consecutive_losses", &TickReplayParams::max_consecutive_losses)
        .def_readwrite("cooldown_after_loss_s", &TickReplayParams::cooldown_after_loss_s)
        .def_readwrite("consecutive_loss_cooldown_semantics", &TickReplayParams::consecutive_loss_cooldown_semantics)
        .def_readwrite("consecutive_loss_snapshot_enabled", &TickReplayParams::consecutive_loss_snapshot_enabled)
        .def_readwrite("consecutive_loss_snapshot_schema", &TickReplayParams::consecutive_loss_snapshot_schema)
        .def_readwrite("initial_loss_open_commission", &TickReplayParams::initial_loss_open_commission)
        .def_readwrite("initial_loss_round_trip_pnl", &TickReplayParams::initial_loss_round_trip_pnl)
        .def_readwrite("initial_loss_consecutive_losses", &TickReplayParams::initial_loss_consecutive_losses)
        .def_readwrite("initial_loss_cooldown_until_ms", &TickReplayParams::initial_loss_cooldown_until_ms)
        .def_readwrite("initial_loss_last_cancel_ts_ms", &TickReplayParams::initial_loss_last_cancel_ts_ms)
        .def_readwrite("initial_loss_threshold_pending", &TickReplayParams::initial_loss_threshold_pending)
        .def_readwrite("initial_loss_trigger_count", &TickReplayParams::initial_loss_trigger_count)
        .def_readwrite("initial_loss_expiry_count", &TickReplayParams::initial_loss_expiry_count)
        .def_readwrite("initial_loss_losing_round_trips", &TickReplayParams::initial_loss_losing_round_trips)
        .def_readwrite("initial_loss_winning_or_flat_round_trips", &TickReplayParams::initial_loss_winning_or_flat_round_trips)
        .def_readwrite("initial_loss_max_observed_consecutive_losses", &TickReplayParams::initial_loss_max_observed_consecutive_losses)
        .def_readwrite("sync_adjust_degrade_enabled", &TickReplayParams::sync_adjust_degrade_enabled)
        .def_readwrite("sync_adjust_pause_s", &TickReplayParams::sync_adjust_pause_s)
        .def_readwrite("sync_adjust_cancel_orders", &TickReplayParams::sync_adjust_cancel_orders)
        .def_readwrite("sync_adjust_replay_mode", &TickReplayParams::sync_adjust_replay_mode)
        .def_readwrite("sync_adjust_semantics", &TickReplayParams::sync_adjust_semantics)
        .def_readwrite("thin_depth_threshold", &TickReplayParams::thin_depth_threshold)
        .def_readwrite("l2_refill_cancel_near_levels", &TickReplayParams::l2_refill_cancel_near_levels)
        .def_readwrite("l2_policy_depth_levels", &TickReplayParams::l2_policy_depth_levels)
        .def_readwrite("l2_refill_cancel_lookback_s", &TickReplayParams::l2_refill_cancel_lookback_s)
        .def_readwrite(
            "markout_ema_span_fills",
            &TickReplayParams::markout_ema_span_fills
        )
        .def_readwrite("markout_horizon_s", &TickReplayParams::markout_horizon_s)
        .def_readwrite("adverse_markout_pause_hybrid", &TickReplayParams::adverse_markout_pause_hybrid)
        .def_readwrite("adverse_markout_pause_base_s", &TickReplayParams::adverse_markout_pause_base_s)
        .def_readwrite("adverse_markout_pause_min_s", &TickReplayParams::adverse_markout_pause_min_s)
        .def_readwrite("adverse_markout_pause_max_s", &TickReplayParams::adverse_markout_pause_max_s)
        .def_readwrite("adverse_markout_decay_tau_s", &TickReplayParams::adverse_markout_decay_tau_s)
        .def_readwrite("adverse_markout_max_resolve_gap_s", &TickReplayParams::adverse_markout_max_resolve_gap_s)
        .def_readwrite("use_bar_pricing", &TickReplayParams::use_bar_pricing)
        .def_readwrite("ret_demean_halflife", &TickReplayParams::ret_demean_halflife)
        .def_readwrite("initial_inventory", &TickReplayParams::initial_inventory)
        .def_readwrite("max_daily_loss", &TickReplayParams::max_daily_loss)
        .def_readwrite("max_position_value", &TickReplayParams::max_position_value)
        .def_readwrite("emergency_close_dd", &TickReplayParams::emergency_close_dd)
        .def_readwrite("initial_risk_state_enabled", &TickReplayParams::initial_risk_state_enabled)
        .def_readwrite("initial_risk_utc_day", &TickReplayParams::initial_risk_utc_day)
        .def_readwrite("initial_risk_day_start_total_pnl", &TickReplayParams::initial_risk_day_start_total_pnl)
        .def_readwrite("initial_risk_session_peak_pnl", &TickReplayParams::initial_risk_session_peak_pnl)
        .def_readwrite("initial_risk_last_total_pnl", &TickReplayParams::initial_risk_last_total_pnl)
        .def_readwrite("initial_risk_total_pnl_offset", &TickReplayParams::initial_risk_total_pnl_offset)
        .def_readwrite("initial_entry_price", &TickReplayParams::initial_entry_price)
        .def_readwrite("planned_quote_stop_ts_ms", &TickReplayParams::planned_quote_stop_ts_ms)
        .def_readwrite("initial_sigma_sq", &TickReplayParams::initial_sigma_sq)
        .def_readwrite("rng_seed", &TickReplayParams::rng_seed)
        .def_readwrite("latency_seed", &TickReplayParams::latency_seed)
        .def_readwrite("replay_contract_sha256", &TickReplayParams::replay_contract_sha256)
        .def_readwrite("latency_sampler_version", &TickReplayParams::latency_sampler_version)
        .def_readwrite("latency_stress_enabled", &TickReplayParams::latency_stress_enabled)
        .def_readwrite(
            "latency_stress_spike_probability",
            &TickReplayParams::latency_stress_spike_probability
        )
        .def_readwrite(
            "latency_stress_spike_multiplier",
            &TickReplayParams::latency_stress_spike_multiplier
        )
        .def_readwrite("random_passive_enabled", &TickReplayParams::random_passive_enabled)
        .def_readwrite("random_passive_seed", &TickReplayParams::random_passive_seed)
        .def_readwrite("random_passive_side_mirror_prob", &TickReplayParams::random_passive_side_mirror_prob)
        .def_readwrite("random_passive_timing_jitter_fraction", &TickReplayParams::random_passive_timing_jitter_fraction)
        .def_readwrite("random_passive_preserve_inventory_skew", &TickReplayParams::random_passive_preserve_inventory_skew)
        .def_readwrite("fixed_spread_probe_enabled", &TickReplayParams::fixed_spread_probe_enabled)
        .def_readwrite("fixed_spread_probe_ticks", &TickReplayParams::fixed_spread_probe_ticks)
        .def_readwrite("paired_fixed_spread_probe_enabled", &TickReplayParams::paired_fixed_spread_probe_enabled)
        .def_readwrite("paired_fixed_spread_probe_ticks", &TickReplayParams::paired_fixed_spread_probe_ticks)
        .def_readwrite("paired_fixed_spread_fail_on_violation", &TickReplayParams::paired_fixed_spread_fail_on_violation)
        .def_readwrite("paired_fixed_spread_max_recorded_violations", &TickReplayParams::paired_fixed_spread_max_recorded_violations)
        .def_readwrite("local_extreme_guard_enabled", &TickReplayParams::local_extreme_guard_enabled)
        .def_readwrite("local_extreme_window_s", &TickReplayParams::local_extreme_window_s)
        .def_readwrite("local_extreme_rank_threshold", &TickReplayParams::local_extreme_rank_threshold)
        .def_readwrite("local_extreme_require_thin_depth", &TickReplayParams::local_extreme_require_thin_depth)
        .def_readwrite("local_extreme_thin_depth_threshold", &TickReplayParams::local_extreme_thin_depth_threshold)
        .def_readwrite("local_extreme_spread_mult", &TickReplayParams::local_extreme_spread_mult)
        .def_readwrite("local_extreme_pause", &TickReplayParams::local_extreme_pause)
        .def_readwrite("fragile_order_ttl_s", &TickReplayParams::fragile_order_ttl_s)
        .def_readwrite("adaptive_add_cooldown_enabled", &TickReplayParams::adaptive_add_cooldown_enabled)
        .def_readwrite("adaptive_add_cooldown_min_mult", &TickReplayParams::adaptive_add_cooldown_min_mult)
        .def_readwrite("adaptive_add_cooldown_max_mult", &TickReplayParams::adaptive_add_cooldown_max_mult)
        .def_readwrite("adaptive_add_cooldown_w_markout", &TickReplayParams::adaptive_add_cooldown_w_markout)
        .def_readwrite("adaptive_add_cooldown_w_flow", &TickReplayParams::adaptive_add_cooldown_w_flow)
        .def_readwrite("adaptive_add_cooldown_w_campaign", &TickReplayParams::adaptive_add_cooldown_w_campaign)
        .def_readwrite("adaptive_add_cooldown_w_trend", &TickReplayParams::adaptive_add_cooldown_w_trend)
        .def_readwrite("adaptive_add_cooldown_w_refill_weak", &TickReplayParams::adaptive_add_cooldown_w_refill_weak)
        .def_readwrite("adaptive_add_cooldown_w_refill_good", &TickReplayParams::adaptive_add_cooldown_w_refill_good)
        .def_readwrite("adaptive_add_cooldown_w_reversion", &TickReplayParams::adaptive_add_cooldown_w_reversion)
        .def_readwrite("adaptive_add_cooldown_mo_ref", &TickReplayParams::adaptive_add_cooldown_mo_ref)
        .def_readwrite("adaptive_add_cooldown_flow_ref", &TickReplayParams::adaptive_add_cooldown_flow_ref)
        .def_readwrite("adaptive_add_cooldown_campaign_inv_ref", &TickReplayParams::adaptive_add_cooldown_campaign_inv_ref)
        .def_readwrite("adaptive_add_cooldown_campaign_age_ref_s", &TickReplayParams::adaptive_add_cooldown_campaign_age_ref_s)
        .def_readwrite("adaptive_add_cooldown_trend_ret_ref", &TickReplayParams::adaptive_add_cooldown_trend_ret_ref)
        .def_readwrite("adaptive_add_cooldown_refill_ref", &TickReplayParams::adaptive_add_cooldown_refill_ref)
        .def_readwrite("adaptive_add_cooldown_reversion_ref", &TickReplayParams::adaptive_add_cooldown_reversion_ref)
        .def_readwrite("adaptive_add_cooldown_gate_enabled", &TickReplayParams::adaptive_add_cooldown_gate_enabled)
        .def_readwrite("adaptive_add_cooldown_gate_mult", &TickReplayParams::adaptive_add_cooldown_gate_mult)
        .def_readwrite("adaptive_add_cooldown_gate_campaign_score", &TickReplayParams::adaptive_add_cooldown_gate_campaign_score)
        .def_readwrite("adaptive_add_cooldown_gate_trend_score", &TickReplayParams::adaptive_add_cooldown_gate_trend_score)
        .def_readwrite("adaptive_add_cooldown_gate_refill_edge_max", &TickReplayParams::adaptive_add_cooldown_gate_refill_edge_max)
        .def_readwrite("adaptive_add_cooldown_gate_reversion_max", &TickReplayParams::adaptive_add_cooldown_gate_reversion_max)
        .def_readwrite("adaptive_add_cooldown_gate_side", &TickReplayParams::adaptive_add_cooldown_gate_side)
        .def_readwrite("flat_unilateral_max_s", &TickReplayParams::flat_unilateral_max_s)
        .def_readwrite("campaign_stop_add_enabled", &TickReplayParams::campaign_stop_add_enabled)
        .def_readwrite("campaign_stop_add_inv_threshold", &TickReplayParams::campaign_stop_add_inv_threshold)
        .def_readwrite("campaign_stop_add_age_s", &TickReplayParams::campaign_stop_add_age_s)
        .def_readwrite("campaign_soft_control_enabled", &TickReplayParams::campaign_soft_control_enabled)
        .def_readwrite("campaign_soft_inv_threshold", &TickReplayParams::campaign_soft_inv_threshold)
        .def_readwrite("campaign_soft_age_s", &TickReplayParams::campaign_soft_age_s)
        .def_readwrite("campaign_soft_spread_mult", &TickReplayParams::campaign_soft_spread_mult)
        .def_readwrite("campaign_soft_gate_enabled", &TickReplayParams::campaign_soft_gate_enabled)
        .def_readwrite("campaign_soft_gate_campaign_inv_ref", &TickReplayParams::campaign_soft_gate_campaign_inv_ref)
        .def_readwrite("campaign_soft_gate_campaign_age_ref_s", &TickReplayParams::campaign_soft_gate_campaign_age_ref_s)
        .def_readwrite("campaign_soft_gate_trend_ret_ref", &TickReplayParams::campaign_soft_gate_trend_ret_ref)
        .def_readwrite("campaign_soft_gate_refill_ref", &TickReplayParams::campaign_soft_gate_refill_ref)
        .def_readwrite("campaign_soft_gate_campaign_score", &TickReplayParams::campaign_soft_gate_campaign_score)
        .def_readwrite("campaign_soft_gate_trend_score", &TickReplayParams::campaign_soft_gate_trend_score)
        .def_readwrite("campaign_soft_gate_refill_edge_max", &TickReplayParams::campaign_soft_gate_refill_edge_max)
        .def_readwrite("campaign_soft_gate_reversion_max", &TickReplayParams::campaign_soft_gate_reversion_max)
        .def_readwrite("campaign_soft_gate_side", &TickReplayParams::campaign_soft_gate_side)
        .def_readwrite("position_timeout_s", &TickReplayParams::position_timeout_s)
        .def_readwrite("circuit_breaker_sigma", &TickReplayParams::circuit_breaker_sigma)
        .def_readwrite("circuit_breaker_maker_close", &TickReplayParams::circuit_breaker_maker_close)
        .def_readwrite("emergency_taker_close_enabled", &TickReplayParams::emergency_taker_close_enabled)
        .def_readwrite("buy_fill_selection_live_enabled", &TickReplayParams::buy_fill_selection_live_enabled)
        .def_readwrite("buy_fill_selection_live_score_threshold", &TickReplayParams::buy_fill_selection_live_score_threshold)
        .def_readwrite("buy_fill_selection_live_spread_mult_cap", &TickReplayParams::buy_fill_selection_live_spread_mult_cap)
        .def_readwrite("buy_fill_selection_live_apply_reducing", &TickReplayParams::buy_fill_selection_live_apply_reducing)
        .def_readwrite("buy_fill_selection_live_max_missing_features", &TickReplayParams::buy_fill_selection_live_max_missing_features)
        .def_readwrite("buy_fill_selection_models", &TickReplayParams::buy_fill_selection_models)
        .def_readwrite("buy_soft_widen_release_probe_enabled", &TickReplayParams::buy_soft_widen_release_probe_enabled)
        .def_readwrite("buy_soft_widen_release_probe_apply_candidate", &TickReplayParams::buy_soft_widen_release_probe_apply_candidate)
        .def_readwrite("buy_soft_widen_release_target_ts_ms", &TickReplayParams::buy_soft_widen_release_target_ts_ms)
        .def_readwrite("buy_soft_widen_release_target_role", &TickReplayParams::buy_soft_widen_release_target_role)
        .def_readwrite("buy_soft_widen_release_spread_mult_cap", &TickReplayParams::buy_soft_widen_release_spread_mult_cap)
        .def_readwrite("conditional_p3_reach_gate_enabled", &TickReplayParams::conditional_p3_reach_gate_enabled)
        .def_readwrite("conditional_p3_reach_gate_outward_ticks", &TickReplayParams::conditional_p3_reach_gate_outward_ticks)
        .def_readwrite("conditional_p3_reach_gate_grid_min_ticks", &TickReplayParams::conditional_p3_reach_gate_grid_min_ticks)
        .def_readwrite("conditional_p3_reach_gate_buy_toxicity_threshold", &TickReplayParams::conditional_p3_reach_gate_buy_toxicity_threshold)
        .def_readwrite("conditional_p3_reach_gate_sell_toxicity_threshold", &TickReplayParams::conditional_p3_reach_gate_sell_toxicity_threshold)
        .def_readwrite("conditional_p3_reach_budget_policy_enabled", &TickReplayParams::conditional_p3_reach_budget_policy_enabled)
        .def_readwrite("conditional_p3_reach_budget_grid_min_ticks", &TickReplayParams::conditional_p3_reach_budget_grid_min_ticks)
        .def_readwrite("conditional_p3_reach_budget_buy_toxicity_threshold", &TickReplayParams::conditional_p3_reach_budget_buy_toxicity_threshold)
        .def_readwrite("conditional_p3_reach_budget_sell_toxicity_threshold", &TickReplayParams::conditional_p3_reach_budget_sell_toxicity_threshold)
        .def_readwrite("cross_venue_fair_center_shift_enabled", &TickReplayParams::cross_venue_fair_center_shift_enabled)
        .def_readwrite("cross_venue_fair_center_max_state_age_ms", &TickReplayParams::cross_venue_fair_center_max_state_age_ms)
        .def_readwrite("trace_fills_max", &TickReplayParams::trace_fills_max)
        .def_readwrite("trace_quotes_max", &TickReplayParams::trace_quotes_max)
        .def_readwrite("trace_p3_reach_decisions_max", &TickReplayParams::trace_p3_reach_decisions_max)
        .def_readwrite(
            "trace_cooldown_duration_opportunities_max",
            &TickReplayParams::trace_cooldown_duration_opportunities_max)
        .def_readwrite(
            "cooldown_duration_fork_enabled",
            &TickReplayParams::cooldown_duration_fork_enabled)
        .def_readwrite(
            "cooldown_duration_fork_action",
            &TickReplayParams::cooldown_duration_fork_action)
        .def_readwrite(
            "cooldown_duration_fork_target_ordinal",
            &TickReplayParams::cooldown_duration_fork_target_ordinal)
        .def_readwrite(
            "cooldown_duration_fork_target_ts_ms",
            &TickReplayParams::cooldown_duration_fork_target_ts_ms)
        .def_readwrite(
            "cooldown_duration_fork_target_side",
            &TickReplayParams::cooldown_duration_fork_target_side)
        .def_readwrite(
            "cooldown_duration_fork_target_order_id",
            &TickReplayParams::cooldown_duration_fork_target_order_id)
        .def_readwrite(
            "cooldown_duration_fork_target_campaign_id",
            &TickReplayParams::cooldown_duration_fork_target_campaign_id)
        .def_readwrite(
            "cooldown_duration_fork_expected_baseline_ms",
            &TickReplayParams::cooldown_duration_fork_expected_baseline_ms)
        .def_readwrite(
            "cooldown_duration_fork_fixed_ms",
            &TickReplayParams::cooldown_duration_fork_fixed_ms)
        .def_readwrite(
            "cooldown_duration_fork_baseline_policy_enabled",
            &TickReplayParams::cooldown_duration_fork_baseline_policy_enabled)
        .def_readwrite(
            "cooldown_duration_fork_expected_owner_action",
            &TickReplayParams::cooldown_duration_fork_expected_owner_action)
        .def_readwrite(
            "cooldown_duration_fork_expected_owner_policy_sha256",
            &TickReplayParams::cooldown_duration_fork_expected_owner_policy_sha256)
        .def_readwrite(
            "f05_repeated_cooldown_runtime",
            &TickReplayParams::f05_repeated_cooldown_runtime)
        .def_readwrite(
            "f05_cooldown_window_tape",
            &TickReplayParams::f05_cooldown_window_tape)
        .def_readwrite(
            "f05_cooldown_window_tape_shared",
            &TickReplayParams::f05_cooldown_window_tape_shared)
        .def(
            "set_f05_cooldown_window_arrays",
            [](TickReplayParams &self,
               py::array_t<std::int64_t,
                           py::array::c_style | py::array::forcecast> left,
               py::array_t<std::int64_t,
                           py::array::c_style | py::array::forcecast> right,
               py::array_t<std::int64_t,
                           py::array::c_style | py::array::forcecast> ready,
               py::array_t<std::int64_t,
                           py::array::c_style | py::array::forcecast> market,
               py::array_t<std::int64_t,
                           py::array::c_style | py::array::forcecast> depth,
               py::array_t<double,
                           py::array::c_style | py::array::forcecast> mid,
               py::array_t<std::uint8_t,
                           py::array::c_style | py::array::forcecast> reset,
               py::array_t<std::uint8_t,
                           py::array::c_style | py::array::forcecast> source_gap,
               py::array_t<std::uint8_t,
                           py::array::c_style | py::array::forcecast> source_stale,
               py::array_t<std::uint8_t,
                           py::array::c_style | py::array::forcecast> warmup,
               py::array_t<std::uint8_t,
                           py::array::c_style | py::array::forcecast>
                   channel_support) {
              const auto count = left.size();
              if (left.ndim() != 1 || right.ndim() != 1 || ready.ndim() != 1 ||
                  market.ndim() != 1 || depth.ndim() != 1 || mid.ndim() != 1 ||
                  reset.ndim() != 1 || source_gap.ndim() != 1 ||
                  source_stale.ndim() != 1 ||
                  warmup.ndim() != 1 || channel_support.ndim() != 1 ||
                  right.size() != count || ready.size() != count ||
                  market.size() != count || depth.size() != count ||
                  mid.size() != count || reset.size() != count ||
                  source_gap.size() != count ||
                  source_stale.size() != count || warmup.size() != count ||
                  channel_support.size() != count) {
                throw std::invalid_argument(
                    "F05 cooldown window array shape drifted");
              }
              const auto left_view = left.unchecked<1>();
              const auto right_view = right.unchecked<1>();
              const auto ready_view = ready.unchecked<1>();
              const auto market_view = market.unchecked<1>();
              const auto depth_view = depth.unchecked<1>();
              const auto mid_view = mid.unchecked<1>();
              const auto reset_view = reset.unchecked<1>();
              const auto gap_view = source_gap.unchecked<1>();
              const auto stale_view = source_stale.unchecked<1>();
              const auto warmup_view = warmup.unchecked<1>();
              const auto support_view = channel_support.unchecked<1>();
              std::vector<F05CooldownWindowObservation> tape;
              tape.reserve(static_cast<std::size_t>(count));
              py::gil_scoped_release release;
              for (py::ssize_t index = 0; index < count; ++index) {
                F05CooldownWindowObservation observation;
                observation.left_ts_ns = left_view(index);
                observation.right_ts_ns = right_view(index);
                observation.feature_ready_ts_ns = ready_view(index);
                observation.market_generation = market_view(index);
                observation.depth_generation = depth_view(index);
                if (std::isfinite(mid_view(index))) {
                  observation.mid_usdc_per_btc = mid_view(index);
                }
                observation.reset_feature_state = reset_view(index) != 0;
                observation.source_gap = gap_view(index) != 0;
                observation.source_stale = stale_view(index) != 0;
                observation.warmup_admitted = warmup_view(index) != 0;
                observation.channel_support_valid = support_view(index) != 0;
                tape.push_back(std::move(observation));
              }
              self.f05_cooldown_window_tape = std::move(tape);
              return count;
            },
            py::arg("left_ts_ns"), py::arg("right_ts_ns"),
            py::arg("feature_ready_ts_ns"), py::arg("market_generation"),
            py::arg("depth_generation"), py::arg("mid_usdc_per_btc"),
            py::arg("reset_feature_state"),
            py::arg("source_gap"), py::arg("source_stale"),
            py::arg("warmup_admitted"), py::arg("channel_support_valid"))
        .def_readwrite(
            "f05_cooldown_predicate_rows",
            &TickReplayParams::f05_cooldown_predicate_rows)
        .def_readwrite("trace_window_ms", &TickReplayParams::trace_window_ms)
        .def_readwrite("collect_curves", &TickReplayParams::collect_curves);

    m.def(
        "build_f05_cooldown_window_tape",
        [](py::array_t<std::int64_t,
                       py::array::c_style | py::array::forcecast> left,
           py::array_t<std::int64_t,
                       py::array::c_style | py::array::forcecast> right,
           py::array_t<std::int64_t,
                       py::array::c_style | py::array::forcecast> ready,
           py::array_t<std::int64_t,
                       py::array::c_style | py::array::forcecast> market,
           py::array_t<std::int64_t,
                       py::array::c_style | py::array::forcecast> depth,
           py::array_t<double,
                       py::array::c_style | py::array::forcecast> mid,
           py::array_t<std::uint8_t,
                       py::array::c_style | py::array::forcecast> reset,
           py::array_t<std::uint8_t,
                       py::array::c_style | py::array::forcecast> source_gap,
           py::array_t<std::uint8_t,
                       py::array::c_style | py::array::forcecast> source_stale,
           py::array_t<std::uint8_t,
                       py::array::c_style | py::array::forcecast> warmup,
           py::array_t<std::uint8_t,
                       py::array::c_style | py::array::forcecast> channel_support,
           const std::string& content_sha256) {
          const auto count = left.size();
          if (left.ndim() != 1 || right.ndim() != 1 || ready.ndim() != 1 ||
              market.ndim() != 1 || depth.ndim() != 1 || mid.ndim() != 1 ||
              reset.ndim() != 1 || source_gap.ndim() != 1 ||
              source_stale.ndim() != 1 ||
              warmup.ndim() != 1 || channel_support.ndim() != 1 ||
              right.size() != count || ready.size() != count ||
              market.size() != count || depth.size() != count ||
              mid.size() != count || reset.size() != count ||
              source_gap.size() != count ||
              source_stale.size() != count || warmup.size() != count ||
              channel_support.size() != count || content_sha256.size() != 64) {
            throw std::invalid_argument(
                "F05 shared cooldown window tape contract drifted");
          }
          const auto left_view = left.unchecked<1>();
          const auto right_view = right.unchecked<1>();
          const auto ready_view = ready.unchecked<1>();
          const auto market_view = market.unchecked<1>();
          const auto depth_view = depth.unchecked<1>();
          const auto mid_view = mid.unchecked<1>();
          const auto reset_view = reset.unchecked<1>();
          const auto gap_view = source_gap.unchecked<1>();
          const auto stale_view = source_stale.unchecked<1>();
          const auto warmup_view = warmup.unchecked<1>();
          const auto support_view = channel_support.unchecked<1>();
          auto tape = std::make_shared<F05CooldownWindowTape>();
          tape->content_sha256 = content_sha256;
          tape->observations.reserve(static_cast<std::size_t>(count));
          py::gil_scoped_release release;
          for (py::ssize_t index = 0; index < count; ++index) {
            F05CooldownWindowObservation observation;
            observation.left_ts_ns = left_view(index);
            observation.right_ts_ns = right_view(index);
            observation.feature_ready_ts_ns = ready_view(index);
            observation.market_generation = market_view(index);
            observation.depth_generation = depth_view(index);
            if (std::isfinite(mid_view(index))) {
              observation.mid_usdc_per_btc = mid_view(index);
            }
            observation.reset_feature_state = reset_view(index) != 0;
            observation.source_gap = gap_view(index) != 0;
            observation.source_stale = stale_view(index) != 0;
            observation.warmup_admitted = warmup_view(index) != 0;
            observation.channel_support_valid = support_view(index) != 0;
            tape->observations.push_back(std::move(observation));
          }
          return tape;
        },
        py::arg("left_ts_ns"), py::arg("right_ts_ns"),
        py::arg("feature_ready_ts_ns"), py::arg("market_generation"),
        py::arg("depth_generation"), py::arg("mid_usdc_per_btc"),
        py::arg("reset_feature_state"),
        py::arg("source_gap"), py::arg("source_stale"),
        py::arg("warmup_admitted"), py::arg("channel_support_valid"),
        py::arg("content_sha256"));

    py::class_<TickReplaySummary>(m, "TickReplaySummary")
        .def(py::init<>())
        .def_readwrite("pnl", &TickReplaySummary::pnl)
        .def_readwrite("sharpe", &TickReplaySummary::sharpe)
        .def_readwrite("max_drawdown", &TickReplaySummary::max_drawdown)
        .def_readwrite("cash", &TickReplaySummary::cash)
        .def_readwrite("terminal_mark_price", &TickReplaySummary::terminal_mark_price)
        .def_readwrite("mtm_before_terminal_fee", &TickReplaySummary::mtm_before_terminal_fee)
        .def_readwrite("terminal_fee_drag", &TickReplaySummary::terminal_fee_drag)
        .def_readwrite("terminal_liquidation_fee_estimate", &TickReplaySummary::terminal_liquidation_fee_estimate)
        .def_readwrite("inventory_pnl", &TickReplaySummary::inventory_pnl)
        .def_readwrite("final_inventory", &TickReplaySummary::final_inventory)
        .def_readwrite("max_abs_inventory", &TickReplaySummary::max_abs_inventory)
        .def_readwrite("planned_quote_stop_triggered", &TickReplaySummary::planned_quote_stop_triggered)
        .def_readwrite("planned_quote_stop_trigger_ts_ms", &TickReplaySummary::planned_quote_stop_trigger_ts_ms)
        .def_readwrite("planned_shutdown_orders_at_trigger", &TickReplaySummary::planned_shutdown_orders_at_trigger)
        .def_readwrite("planned_shutdown_open_order_count", &TickReplaySummary::planned_shutdown_open_order_count)
        .def_readwrite("planned_shutdown_pending_new_order_count", &TickReplaySummary::planned_shutdown_pending_new_order_count)
        .def_readwrite("planned_shutdown_pending_cancel_order_count", &TickReplaySummary::planned_shutdown_pending_cancel_order_count)
        .def_readwrite("fills_bid", &TickReplaySummary::fills_bid)
        .def_readwrite("fills_ask", &TickReplaySummary::fills_ask)
        .def_readwrite("fills_total", &TickReplaySummary::fills_total)
        .def_readwrite("native_book_events_consumed", &TickReplaySummary::native_book_events_consumed)
        .def_readwrite("native_book_events_accepted", &TickReplaySummary::native_book_events_accepted)
        .def_readwrite("native_book_events_rejected", &TickReplaySummary::native_book_events_rejected)
        .def_readwrite("native_book_snapshot_events", &TickReplaySummary::native_book_snapshot_events)
        .def_readwrite("native_book_sequence_gaps", &TickReplaySummary::native_book_sequence_gaps)
        .def_readwrite("native_queue_lookup_count", &TickReplaySummary::native_queue_lookup_count)
        .def_readwrite("native_queue_exact_count", &TickReplaySummary::native_queue_exact_count)
        .def_readwrite("native_queue_known_zero_count", &TickReplaySummary::native_queue_known_zero_count)
        .def_readwrite("native_queue_missing_count", &TickReplaySummary::native_queue_missing_count)
        .def_readwrite("native_queue_invalidated_order_count", &TickReplaySummary::native_queue_invalidated_order_count)
        .def_readwrite("native_queue_ambiguous_event_count", &TickReplaySummary::native_queue_ambiguous_event_count)
        .def_readwrite("native_queue_cancel_ahead_event_count", &TickReplaySummary::native_queue_cancel_ahead_event_count)
        .def_readwrite("native_queue_cancel_ahead_qty", &TickReplaySummary::native_queue_cancel_ahead_qty)
        .def_readwrite("integer_tick_crossing_recovered_bid_candidates", &TickReplaySummary::integer_tick_crossing_recovered_bid_candidates)
        .def_readwrite("integer_tick_crossing_recovered_ask_candidates", &TickReplaySummary::integer_tick_crossing_recovered_ask_candidates)
        .def_readwrite("integer_tick_crossing_recovered_bid_fills", &TickReplaySummary::integer_tick_crossing_recovered_bid_fills)
        .def_readwrite("integer_tick_crossing_recovered_ask_fills", &TickReplaySummary::integer_tick_crossing_recovered_ask_fills)
        .def_readwrite("avg_markout", &TickReplaySummary::avg_markout)
        .def_readwrite("avg_markout_bid", &TickReplaySummary::avg_markout_bid)
        .def_readwrite("avg_markout_ask", &TickReplaySummary::avg_markout_ask)
        .def_readwrite("markout_count", &TickReplaySummary::markout_count)
        .def_readwrite("markout_qty_btc", &TickReplaySummary::markout_qty_btc)
        .def_readwrite("markout_qty_bid_btc", &TickReplaySummary::markout_qty_bid_btc)
        .def_readwrite("markout_qty_ask_btc", &TickReplaySummary::markout_qty_ask_btc)
        .def_readwrite("n_requotes", &TickReplaySummary::n_requotes)
        .def_readwrite("avg_rq_ms", &TickReplaySummary::avg_rq_ms)
        .def_readwrite("avg_spread", &TickReplaySummary::avg_spread)
        .def_readwrite("avg_final_spread", &TickReplaySummary::avg_final_spread)
        .def_readwrite("n_final_spread", &TickReplaySummary::n_final_spread)
        .def_readwrite("signed_inventory_time_s", &TickReplaySummary::signed_inventory_time_s)
        .def_readwrite("abs_inventory_time_s", &TickReplaySummary::abs_inventory_time_s)
        .def_readwrite("sq_inventory_time_s", &TickReplaySummary::sq_inventory_time_s)
        .def_readwrite("signed_notional_inventory_time_s", &TickReplaySummary::signed_notional_inventory_time_s)
        .def_readwrite("notional_inventory_time_s", &TickReplaySummary::notional_inventory_time_s)
        .def_readwrite("cap_hit_count", &TickReplaySummary::cap_hit_count)
        .def_readwrite("delta_cap_hit_count", &TickReplaySummary::delta_cap_hit_count)
        .def_readwrite("final_cap_compress_count", &TickReplaySummary::final_cap_compress_count)
        .def_readwrite("final_cap_rounding_count", &TickReplaySummary::final_cap_rounding_count)
        .def_readwrite("final_cap_mid_guard_count", &TickReplaySummary::final_cap_mid_guard_count)
        .def_readwrite("final_cap_post_only_count", &TickReplaySummary::final_cap_post_only_count)
        .def_readwrite("final_cap_delta_count", &TickReplaySummary::final_cap_delta_count)
        .def_readwrite("cap_exposure_block_count", &TickReplaySummary::cap_exposure_block_count)
        .def_readwrite("bid_cap_exposure_block_count", &TickReplaySummary::bid_cap_exposure_block_count)
        .def_readwrite("ask_cap_exposure_block_count", &TickReplaySummary::ask_cap_exposure_block_count)
        .def_readwrite("fills_bid_final_compressed", &TickReplaySummary::fills_bid_final_compressed)
        .def_readwrite("fills_ask_final_compressed", &TickReplaySummary::fills_ask_final_compressed)
        .def_readwrite("fills_bid_not_final_compressed", &TickReplaySummary::fills_bid_not_final_compressed)
        .def_readwrite("fills_ask_not_final_compressed", &TickReplaySummary::fills_ask_not_final_compressed)
        .def_readwrite("avg_markout_final_compressed", &TickReplaySummary::avg_markout_final_compressed)
        .def_readwrite("avg_markout_not_final_compressed", &TickReplaySummary::avg_markout_not_final_compressed)
        .def_readwrite("markout_qty_final_compressed_btc", &TickReplaySummary::markout_qty_final_compressed_btc)
        .def_readwrite("markout_qty_not_final_compressed_btc", &TickReplaySummary::markout_qty_not_final_compressed_btc)
        .def_readwrite("random_passive_mirror_count", &TickReplaySummary::random_passive_mirror_count)
        .def_readwrite("random_passive_mirror_eligible_count", &TickReplaySummary::random_passive_mirror_eligible_count)
        .def_readwrite("random_passive_timing_jitter_count", &TickReplaySummary::random_passive_timing_jitter_count)
        .def_readwrite("exec_book_visibility_delay_applied_count", &TickReplaySummary::exec_book_visibility_delay_applied_count)
        .def_readwrite("exec_book_visibility_delay_sum_ms", &TickReplaySummary::exec_book_visibility_delay_sum_ms)
        .def_readwrite("exec_book_visibility_delay_max_ms", &TickReplaySummary::exec_book_visibility_delay_max_ms)
        .def_readwrite("quote_spread_lt_100_count", &TickReplaySummary::quote_spread_lt_100_count)
        .def_readwrite("quote_spread_lt_150_count", &TickReplaySummary::quote_spread_lt_150_count)
        .def_readwrite("final_spread_lt_100_count", &TickReplaySummary::final_spread_lt_100_count)
        .def_readwrite("final_spread_lt_150_count", &TickReplaySummary::final_spread_lt_150_count)
        .def_readwrite("stale_book_skip_count", &TickReplaySummary::stale_book_skip_count)
        .def_readwrite("ber_active_count", &TickReplaySummary::ber_active_count)
        .def_readwrite("ber_feature_publish_count", &TickReplaySummary::ber_feature_publish_count)
        .def_readwrite("ber_role_safe_decision_count", &TickReplaySummary::ber_role_safe_decision_count)
        .def_readwrite("ber_role_safe_buy_add_count", &TickReplaySummary::ber_role_safe_buy_add_count)
        .def_readwrite("ber_role_safe_sell_add_count", &TickReplaySummary::ber_role_safe_sell_add_count)
        .def_readwrite("ber_role_safe_flat_bypass_count", &TickReplaySummary::ber_role_safe_flat_bypass_count)
        .def_readwrite("ber_role_safe_mixed_fail_closed_count", &TickReplaySummary::ber_role_safe_mixed_fail_closed_count)
        .def_readwrite("ber_role_safe_pair_change_count", &TickReplaySummary::ber_role_safe_pair_change_count)
        .def_readwrite("ber_role_safe_bid_change_count", &TickReplaySummary::ber_role_safe_bid_change_count)
        .def_readwrite("ber_role_safe_ask_change_count", &TickReplaySummary::ber_role_safe_ask_change_count)
        .def_readwrite("ber_role_safe_source_mismatch_count", &TickReplaySummary::ber_role_safe_source_mismatch_count)
        .def_readwrite("ber_role_safe_cap_collision_count", &TickReplaySummary::ber_role_safe_cap_collision_count)
        .def_readwrite("ber_role_safe_cap_infeasible_count", &TickReplaySummary::ber_role_safe_cap_infeasible_count)
        .def_readwrite("ber_held_input_end", &TickReplaySummary::ber_held_input_end)
        .def_readwrite("ber_ema_fast_end", &TickReplaySummary::ber_ema_fast_end)
        .def_readwrite("ber_ema_slow_end", &TickReplaySummary::ber_ema_slow_end)
        .def_readwrite("ber_active_end", &TickReplaySummary::ber_active_end)
        .def_readwrite("fill_cooldown_bid_block_count", &TickReplaySummary::fill_cooldown_bid_block_count)
        .def_readwrite("fill_cooldown_ask_block_count", &TickReplaySummary::fill_cooldown_ask_block_count)
        .def_readwrite("consecutive_loss_cooldown_trigger_count", &TickReplaySummary::consecutive_loss_cooldown_trigger_count)
        .def_readwrite("consecutive_loss_cooldown_expiry_count", &TickReplaySummary::consecutive_loss_cooldown_expiry_count)
        .def_readwrite("consecutive_loss_cooldown_block_count", &TickReplaySummary::consecutive_loss_cooldown_block_count)
        .def_readwrite("consecutive_loss_cooldown_cancel_count", &TickReplaySummary::consecutive_loss_cooldown_cancel_count)
        .def_readwrite("consecutive_loss_round_trip_loss_count", &TickReplaySummary::consecutive_loss_round_trip_loss_count)
        .def_readwrite("consecutive_loss_round_trip_nonloss_count", &TickReplaySummary::consecutive_loss_round_trip_nonloss_count)
        .def_readwrite("consecutive_loss_count_end", &TickReplaySummary::consecutive_loss_count_end)
        .def_readwrite("consecutive_loss_count_max", &TickReplaySummary::consecutive_loss_count_max)
        .def_readwrite("consecutive_loss_cooldown_until_ms", &TickReplaySummary::consecutive_loss_cooldown_until_ms)
        .def_readwrite("consecutive_loss_last_cancel_ts_end", &TickReplaySummary::consecutive_loss_last_cancel_ts_end)
        .def_readwrite("consecutive_loss_snapshot_schema", &TickReplaySummary::consecutive_loss_snapshot_schema)
        .def_readwrite("consecutive_loss_inventory_end", &TickReplaySummary::consecutive_loss_inventory_end)
        .def_readwrite("consecutive_loss_avg_entry_end", &TickReplaySummary::consecutive_loss_avg_entry_end)
        .def_readwrite("consecutive_loss_open_commission_end", &TickReplaySummary::consecutive_loss_open_commission_end)
        .def_readwrite("consecutive_loss_round_trip_pnl_end", &TickReplaySummary::consecutive_loss_round_trip_pnl_end)
        .def_readwrite("consecutive_loss_threshold_pending_end", &TickReplaySummary::consecutive_loss_threshold_pending_end)
        .def_readwrite("sync_adjust_degrade_trigger_count", &TickReplaySummary::sync_adjust_degrade_trigger_count)
        .def_readwrite("sync_adjust_degrade_block_bid_count", &TickReplaySummary::sync_adjust_degrade_block_bid_count)
        .def_readwrite("sync_adjust_degrade_block_ask_count", &TickReplaySummary::sync_adjust_degrade_block_ask_count)
        .def_readwrite("sync_adjust_degrade_until_ms", &TickReplaySummary::sync_adjust_degrade_until_ms)
        .def_readwrite("sync_adjust_censored", &TickReplaySummary::sync_adjust_censored)
        .def_readwrite("sync_adjust_censor_ts_ms", &TickReplaySummary::sync_adjust_censor_ts_ms)
        .def_readwrite("post_only_guard_hits", &TickReplaySummary::post_only_guard_hits)
        .def_readwrite("adverse_guard_count", &TickReplaySummary::adverse_guard_count)
        .def_readwrite("adverse_pause_count", &TickReplaySummary::adverse_pause_count)
        .def_readwrite("adverse_markout_stale_drop_count", &TickReplaySummary::adverse_markout_stale_drop_count)
        .def_readwrite("bid_adverse_markout_pause_extend_count", &TickReplaySummary::bid_adverse_markout_pause_extend_count)
        .def_readwrite("ask_adverse_markout_pause_extend_count", &TickReplaySummary::ask_adverse_markout_pause_extend_count)
        .def_readwrite("defense_guard_count", &TickReplaySummary::defense_guard_count)
        .def_readwrite("defense_pause_count", &TickReplaySummary::defense_pause_count)
        .def_readwrite("pending_cancel_fills", &TickReplaySummary::pending_cancel_fills)
        .def_readwrite("queue_l2_cancel_ahead_event_count", &TickReplaySummary::queue_l2_cancel_ahead_event_count)
        .def_readwrite("queue_l2_cancel_ahead_bid_event_count", &TickReplaySummary::queue_l2_cancel_ahead_bid_event_count)
        .def_readwrite("queue_l2_cancel_ahead_ask_event_count", &TickReplaySummary::queue_l2_cancel_ahead_ask_event_count)
        .def_readwrite("queue_l2_cancel_ahead_qty", &TickReplaySummary::queue_l2_cancel_ahead_qty)
        .def_readwrite("local_extreme_guard_count", &TickReplaySummary::local_extreme_guard_count)
        .def_readwrite("local_extreme_pause_count", &TickReplaySummary::local_extreme_pause_count)
        .def_readwrite("bid_local_extreme_guard_count", &TickReplaySummary::bid_local_extreme_guard_count)
        .def_readwrite("ask_local_extreme_guard_count", &TickReplaySummary::ask_local_extreme_guard_count)
        .def_readwrite("fills_bid_local_extreme_guard", &TickReplaySummary::fills_bid_local_extreme_guard)
        .def_readwrite("fills_ask_local_extreme_guard", &TickReplaySummary::fills_ask_local_extreme_guard)
        .def_readwrite("fragile_ttl_cancel_count", &TickReplaySummary::fragile_ttl_cancel_count)
        .def_readwrite("flat_unilateral_release_count", &TickReplaySummary::flat_unilateral_release_count)
        .def_readwrite("flat_unilateral_bid_release_count", &TickReplaySummary::flat_unilateral_bid_release_count)
        .def_readwrite("flat_unilateral_ask_release_count", &TickReplaySummary::flat_unilateral_ask_release_count)
        .def_readwrite("position_timeout_count", &TickReplaySummary::position_timeout_count)
        .def_readwrite("circuit_breaker_count", &TickReplaySummary::circuit_breaker_count)
        .def_readwrite("risk_daily_loss_block_count", &TickReplaySummary::risk_daily_loss_block_count)
        .def_readwrite("risk_position_value_block_count", &TickReplaySummary::risk_position_value_block_count)
        .def_readwrite("risk_emergency_close_count", &TickReplaySummary::risk_emergency_close_count)
        .def_readwrite("risk_notional_cap_count", &TickReplaySummary::risk_notional_cap_count)
        .def_readwrite("risk_emergency_latched", &TickReplaySummary::risk_emergency_latched)
        .def_readwrite("risk_utc_day", &TickReplaySummary::risk_utc_day)
        .def_readwrite("risk_day_start_total_pnl", &TickReplaySummary::risk_day_start_total_pnl)
        .def_readwrite("risk_session_peak_pnl", &TickReplaySummary::risk_session_peak_pnl)
        .def_readwrite("risk_last_total_pnl", &TickReplaySummary::risk_last_total_pnl)
        .def_readwrite("risk_total_pnl_offset", &TickReplaySummary::risk_total_pnl_offset)
        .def_readwrite("circuit_breaker_close_place_count", &TickReplaySummary::circuit_breaker_close_place_count)
        .def_readwrite("circuit_breaker_close_keep_count", &TickReplaySummary::circuit_breaker_close_keep_count)
        .def_readwrite("circuit_breaker_close_fill_count", &TickReplaySummary::circuit_breaker_close_fill_count)
        .def_readwrite("circuit_breaker_close_gtx_reject_count", &TickReplaySummary::circuit_breaker_close_gtx_reject_count)
        .def_readwrite("circuit_breaker_close_ioc_place_count", &TickReplaySummary::circuit_breaker_close_ioc_place_count)
        .def_readwrite("circuit_breaker_close_ioc_fill_count", &TickReplaySummary::circuit_breaker_close_ioc_fill_count)
        .def_readwrite("circuit_breaker_close_ioc_expire_count", &TickReplaySummary::circuit_breaker_close_ioc_expire_count)
        .def_readwrite("circuit_breaker_closing", &TickReplaySummary::circuit_breaker_closing)
        .def_readwrite("emergency_close_count", &TickReplaySummary::emergency_close_count)
        .def_readwrite("replace_throttle_count", &TickReplaySummary::replace_throttle_count)
        .def_readwrite("replace_throttle_bid_count", &TickReplaySummary::replace_throttle_bid_count)
        .def_readwrite("replace_throttle_ask_count", &TickReplaySummary::replace_throttle_ask_count)
        .def_readwrite("replace_throttle_price_count", &TickReplaySummary::replace_throttle_price_count)
        .def_readwrite("replace_throttle_age_count", &TickReplaySummary::replace_throttle_age_count)
        .def_readwrite("campaign_stop_add_count", &TickReplaySummary::campaign_stop_add_count)
        .def_readwrite("bid_campaign_stop_add_count", &TickReplaySummary::bid_campaign_stop_add_count)
        .def_readwrite("ask_campaign_stop_add_count", &TickReplaySummary::ask_campaign_stop_add_count)
        .def_readwrite("campaign_soft_control_count", &TickReplaySummary::campaign_soft_control_count)
        .def_readwrite("bid_campaign_soft_control_count", &TickReplaySummary::bid_campaign_soft_control_count)
        .def_readwrite("ask_campaign_soft_control_count", &TickReplaySummary::ask_campaign_soft_control_count)
        .def_readwrite("adaptive_add_cooldown_hit_count", &TickReplaySummary::adaptive_add_cooldown_hit_count)
        .def_readwrite("adaptive_add_cooldown_bid_hit_count", &TickReplaySummary::adaptive_add_cooldown_bid_hit_count)
        .def_readwrite("adaptive_add_cooldown_ask_hit_count", &TickReplaySummary::adaptive_add_cooldown_ask_hit_count)
        .def_readwrite("decision_place_count", &TickReplaySummary::decision_place_count)
        .def_readwrite("decision_replace_count", &TickReplaySummary::decision_replace_count)
        .def_readwrite("decision_keep_count", &TickReplaySummary::decision_keep_count)
        .def_readwrite("decision_pause_count", &TickReplaySummary::decision_pause_count)
        .def_readwrite("decision_none_count", &TickReplaySummary::decision_none_count)
        .def_readwrite("decision_pending_coalesce_count", &TickReplaySummary::decision_pending_coalesce_count)
        .def_readwrite("decision_cancel_first_count", &TickReplaySummary::decision_cancel_first_count)
        .def_readwrite(
            "replace_terminal_continuation_terminal_count",
            &TickReplaySummary::replace_terminal_continuation_terminal_count
        )
        .def_readwrite(
            "replace_terminal_continuation_decision_count",
            &TickReplaySummary::replace_terminal_continuation_decision_count
        )
        .def_readwrite(
            "replace_terminal_continuation_bid_decision_count",
            &TickReplaySummary::replace_terminal_continuation_bid_decision_count
        )
        .def_readwrite(
            "replace_terminal_continuation_ask_decision_count",
            &TickReplaySummary::replace_terminal_continuation_ask_decision_count
        )
        .def_readwrite(
            "replace_terminal_continuation_decision_latency_sum_ms",
            &TickReplaySummary::replace_terminal_continuation_decision_latency_sum_ms
        )
        .def_readwrite(
            "replace_terminal_continuation_decision_latency_max_ms",
            &TickReplaySummary::replace_terminal_continuation_decision_latency_max_ms
        )
        .def_readwrite("max_pending_new_orders", &TickReplaySummary::max_pending_new_orders)
        .def_readwrite("max_pending_cancel_orders", &TickReplaySummary::max_pending_cancel_orders)
        .def_readwrite("buy_fill_selection_live_eval_count", &TickReplaySummary::buy_fill_selection_live_eval_count)
        .def_readwrite("buy_fill_selection_live_hit_count", &TickReplaySummary::buy_fill_selection_live_hit_count)
        .def_readwrite("buy_fill_selection_live_score_sum", &TickReplaySummary::buy_fill_selection_live_score_sum)
        .def_readwrite("buy_fill_selection_live_score_max", &TickReplaySummary::buy_fill_selection_live_score_max)
        .def_readwrite("buy_fill_selection_live_score_ge_042", &TickReplaySummary::buy_fill_selection_live_score_ge_042)
        .def_readwrite("buy_fill_selection_live_score_ge_043", &TickReplaySummary::buy_fill_selection_live_score_ge_043)
        .def_readwrite("buy_fill_selection_live_score_ge_044", &TickReplaySummary::buy_fill_selection_live_score_ge_044)
        .def_readwrite("buy_fill_selection_live_score_ge_045", &TickReplaySummary::buy_fill_selection_live_score_ge_045)
        .def_readwrite("buy_soft_widen_release_target_reached_count", &TickReplaySummary::buy_soft_widen_release_target_reached_count)
        .def_readwrite("buy_soft_widen_release_eligible_count", &TickReplaySummary::buy_soft_widen_release_eligible_count)
        .def_readwrite("buy_soft_widen_release_requested_count", &TickReplaySummary::buy_soft_widen_release_requested_count)
        .def_readwrite("buy_soft_widen_release_effective_mult_count", &TickReplaySummary::buy_soft_widen_release_effective_mult_count)
        .def_readwrite("buy_soft_widen_release_effective_price_count", &TickReplaySummary::buy_soft_widen_release_effective_price_count)
        .def_readwrite("buy_soft_widen_release_role_observed", &TickReplaySummary::buy_soft_widen_release_role_observed)
        .def_readwrite("buy_soft_widen_release_reason", &TickReplaySummary::buy_soft_widen_release_reason)
        .def_readwrite("buy_soft_widen_release_baseline_spread_mult", &TickReplaySummary::buy_soft_widen_release_baseline_spread_mult)
        .def_readwrite("buy_soft_widen_release_selected_spread_mult", &TickReplaySummary::buy_soft_widen_release_selected_spread_mult)
        .def_readwrite("buy_soft_widen_release_baseline_bid_price", &TickReplaySummary::buy_soft_widen_release_baseline_bid_price)
        .def_readwrite("buy_soft_widen_release_candidate_bid_price", &TickReplaySummary::buy_soft_widen_release_candidate_bid_price)
        .def_readwrite("p3_reach_gate_eval_count", &TickReplaySummary::p3_reach_gate_eval_count)
        .def_readwrite("p3_reach_gate_toxicity_trigger_count", &TickReplaySummary::p3_reach_gate_toxicity_trigger_count)
        .def_readwrite("p3_reach_gate_supported_count", &TickReplaySummary::p3_reach_gate_supported_count)
        .def_readwrite("p3_reach_gate_pass_count", &TickReplaySummary::p3_reach_gate_pass_count)
        .def_readwrite("p3_reach_gate_price_change_count", &TickReplaySummary::p3_reach_gate_price_change_count)
        .def_readwrite("p3_reach_gate_spread_cap_noop_count", &TickReplaySummary::p3_reach_gate_spread_cap_noop_count)
        .def_readwrite("p3_reach_gate_buy_price_change_count", &TickReplaySummary::p3_reach_gate_buy_price_change_count)
        .def_readwrite("p3_reach_gate_sell_price_change_count", &TickReplaySummary::p3_reach_gate_sell_price_change_count)
        .def_readwrite("p3_reach_budget_bucket_eval_count", &TickReplaySummary::p3_reach_budget_bucket_eval_count)
        .def_readwrite("p3_reach_budget_toxicity_trigger_count", &TickReplaySummary::p3_reach_budget_toxicity_trigger_count)
        .def_readwrite("p3_reach_budget_activation_count", &TickReplaySummary::p3_reach_budget_activation_count)
        .def_readwrite("p3_reach_budget_buy_activation_count", &TickReplaySummary::p3_reach_budget_buy_activation_count)
        .def_readwrite("p3_reach_budget_sell_activation_count", &TickReplaySummary::p3_reach_budget_sell_activation_count)
        .def_readwrite("p3_reach_budget_no_action_count", &TickReplaySummary::p3_reach_budget_no_action_count)
        .def_readwrite("p3_reach_budget_unsupported_count", &TickReplaySummary::p3_reach_budget_unsupported_count)
        .def_readwrite("p3_reach_budget_reuse_count", &TickReplaySummary::p3_reach_budget_reuse_count)
        .def_readwrite("p3_reach_budget_exposure_decision_count", &TickReplaySummary::p3_reach_budget_exposure_decision_count)
        .def_readwrite("p3_reach_budget_price_change_count", &TickReplaySummary::p3_reach_budget_price_change_count)
        .def_readwrite("p3_reach_budget_buy_price_change_count", &TickReplaySummary::p3_reach_budget_buy_price_change_count)
        .def_readwrite("p3_reach_budget_sell_price_change_count", &TickReplaySummary::p3_reach_budget_sell_price_change_count)
        .def_readwrite("p3_reach_budget_hard_safety_suppressed_count", &TickReplaySummary::p3_reach_budget_hard_safety_suppressed_count)
        .def_readwrite("p3_reach_budget_reducing_unchanged_count", &TickReplaySummary::p3_reach_budget_reducing_unchanged_count)
        .def_readwrite("p3_reach_budget_spread_cap_noop_count", &TickReplaySummary::p3_reach_budget_spread_cap_noop_count)
        .def_readwrite("p3_reach_budget_bucket_expiry_count", &TickReplaySummary::p3_reach_budget_bucket_expiry_count)
        .def_readwrite("p3_reach_budget_flat_reset_count", &TickReplaySummary::p3_reach_budget_flat_reset_count)
        .def_readwrite("p3_reach_budget_selected_k_sum", &TickReplaySummary::p3_reach_budget_selected_k_sum)
        .def_readwrite("p3_reach_budget_selected_k_max", &TickReplaySummary::p3_reach_budget_selected_k_max)
        .def_readwrite("p3_reach_budget_active_end_count", &TickReplaySummary::p3_reach_budget_active_end_count)
        .def_readwrite("p3_reach_budget_buy_selected_k_end", &TickReplaySummary::p3_reach_budget_buy_selected_k_end)
        .def_readwrite("p3_reach_budget_sell_selected_k_end", &TickReplaySummary::p3_reach_budget_sell_selected_k_end)
        .def_readwrite("fair_center_eval_count", &TickReplaySummary::fair_center_eval_count)
        .def_readwrite("fair_center_valid_count", &TickReplaySummary::fair_center_valid_count)
        .def_readwrite("fair_center_invalid_count", &TickReplaySummary::fair_center_invalid_count)
        .def_readwrite("fair_center_nonzero_request_count", &TickReplaySummary::fair_center_nonzero_request_count)
        .def_readwrite("fair_center_price_change_count", &TickReplaySummary::fair_center_price_change_count)
        .def_readwrite("fair_center_gtx_clamp_count", &TickReplaySummary::fair_center_gtx_clamp_count)
        .def_readwrite("fair_center_no_pair_support_count", &TickReplaySummary::fair_center_no_pair_support_count)
        .def_readwrite("fair_center_effective_shift_ticks_abs_sum", &TickReplaySummary::fair_center_effective_shift_ticks_abs_sum)
        .def_readwrite("fair_center_effective_shift_ticks_abs_max", &TickReplaySummary::fair_center_effective_shift_ticks_abs_max)
        .def_readwrite("fixed_spread_probe_bid_submitted_orders", &TickReplaySummary::fixed_spread_probe_bid_submitted_orders)
        .def_readwrite("fixed_spread_probe_ask_submitted_orders", &TickReplaySummary::fixed_spread_probe_ask_submitted_orders)
        .def_readwrite("fixed_spread_probe_bid_activation_gtx_rejects", &TickReplaySummary::fixed_spread_probe_bid_activation_gtx_rejects)
        .def_readwrite("fixed_spread_probe_ask_activation_gtx_rejects", &TickReplaySummary::fixed_spread_probe_ask_activation_gtx_rejects)
        .def_readwrite("fixed_spread_probe_bid_placed_orders", &TickReplaySummary::fixed_spread_probe_bid_placed_orders)
        .def_readwrite("fixed_spread_probe_ask_placed_orders", &TickReplaySummary::fixed_spread_probe_ask_placed_orders)
        .def_readwrite("fixed_spread_probe_bid_queue_visible_positive_orders", &TickReplaySummary::fixed_spread_probe_bid_queue_visible_positive_orders)
        .def_readwrite("fixed_spread_probe_ask_queue_visible_positive_orders", &TickReplaySummary::fixed_spread_probe_ask_queue_visible_positive_orders)
        .def_readwrite("fixed_spread_probe_bid_queue_known_zero_orders", &TickReplaySummary::fixed_spread_probe_bid_queue_known_zero_orders)
        .def_readwrite("fixed_spread_probe_ask_queue_known_zero_orders", &TickReplaySummary::fixed_spread_probe_ask_queue_known_zero_orders)
        .def_readwrite("fixed_spread_probe_bid_queue_fallback_orders", &TickReplaySummary::fixed_spread_probe_bid_queue_fallback_orders)
        .def_readwrite("fixed_spread_probe_ask_queue_fallback_orders", &TickReplaySummary::fixed_spread_probe_ask_queue_fallback_orders)
        .def_readwrite("fixed_spread_probe_bid_active_touched_orders", &TickReplaySummary::fixed_spread_probe_bid_active_touched_orders)
        .def_readwrite("fixed_spread_probe_ask_active_touched_orders", &TickReplaySummary::fixed_spread_probe_ask_active_touched_orders)
        .def_readwrite("fixed_spread_probe_bid_filled_orders", &TickReplaySummary::fixed_spread_probe_bid_filled_orders)
        .def_readwrite("fixed_spread_probe_ask_filled_orders", &TickReplaySummary::fixed_spread_probe_ask_filled_orders)
        .def_readwrite("fixed_spread_probe_bid_fully_filled_orders", &TickReplaySummary::fixed_spread_probe_bid_fully_filled_orders)
        .def_readwrite("fixed_spread_probe_ask_fully_filled_orders", &TickReplaySummary::fixed_spread_probe_ask_fully_filled_orders)
        .def_readwrite("fixed_spread_probe_bid_first_fill_pending_cancel_orders", &TickReplaySummary::fixed_spread_probe_bid_first_fill_pending_cancel_orders)
        .def_readwrite("fixed_spread_probe_ask_first_fill_pending_cancel_orders", &TickReplaySummary::fixed_spread_probe_ask_first_fill_pending_cancel_orders)
        .def_readwrite("fixed_spread_probe_bid_filled_within_1s", &TickReplaySummary::fixed_spread_probe_bid_filled_within_1s)
        .def_readwrite("fixed_spread_probe_ask_filled_within_1s", &TickReplaySummary::fixed_spread_probe_ask_filled_within_1s)
        .def_readwrite("fixed_spread_probe_bid_filled_within_5s", &TickReplaySummary::fixed_spread_probe_bid_filled_within_5s)
        .def_readwrite("fixed_spread_probe_ask_filled_within_5s", &TickReplaySummary::fixed_spread_probe_ask_filled_within_5s)
        .def_readwrite("fixed_spread_probe_bid_filled_within_10s", &TickReplaySummary::fixed_spread_probe_bid_filled_within_10s)
        .def_readwrite("fixed_spread_probe_ask_filled_within_10s", &TickReplaySummary::fixed_spread_probe_ask_filled_within_10s)
        .def_readwrite("fixed_spread_probe_bid_end_censored_unfilled", &TickReplaySummary::fixed_spread_probe_bid_end_censored_unfilled)
        .def_readwrite("fixed_spread_probe_ask_end_censored_unfilled", &TickReplaySummary::fixed_spread_probe_ask_end_censored_unfilled)
        .def_readwrite("fixed_spread_probe_bid_end_censored_before_1s", &TickReplaySummary::fixed_spread_probe_bid_end_censored_before_1s)
        .def_readwrite("fixed_spread_probe_ask_end_censored_before_1s", &TickReplaySummary::fixed_spread_probe_ask_end_censored_before_1s)
        .def_readwrite("fixed_spread_probe_bid_end_censored_before_5s", &TickReplaySummary::fixed_spread_probe_bid_end_censored_before_5s)
        .def_readwrite("fixed_spread_probe_ask_end_censored_before_5s", &TickReplaySummary::fixed_spread_probe_ask_end_censored_before_5s)
        .def_readwrite("fixed_spread_probe_bid_end_censored_before_10s", &TickReplaySummary::fixed_spread_probe_bid_end_censored_before_10s)
        .def_readwrite("fixed_spread_probe_ask_end_censored_before_10s", &TickReplaySummary::fixed_spread_probe_ask_end_censored_before_10s)
        .def_readwrite("fixed_spread_probe_bid_fill_qty", &TickReplaySummary::fixed_spread_probe_bid_fill_qty)
        .def_readwrite("fixed_spread_probe_ask_fill_qty", &TickReplaySummary::fixed_spread_probe_ask_fill_qty);

    py::class_<PairedFixedSpreadProbeRow>(m, "PairedFixedSpreadProbeRow")
        .def(py::init<>())
        .def_property_readonly(
            "side",
            [](const PairedFixedSpreadProbeRow& row) {
                return std::string(side_name(row.side));
            })
        .def_readwrite("distance_ticks", &PairedFixedSpreadProbeRow::distance_ticks)
        .def_readwrite("submitted_orders", &PairedFixedSpreadProbeRow::submitted_orders)
        .def_readwrite("activation_gtx_rejects", &PairedFixedSpreadProbeRow::activation_gtx_rejects)
        .def_readwrite("cancelled_before_activation", &PairedFixedSpreadProbeRow::cancelled_before_activation)
        .def_readwrite("placed_orders", &PairedFixedSpreadProbeRow::placed_orders)
        .def_readwrite("queue_visible_positive_orders", &PairedFixedSpreadProbeRow::queue_visible_positive_orders)
        .def_readwrite("queue_known_zero_orders", &PairedFixedSpreadProbeRow::queue_known_zero_orders)
        .def_readwrite("queue_fallback_orders", &PairedFixedSpreadProbeRow::queue_fallback_orders)
        .def_readwrite("exact_touched_orders", &PairedFixedSpreadProbeRow::exact_touched_orders)
        .def_readwrite("through_touched_orders", &PairedFixedSpreadProbeRow::through_touched_orders)
        .def_readwrite("any_touched_orders", &PairedFixedSpreadProbeRow::any_touched_orders)
        .def_readwrite("filled_orders", &PairedFixedSpreadProbeRow::filled_orders)
        .def_readwrite("fully_filled_orders", &PairedFixedSpreadProbeRow::fully_filled_orders)
        .def_readwrite("filled_via_exact_orders", &PairedFixedSpreadProbeRow::filled_via_exact_orders)
        .def_readwrite("filled_via_through_orders", &PairedFixedSpreadProbeRow::filled_via_through_orders)
        .def_readwrite("through_forced_fill_orders", &PairedFixedSpreadProbeRow::through_forced_fill_orders)
        .def_readwrite("first_fill_pending_cancel_orders", &PairedFixedSpreadProbeRow::first_fill_pending_cancel_orders)
        .def_readwrite("filled_within_1s", &PairedFixedSpreadProbeRow::filled_within_1s)
        .def_readwrite("filled_within_5s", &PairedFixedSpreadProbeRow::filled_within_5s)
        .def_readwrite("filled_within_10s", &PairedFixedSpreadProbeRow::filled_within_10s)
        .def_readwrite("cancelled_unfilled_orders", &PairedFixedSpreadProbeRow::cancelled_unfilled_orders)
        .def_readwrite("observed_lifecycle_orders", &PairedFixedSpreadProbeRow::observed_lifecycle_orders)
        .def_readwrite("observed_1s_orders", &PairedFixedSpreadProbeRow::observed_1s_orders)
        .def_readwrite("observed_5s_orders", &PairedFixedSpreadProbeRow::observed_5s_orders)
        .def_readwrite("observed_10s_orders", &PairedFixedSpreadProbeRow::observed_10s_orders)
        .def_readwrite("end_censored_orders", &PairedFixedSpreadProbeRow::end_censored_orders)
        .def_readwrite("end_censored_before_1s", &PairedFixedSpreadProbeRow::end_censored_before_1s)
        .def_readwrite("end_censored_before_5s", &PairedFixedSpreadProbeRow::end_censored_before_5s)
        .def_readwrite("end_censored_before_10s", &PairedFixedSpreadProbeRow::end_censored_before_10s)
        .def_readwrite("fill_qty", &PairedFixedSpreadProbeRow::fill_qty);

    py::class_<PairedFixedSpreadViolationRow>(m, "PairedFixedSpreadViolationRow")
        .def(py::init<>())
        .def_readwrite("cohort_id", &PairedFixedSpreadViolationRow::cohort_id)
        .def_property_readonly(
            "side",
            [](const PairedFixedSpreadViolationRow& row) {
                return std::string(side_name(row.side));
            })
        .def_readwrite("event_ts_ms", &PairedFixedSpreadViolationRow::event_ts_ms)
        .def_readwrite(
            "shallower_distance_ticks",
            &PairedFixedSpreadViolationRow::shallower_distance_ticks)
        .def_readwrite(
            "deeper_distance_ticks",
            &PairedFixedSpreadViolationRow::deeper_distance_ticks);

    py::class_<TraceOrderRow>(m, "TraceOrderRow")
        .def(py::init<>())
        .def_readwrite("order_id", &TraceOrderRow::order_id)
        .def_property_readonly(
            "side", [](const TraceOrderRow& row) { return std::string(side_name(row.side)); })
        .def_readwrite("submit_ts", &TraceOrderRow::submit_ts)
        .def_readwrite("activate_ts", &TraceOrderRow::activate_ts)
        .def_readwrite("quote_ts", &TraceOrderRow::quote_ts)
        .def_readwrite("price", &TraceOrderRow::price)
        .def_readwrite("quantity", &TraceOrderRow::quantity)
        .def_readwrite("raw_half_spread", &TraceOrderRow::raw_half_spread)
        .def_readwrite("capped_half_spread", &TraceOrderRow::capped_half_spread)
        .def_readwrite("raw_mid_shift", &TraceOrderRow::raw_mid_shift)
        .def_readwrite("raw_reservation_shift", &TraceOrderRow::raw_reservation_shift)
        .def_readwrite("raw_asym_shift", &TraceOrderRow::raw_asym_shift)
        .def_readwrite("asym", &TraceOrderRow::asym)
        .def_readwrite("inventory", &TraceOrderRow::inventory)
        .def_readwrite("dir_signal", &TraceOrderRow::dir_signal)
        .def_readwrite("pred_dir", &TraceOrderRow::pred_dir)
        .def_readwrite("pred_ret", &TraceOrderRow::pred_ret)
        .def_readwrite("tox_bid", &TraceOrderRow::tox_bid)
        .def_readwrite("tox_ask", &TraceOrderRow::tox_ask)
        .def_readwrite("book_imb", &TraceOrderRow::book_imb)
        .def_readwrite("microprice_shift_bps", &TraceOrderRow::microprice_shift_bps)
        .def_readwrite("near_depth_total", &TraceOrderRow::near_depth_total)
        .def_readwrite("l2_near_depth_total", &TraceOrderRow::l2_near_depth_total)
        .def_readwrite("l2_quote_flip_rate", &TraceOrderRow::l2_quote_flip_rate)
        .def_readwrite("l2_book_refresh_ratio", &TraceOrderRow::l2_book_refresh_ratio)
        .def_readwrite("l2_book_cancel_ratio", &TraceOrderRow::l2_book_cancel_ratio)
        .def_readwrite("mo_ema_bid", &TraceOrderRow::mo_ema_bid)
        .def_readwrite("mo_ema_ask", &TraceOrderRow::mo_ema_ask)
        .def_readwrite("fair", &TraceOrderRow::fair)
        .def_readwrite("mid", &TraceOrderRow::mid)
        .def_readwrite("best_bid", &TraceOrderRow::best_bid)
        .def_readwrite("best_ask", &TraceOrderRow::best_ask)
        .def_readwrite("raw_pair_spread", &TraceOrderRow::raw_pair_spread)
        .def_readwrite("capped_pair_spread", &TraceOrderRow::capped_pair_spread)
        .def_readwrite("final_pair_spread", &TraceOrderRow::final_pair_spread)
        .def_readwrite("raw_price", &TraceOrderRow::raw_price)
        .def_readwrite("pre_guard_price", &TraceOrderRow::pre_guard_price)
        .def_readwrite("final_price", &TraceOrderRow::final_price)
        .def_readwrite("raw_quote_delta_to_bbo", &TraceOrderRow::raw_quote_delta_to_bbo)
        .def_readwrite("pre_guard_delta_to_bbo", &TraceOrderRow::pre_guard_delta_to_bbo)
        .def_readwrite("final_quote_delta_to_bbo", &TraceOrderRow::final_quote_delta_to_bbo)
        .def_readwrite("raw_distance_to_mid", &TraceOrderRow::raw_distance_to_mid)
        .def_readwrite("final_distance_to_mid", &TraceOrderRow::final_distance_to_mid)
        .def_readwrite("raw_quote_skew", &TraceOrderRow::raw_quote_skew)
        .def_readwrite("final_quote_skew", &TraceOrderRow::final_quote_skew)
        .def_property_readonly(
            "raw_bias_side",
            [](const TraceOrderRow& row) { return std::string(bias_side_name(row.raw_bias_side)); })
        .def_property_readonly(
            "final_bias_side",
            [](const TraceOrderRow& row) { return std::string(bias_side_name(row.final_bias_side)); })
        .def_readwrite("favored_by_raw_shift", &TraceOrderRow::favored_by_raw_shift)
        .def_readwrite("delta_cap", &TraceOrderRow::delta_cap)
        .def_readwrite("mid_guard", &TraceOrderRow::mid_guard)
        .def_readwrite("post_only", &TraceOrderRow::post_only)
        .def_readwrite("side_adverse", &TraceOrderRow::side_adverse)
        .def_readwrite("side_adverse_pause", &TraceOrderRow::side_adverse_pause)
        .def_readwrite("adverse_toxicity", &TraceOrderRow::adverse_toxicity)
        .def_readwrite("adverse_markout", &TraceOrderRow::adverse_markout)
        .def_readwrite("adverse_direction", &TraceOrderRow::adverse_direction)
        .def_readwrite("adverse_ret", &TraceOrderRow::adverse_ret)
        .def_readwrite("adverse_microprice", &TraceOrderRow::adverse_microprice)
        .def_readwrite("adverse_thin_depth", &TraceOrderRow::adverse_thin_depth)
        .def_readwrite("local_extreme_guard", &TraceOrderRow::local_extreme_guard)
        .def_readwrite("local_extreme_pause", &TraceOrderRow::local_extreme_pause)
        .def_readwrite("local_extreme_rank", &TraceOrderRow::local_extreme_rank)
        .def_readwrite("local_extreme_window_s", &TraceOrderRow::local_extreme_window_s)
        .def_readwrite("defense_guard", &TraceOrderRow::defense_guard)
        .def_readwrite("defense_pause", &TraceOrderRow::defense_pause)
        .def_readwrite("defense_reducing", &TraceOrderRow::defense_reducing)
        .def_readwrite("defense_emergency", &TraceOrderRow::defense_emergency)
        .def_readwrite("defense_markout", &TraceOrderRow::defense_markout)
        .def_readwrite("defense_direction", &TraceOrderRow::defense_direction)
        .def_readwrite("defense_ret", &TraceOrderRow::defense_ret)
        .def_readwrite("defense_microprice", &TraceOrderRow::defense_microprice)
        .def_readwrite("defense_spread_mult", &TraceOrderRow::defense_spread_mult)
        .def_readwrite("final_compressed", &TraceOrderRow::final_compressed)
        .def_readwrite("bid_adverse", &TraceOrderRow::bid_adverse)
        .def_readwrite("ask_adverse", &TraceOrderRow::ask_adverse)
        .def_readwrite("buy_fill_selection_live_score", &TraceOrderRow::buy_fill_selection_live_score)
        .def_readwrite("buy_fill_selection_live_hit", &TraceOrderRow::buy_fill_selection_live_hit)
        .def_readwrite(
            "buy_fill_selection_live_missing_features",
            &TraceOrderRow::buy_fill_selection_live_missing_features)
        .def_readwrite("random_passive_mirrored", &TraceOrderRow::random_passive_mirrored)
        .def_readwrite("final_guard_changed", &TraceOrderRow::final_guard_changed)
        .def_readwrite("any_constraint_changed", &TraceOrderRow::any_constraint_changed)
        .def_property_readonly(
            "outcome",
            [](const TraceOrderRow& row) { return std::string(trace_outcome_name(row.outcome)); })
        .def_readwrite("outcome_ts", &TraceOrderRow::outcome_ts)
        .def_readwrite("lifetime_ms", &TraceOrderRow::lifetime_ms)
        .def_property_readonly(
            "cancel_reason",
            [](const TraceOrderRow& row) { return std::string(cancel_reason_name(row.cancel_reason)); })
        .def_readwrite("fill_qty", &TraceOrderRow::fill_qty)
        .def_readwrite("remaining", &TraceOrderRow::remaining)
        .def_readwrite("queue_init", &TraceOrderRow::queue_init)
        .def_readwrite("queue_left", &TraceOrderRow::queue_left)
        .def_readwrite("pending_cancel", &TraceOrderRow::pending_cancel);

    py::class_<P3ReachDecisionRow>(m, "P3ReachDecisionRow")
        .def(py::init<>())
        .def_readwrite("decision_ts_ms", &P3ReachDecisionRow::decision_ts_ms)
        .def_readwrite("prediction_ts_ms", &P3ReachDecisionRow::prediction_ts_ms)
        .def_property_readonly(
            "side", [](const P3ReachDecisionRow& row) { return std::string(side_name(row.side)); })
        .def_property_readonly(
            "role", [](const P3ReachDecisionRow& row) { return row.opener ? "opener" : "add"; })
        .def_readwrite("exposure_increasing", &P3ReachDecisionRow::exposure_increasing)
        .def_readwrite("baseline_eligible", &P3ReachDecisionRow::baseline_eligible)
        .def_readwrite("toxicity_score", &P3ReachDecisionRow::toxicity_score)
        .def_readwrite("toxicity_threshold", &P3ReachDecisionRow::toxicity_threshold)
        .def_readwrite("best_bid", &P3ReachDecisionRow::best_bid)
        .def_readwrite("best_ask", &P3ReachDecisionRow::best_ask)
        .def_readwrite("inventory_btc", &P3ReachDecisionRow::inventory_btc)
        .def_readwrite("baseline_price", &P3ReachDecisionRow::baseline_price)
        .def_readwrite("candidate_price", &P3ReachDecisionRow::candidate_price)
        .def_readwrite("side_policy_spread_mult", &P3ReachDecisionRow::side_policy_spread_mult)
        .def_readwrite("side_policy_allow_post", &P3ReachDecisionRow::side_policy_allow_post)
        .def_readwrite("side_policy_allow_exposure_increase", &P3ReachDecisionRow::side_policy_allow_exposure_increase)
        .def_readwrite("side_policy_reason_mask", &P3ReachDecisionRow::side_policy_reason_mask)
        .def_readwrite("baseline_distance_ticks", &P3ReachDecisionRow::baseline_distance_ticks)
        .def_readwrite("reach_gate_status", &P3ReachDecisionRow::reach_gate_status)
        .def_readwrite("reach_gate_requested", &P3ReachDecisionRow::reach_gate_requested)
        .def_readwrite("price_changed", &P3ReachDecisionRow::price_changed)
        .def_readwrite("spread_cap_noop", &P3ReachDecisionRow::spread_cap_noop);

    py::class_<CooldownDurationOpportunityRow>(
        m,
        "CooldownDurationOpportunityRow")
        .def(py::init<>())
        .def_readwrite(
            "schema_version",
            &CooldownDurationOpportunityRow::schema_version)
        .def_readwrite(
            "fill_clock_semantics",
            &CooldownDurationOpportunityRow::fill_clock_semantics)
        .def_readwrite(
            "live_receive_time_authority",
            &CooldownDurationOpportunityRow::live_receive_time_authority)
        .def_readwrite(
            "exposure_fill_ordinal",
            &CooldownDurationOpportunityRow::exposure_fill_ordinal)
        .def_readwrite(
            "fill_visible_ts_ms",
            &CooldownDurationOpportunityRow::fill_visible_ts_ms)
        .def_readwrite(
            "fill_exchange_ts_ms",
            &CooldownDurationOpportunityRow::fill_exchange_ts_ms)
        .def_property_readonly(
            "side",
            [](const CooldownDurationOpportunityRow& row) {
                return std::string(side_name(row.side));
            })
        .def_property_readonly(
            "role_at_fill",
            [](const CooldownDurationOpportunityRow& row) {
                return row.opener ? "opener" : "add";
            })
        .def_readwrite("order_id", &CooldownDurationOpportunityRow::order_id)
        .def_readwrite(
            "campaign_id",
            &CooldownDurationOpportunityRow::campaign_id)
        .def_readwrite(
            "inventory_before_fill_btc",
            &CooldownDurationOpportunityRow::inventory_before_fill_btc)
        .def_readwrite(
            "inventory_after_fill_btc",
            &CooldownDurationOpportunityRow::inventory_after_fill_btc)
        .def_readwrite(
            "fill_qty_btc",
            &CooldownDurationOpportunityRow::fill_qty_btc)
        .def_readwrite(
            "unit_qty_btc",
            &CooldownDurationOpportunityRow::unit_qty_btc)
        .def_readwrite(
            "consecutive_units_before",
            &CooldownDurationOpportunityRow::consecutive_units_before)
        .def_readwrite(
            "consecutive_units_after",
            &CooldownDurationOpportunityRow::consecutive_units_after)
        .def_readwrite(
            "prior_deadline_ts_ms",
            &CooldownDurationOpportunityRow::prior_deadline_ts_ms)
        .def_readwrite(
            "baseline_duration_ms",
            &CooldownDurationOpportunityRow::baseline_duration_ms)
        .def_readwrite(
            "baseline_deadline_ts_ms",
            &CooldownDurationOpportunityRow::baseline_deadline_ts_ms)
        .def_readwrite(
            "canonical_mid",
            &CooldownDurationOpportunityRow::canonical_mid)
        .def_readwrite("best_bid", &CooldownDurationOpportunityRow::best_bid)
        .def_readwrite("best_ask", &CooldownDurationOpportunityRow::best_ask)
        .def_readwrite(
            "decision_visible_bbo_index",
            &CooldownDurationOpportunityRow::decision_visible_bbo_index)
        .def_readwrite(
            "decision_visible_l2_index",
            &CooldownDurationOpportunityRow::decision_visible_l2_index)
        .def_readwrite(
            "market_event_index",
            &CooldownDurationOpportunityRow::market_event_index)
        .def_readwrite(
            "assignment_equity_usdc",
            &CooldownDurationOpportunityRow::assignment_equity_usdc);

    py::class_<CooldownDurationFillPathRow>(
        m,
        "CooldownDurationFillPathRow")
        .def(py::init<>())
        .def_readwrite(
            "path_fill_ordinal",
            &CooldownDurationFillPathRow::path_fill_ordinal)
        .def_readwrite(
            "fill_visible_ts_ms",
            &CooldownDurationFillPathRow::fill_visible_ts_ms)
        .def_property_readonly(
            "side",
            [](const CooldownDurationFillPathRow& row) {
                return std::string(side_name(row.side));
            })
        .def_readwrite("order_id", &CooldownDurationFillPathRow::order_id)
        .def_readwrite("campaign_id", &CooldownDurationFillPathRow::campaign_id)
        .def_readwrite(
            "exposure_increasing",
            &CooldownDurationFillPathRow::exposure_increasing)
        .def_readwrite("target_fill", &CooldownDurationFillPathRow::target_fill)
        .def_readwrite(
            "fill_price_usdc_per_btc",
            &CooldownDurationFillPathRow::fill_price_usdc_per_btc)
        .def_readwrite("fill_qty_btc", &CooldownDurationFillPathRow::fill_qty_btc)
        .def_readwrite(
            "inventory_before_fill_btc",
            &CooldownDurationFillPathRow::inventory_before_fill_btc)
        .def_readwrite(
            "inventory_after_fill_btc",
            &CooldownDurationFillPathRow::inventory_after_fill_btc)
        .def_readwrite(
            "cash_after_fill_usdc",
            &CooldownDurationFillPathRow::cash_after_fill_usdc)
        .def_readwrite(
            "baseline_duration_ms",
            &CooldownDurationFillPathRow::baseline_duration_ms)
        .def_readwrite(
            "applied_duration_ms",
            &CooldownDurationFillPathRow::applied_duration_ms)
        .def_readwrite(
            "applied_deadline_ts_ms",
            &CooldownDurationFillPathRow::applied_deadline_ts_ms);

    py::class_<CooldownDurationForkTrace>(m, "CooldownDurationForkTrace")
        .def(py::init<>())
        .def_readwrite("enabled", &CooldownDurationForkTrace::enabled)
        .def_readwrite("schema_version", &CooldownDurationForkTrace::schema_version)
        .def_readwrite("action", &CooldownDurationForkTrace::action)
        .def_property_readonly(
            "side",
            [](const CooldownDurationForkTrace& trace) {
                return std::string(side_name(trace.side));
            })
        .def_readwrite("campaign_id", &CooldownDurationForkTrace::campaign_id)
        .def_readwrite(
            "target_exposure_fill_ordinal",
            &CooldownDurationForkTrace::target_exposure_fill_ordinal)
        .def_readwrite("target_order_id", &CooldownDurationForkTrace::target_order_id)
        .def_readwrite("assignment_ts_ms", &CooldownDurationForkTrace::assignment_ts_ms)
        .def_readwrite(
            "assignment_inventory_btc",
            &CooldownDurationForkTrace::assignment_inventory_btc)
        .def_readwrite(
            "assignment_equity_usdc",
            &CooldownDurationForkTrace::assignment_equity_usdc)
        .def_readwrite(
            "baseline_duration_ms",
            &CooldownDurationForkTrace::baseline_duration_ms)
        .def_readwrite(
            "applied_duration_ms",
            &CooldownDurationForkTrace::applied_duration_ms)
        .def_readwrite(
            "applied_deadline_ts_ms",
            &CooldownDurationForkTrace::applied_deadline_ts_ms)
        .def_readwrite(
            "exact_owner_baseline_policy_enabled",
            &CooldownDurationForkTrace::exact_owner_baseline_policy_enabled)
        .def_readwrite(
            "exact_owner_action",
            &CooldownDurationForkTrace::exact_owner_action)
        .def_readwrite(
            "exact_owner_policy_sha256",
            &CooldownDurationForkTrace::exact_owner_policy_sha256)
        .def_readwrite(
            "exact_owner_baseline_duration_ms",
            &CooldownDurationForkTrace::exact_owner_baseline_duration_ms)
        .def_readwrite(
            "quarantine_entered",
            &CooldownDurationForkTrace::quarantine_entered)
        .def_readwrite("quarantine_ts_ms", &CooldownDurationForkTrace::quarantine_ts_ms)
        .def_readwrite("washout_protocol", &CooldownDurationForkTrace::washout_protocol)
        .def_readwrite(
            "control_path_exact_until_quarantine",
            &CooldownDurationForkTrace::control_path_exact_until_quarantine)
        .def_readwrite(
            "exposure_permission_change_count",
            &CooldownDurationForkTrace::exposure_permission_change_count)
        .def_readwrite(
            "reducing_permission_control_checks",
            &CooldownDurationForkTrace::reducing_permission_control_checks)
        .def_readwrite(
            "reducing_quote_change_count",
            &CooldownDurationForkTrace::reducing_quote_change_count)
        .def_readwrite(
            "second_assignment_count",
            &CooldownDurationForkTrace::second_assignment_count)
        .def_readwrite(
            "arm_washout_complete",
            &CooldownDurationForkTrace::arm_washout_complete)
        .def_readwrite("terminal_ts_ms", &CooldownDurationForkTrace::terminal_ts_ms)
        .def_readwrite("terminal_reason", &CooldownDurationForkTrace::terminal_reason)
        .def_readwrite("right_censored", &CooldownDurationForkTrace::right_censored)
        .def_readwrite(
            "terminal_inventory_btc",
            &CooldownDurationForkTrace::terminal_inventory_btc)
        .def_readwrite(
            "terminal_mid_usdc_per_btc",
            &CooldownDurationForkTrace::terminal_mid_usdc_per_btc)
        .def_readwrite("final_cash_usdc", &CooldownDurationForkTrace::final_cash_usdc)
        .def_readwrite("final_pnl_usdc", &CooldownDurationForkTrace::final_pnl_usdc)
        .def_readwrite(
            "accounting_residual_usdc",
            &CooldownDurationForkTrace::accounting_residual_usdc)
        .def_readwrite(
            "assignment_to_washout_value_usdc",
            &CooldownDurationForkTrace::assignment_to_washout_value_usdc)
        .def_readwrite(
            "censor_time_mid_mark_usdc",
            &CooldownDurationForkTrace::censor_time_mid_mark_usdc)
        .def_readwrite(
            "censor_time_executable_mark_usdc",
            &CooldownDurationForkTrace::censor_time_executable_mark_usdc)
        .def_readwrite(
            "censor_marks_are_terminal_bounds",
            &CooldownDurationForkTrace::censor_marks_are_terminal_bounds)
        .def_readwrite(
            "post_assignment_buy_fill_count",
            &CooldownDurationForkTrace::post_assignment_buy_fill_count)
        .def_readwrite(
            "post_assignment_sell_fill_count",
            &CooldownDurationForkTrace::post_assignment_sell_fill_count)
        .def_readwrite(
            "inventory_time_btc_s",
            &CooldownDurationForkTrace::inventory_time_btc_s)
        .def_readwrite("mae_usdc", &CooldownDurationForkTrace::mae_usdc)
        .def_readwrite(
            "max_abs_inventory_btc",
            &CooldownDurationForkTrace::max_abs_inventory_btc)
        .def_readwrite(
            "active_or_pending_order_count",
            &CooldownDurationForkTrace::active_or_pending_order_count)
        .def_readwrite(
            "pending_submit_count",
            &CooldownDurationForkTrace::pending_submit_count)
        .def_readwrite(
            "pending_cancel_count",
            &CooldownDurationForkTrace::pending_cancel_count)
        .def_readwrite(
            "pending_ack_count",
            &CooldownDurationForkTrace::pending_ack_count)
        .def_readwrite(
            "campaign_active",
            &CooldownDurationForkTrace::campaign_active)
        .def_readwrite(
            "cursor_owner_count",
            &CooldownDurationForkTrace::cursor_owner_count)
        .def_readwrite(
            "hazard_owner_count",
            &CooldownDurationForkTrace::hazard_owner_count);

    py::class_<TraceFillRow>(m, "TraceFillRow")
        .def(py::init<>())
        .def_readwrite("fill_sequence", &TraceFillRow::fill_sequence)
        .def_property_readonly(
            "side", [](const TraceFillRow& row) { return std::string(side_name(row.side)); })
        .def_readwrite("fill_ts", &TraceFillRow::fill_ts)
        .def_readwrite("quote_ts", &TraceFillRow::quote_ts)
        .def_readwrite("age_ms", &TraceFillRow::age_ms)
        .def_readwrite("quote_mid", &TraceFillRow::quote_mid)
        .def_readwrite("quote_px", &TraceFillRow::quote_px)
        .def_readwrite("fill_trade_px", &TraceFillRow::fill_trade_px)
        .def_readwrite("quote_dist", &TraceFillRow::quote_dist)
        .def_readwrite("quote_window_extreme", &TraceFillRow::quote_window_extreme)
        .def_readwrite("quote_window_move", &TraceFillRow::quote_window_move)
        .def_readwrite("window5_min", &TraceFillRow::window5_min)
        .def_readwrite("window5_max", &TraceFillRow::window5_max)
        .def_readwrite("window10_min", &TraceFillRow::window10_min)
        .def_readwrite("window10_max", &TraceFillRow::window10_max)
        .def_readwrite("window120_min", &TraceFillRow::window120_min)
        .def_readwrite("window120_max", &TraceFillRow::window120_max)
        .def_readwrite("window120_rank", &TraceFillRow::window120_rank)
        .def_readwrite("move_from_quote_mid_to_fill", &TraceFillRow::move_from_quote_mid_to_fill)
        .def_readwrite("queue_init", &TraceFillRow::queue_init)
        .def_readwrite("queue_before", &TraceFillRow::queue_before)
        .def_readwrite("rem_before", &TraceFillRow::rem_before)
        .def_readwrite("fill_qty", &TraceFillRow::fill_qty)
        .def_readwrite("fill_fee_rate", &TraceFillRow::fill_fee_rate)
        .def_readwrite("fill_fee_usdc", &TraceFillRow::fill_fee_usdc)
        .def_readwrite("inventory_before_fill", &TraceFillRow::inventory_before_fill)
        .def_readwrite("inventory_after_fill", &TraceFillRow::inventory_after_fill)
        .def_readwrite("markout_1s", &TraceFillRow::markout_1s)
        .def_readwrite("markout_5s", &TraceFillRow::markout_5s)
        .def_readwrite("markout_20s", &TraceFillRow::markout_20s)
        .def_readwrite("markout_30s", &TraceFillRow::markout_30s)
        .def_readwrite("ev_1s", &TraceFillRow::ev_1s)
        .def_readwrite("ev_5s", &TraceFillRow::ev_5s)
        .def_readwrite("ev_20s", &TraceFillRow::ev_20s)
        .def_readwrite("ev_30s", &TraceFillRow::ev_30s)
        .def_readwrite("toxic_1s", &TraceFillRow::toxic_1s)
        .def_readwrite("toxic_5s", &TraceFillRow::toxic_5s)
        .def_readwrite("toxic_20s", &TraceFillRow::toxic_20s)
        .def_readwrite("toxic_30s", &TraceFillRow::toxic_30s)
        .def_readwrite("order", &TraceFillRow::order);

    py::class_<TickReplayResult>(m, "TickReplayResult")
        .def(py::init<>())
        .def_readwrite("summary", &TickReplayResult::summary)
        .def_readwrite("pnl_ts_ms", &TickReplayResult::pnl_ts_ms)
        .def_readwrite("pnl", &TickReplayResult::pnl)
        .def_readwrite("inventory", &TickReplayResult::inventory)
        .def_readwrite("quote_trace", &TickReplayResult::quote_trace)
        .def_readwrite("fill_trace", &TickReplayResult::fill_trace)
        .def_readwrite("p3_reach_decision_trace", &TickReplayResult::p3_reach_decision_trace)
        .def_readwrite(
            "cooldown_duration_opportunity_trace",
            &TickReplayResult::cooldown_duration_opportunity_trace)
        .def_readwrite(
            "cooldown_duration_fill_path",
            &TickReplayResult::cooldown_duration_fill_path)
        .def_readwrite(
            "cooldown_duration_fork_trace",
            &TickReplayResult::cooldown_duration_fork_trace)
        .def_readwrite(
            "f05_repeated_cooldown_decisions",
            &TickReplayResult::f05_repeated_cooldown_decisions)
        .def_readwrite(
            "f05_repeated_cooldown_checkpoint",
            &TickReplayResult::f05_repeated_cooldown_checkpoint)
        .def_readwrite(
            "paired_fixed_spread_rows",
            &TickReplayResult::paired_fixed_spread_rows)
        .def_readwrite(
            "paired_fixed_spread_violations",
            &TickReplayResult::paired_fixed_spread_violations);

    m.def(
        "sample_keyed_latency_ms",
        &sample_keyed_latency_ms,
        py::arg("base_ms"),
        py::arg("jitter_ms"),
        py::arg("samples"),
        py::arg("seed"),
        py::arg("event_ts_ms"),
        py::arg("is_buy"),
        py::arg("operation"),
        py::arg("order_ts_ms"),
        py::arg("stress_enabled") = false,
        py::arg("stress_spike_probability") = 0.0,
        py::arg("stress_spike_multiplier") = 1.0
    );

    m.def(
        "sample_keyed_random_passive_unit",
        &sample_keyed_random_passive_unit,
        py::arg("seed"),
        py::arg("event_ts_ms"),
        py::arg("action_identity"),
        py::arg("operation")
    );

    m.def(
        "simulate_tick_arrays",
        [](CArray<std::int64_t> trade_ts_ms,
           CArray<double> trade_price,
           CArray<double> trade_qty,
           CArray<std::uint8_t> is_buyer_maker,
           const TickReplayParams& params) {
            // 最小 replay 入口只用于 smoke/parity；正式 daily replay 应优先走带特征、BBO/L2、
            // quote-EV 数组的 ext_policy 入口，避免误以为 bare trade replay 能代表 live。
            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("trade_ts_ms"),
        py::arg("trade_price"),
        py::arg("trade_qty"),
        py::arg("is_buyer_maker"),
        py::arg("params")
    );

    m.def(
        "simulate_tick_arrays_ext_policy_v3",
        [](CArray<std::int64_t> trade_ts_ms,
           CArray<double> trade_price,
           CArray<double> trade_qty,
           CArray<std::uint8_t> is_buyer_maker,
           CArray<std::int64_t> var_ts_ms,
           CArray<double> var_ssq,
           CArray<double> var_ti,
           CArray<double> var_retsq,
           CArray<std::int64_t> ml_ts_ms,
           CArray<double> ml_dir_10s,
           CArray<double> ml_vol_10s,
           CArray<double> ml_ret_10s,
           CArray<double> ml_tox_bid,
           CArray<double> ml_tox_ask,
           CArray<double> buy_fill_static_logit_delta,
           CArray<double> buy_fill_static_missing,
           CArray<double> buy_fill_static_used,
           CArray<std::int64_t> bbo_ts_ms,
           CArray<double> bbo_best_bid,
           CArray<double> bbo_best_ask,
           CArray<double> bbo_bid_qty,
           CArray<double> bbo_ask_qty,
           CArray<std::int64_t> l2_ts_ms,
           CArray<double> l2_bid_px,
           CArray<double> l2_bid_qty,
           CArray<double> l2_ask_px,
           CArray<double> l2_ask_qty,
           CArray<double> queue_base_by_trade,
           CArray<double> queue_decay_by_trade,
           CArray<double> buy_fill_prob_by_trade,
           CArray<double> sell_fill_prob_by_trade,
           CArray<double> buy_queue_deplete_mult_by_trade,
           CArray<double> sell_queue_deplete_mult_by_trade,
           const TickReplayParams& params) {
            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            set_feature_arrays(
                input,
                var_ts_ms, var_ssq, var_ti, var_retsq,
                ml_ts_ms, ml_dir_10s, ml_vol_10s, ml_ret_10s, ml_tox_bid, ml_tox_ask
            );
            set_buy_fill_selection_static_arrays(
                input,
                buy_fill_static_logit_delta,
                buy_fill_static_missing,
                buy_fill_static_used
            );
            set_book_arrays(
                input,
                bbo_ts_ms, bbo_best_bid, bbo_best_ask, bbo_bid_qty, bbo_ask_qty,
                l2_ts_ms, l2_bid_px, l2_bid_qty, l2_ask_px, l2_ask_qty
            );
            set_per_trade_policy_arrays(
                input,
                queue_base_by_trade, queue_decay_by_trade,
                buy_fill_prob_by_trade, sell_fill_prob_by_trade,
                buy_queue_deplete_mult_by_trade, sell_queue_deplete_mult_by_trade
            );
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("trade_ts_ms"),
        py::arg("trade_price"),
        py::arg("trade_qty"),
        py::arg("is_buyer_maker"),
        py::arg("var_ts_ms"),
        py::arg("var_ssq"),
        py::arg("var_ti"),
        py::arg("var_retsq"),
        py::arg("ml_ts_ms"),
        py::arg("ml_dir_10s"),
        py::arg("ml_vol_10s"),
        py::arg("ml_ret_10s"),
        py::arg("ml_tox_bid"),
        py::arg("ml_tox_ask"),
        py::arg("buy_fill_static_logit_delta"),
        py::arg("buy_fill_static_missing"),
        py::arg("buy_fill_static_used"),
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
        py::arg("queue_base_by_trade"),
        py::arg("queue_decay_by_trade"),
        py::arg("buy_fill_prob_by_trade"),
        py::arg("sell_fill_prob_by_trade"),
        py::arg("buy_queue_deplete_mult_by_trade"),
        py::arg("sell_queue_deplete_mult_by_trade"),
        py::arg("params")
    );

    m.def(
        "simulate_tick_arrays_ext_policy_v4",
        [](CArray<std::int64_t> trade_ts_ms,
           CArray<double> trade_price,
           CArray<double> trade_qty,
           CArray<std::uint8_t> is_buyer_maker,
           CArray<std::int64_t> var_ts_ms,
           CArray<double> var_ssq,
           CArray<double> var_ti,
           CArray<double> var_retsq,
           CArray<std::int64_t> ml_ts_ms,
           CArray<double> ml_dir_10s,
           CArray<double> ml_vol_10s,
           CArray<double> ml_ret_10s,
           CArray<double> ml_tox_bid,
           CArray<double> ml_tox_ask,
           CArray<double> buy_fill_static_logit_delta,
           CArray<double> buy_fill_static_missing,
           CArray<double> buy_fill_static_used,
           CArray<std::int64_t> bbo_ts_ms,
           CArray<double> bbo_best_bid,
           CArray<double> bbo_best_ask,
           CArray<double> bbo_bid_qty,
           CArray<double> bbo_ask_qty,
           CArray<std::int64_t> l2_ts_ms,
           CArray<double> l2_bid_px,
           CArray<double> l2_bid_qty,
           CArray<double> l2_ask_px,
           CArray<double> l2_ask_qty,
           CArray<double> queue_base_by_trade,
           CArray<double> queue_decay_by_trade,
           CArray<double> buy_fill_prob_by_trade,
           CArray<double> sell_fill_prob_by_trade,
           CArray<double> buy_queue_deplete_mult_by_trade,
           CArray<double> sell_queue_deplete_mult_by_trade,
           CArray<std::int64_t> p3_ts_ms,
           CArray<double> p3_delta_star,
           CArray<double> p3_kappa_eff,
           const TickReplayParams& params) {
            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            set_feature_arrays(
                input,
                var_ts_ms, var_ssq, var_ti, var_retsq,
                ml_ts_ms, ml_dir_10s, ml_vol_10s, ml_ret_10s, ml_tox_bid, ml_tox_ask
            );
            set_conditional_p3_arrays(
                input,
                p3_ts_ms,
                p3_delta_star,
                p3_kappa_eff
            );
            set_buy_fill_selection_static_arrays(
                input,
                buy_fill_static_logit_delta,
                buy_fill_static_missing,
                buy_fill_static_used
            );
            set_book_arrays(
                input,
                bbo_ts_ms, bbo_best_bid, bbo_best_ask, bbo_bid_qty, bbo_ask_qty,
                l2_ts_ms, l2_bid_px, l2_bid_qty, l2_ask_px, l2_ask_qty
            );
            set_per_trade_policy_arrays(
                input,
                queue_base_by_trade, queue_decay_by_trade,
                buy_fill_prob_by_trade, sell_fill_prob_by_trade,
                buy_queue_deplete_mult_by_trade, sell_queue_deplete_mult_by_trade
            );
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("trade_ts_ms"),
        py::arg("trade_price"),
        py::arg("trade_qty"),
        py::arg("is_buyer_maker"),
        py::arg("var_ts_ms"),
        py::arg("var_ssq"),
        py::arg("var_ti"),
        py::arg("var_retsq"),
        py::arg("ml_ts_ms"),
        py::arg("ml_dir_10s"),
        py::arg("ml_vol_10s"),
        py::arg("ml_ret_10s"),
        py::arg("ml_tox_bid"),
        py::arg("ml_tox_ask"),
        py::arg("buy_fill_static_logit_delta"),
        py::arg("buy_fill_static_missing"),
        py::arg("buy_fill_static_used"),
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
        py::arg("queue_base_by_trade"),
        py::arg("queue_decay_by_trade"),
        py::arg("buy_fill_prob_by_trade"),
        py::arg("sell_fill_prob_by_trade"),
        py::arg("buy_queue_deplete_mult_by_trade"),
        py::arg("sell_queue_deplete_mult_by_trade"),
        py::arg("p3_ts_ms"),
        py::arg("p3_delta_star"),
        py::arg("p3_kappa_eff"),
        py::arg("params")
    );

    m.def(
        "simulate_tick_arrays_ext_policy_v5",
        [](py::tuple replay_args,
           CArray<std::int64_t> p3_reach_gate_ts_ms,
           CArray<std::uint8_t> p3_reach_gate_status,
           const TickReplayParams& params) {
            constexpr py::ssize_t expected_args = 33;
            if (py::len(replay_args) != expected_args) {
                throw std::invalid_argument(
                    "simulate_tick_arrays_ext_policy_v5 replay_args must contain exactly 33 arrays"
                );
            }

            auto trade_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[0]);
            auto trade_price = py::cast<CArray<double>>(replay_args[1]);
            auto trade_qty = py::cast<CArray<double>>(replay_args[2]);
            auto is_buyer_maker = py::cast<CArray<std::uint8_t>>(replay_args[3]);
            auto var_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[4]);
            auto var_ssq = py::cast<CArray<double>>(replay_args[5]);
            auto var_ti = py::cast<CArray<double>>(replay_args[6]);
            auto var_retsq = py::cast<CArray<double>>(replay_args[7]);
            auto ml_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[8]);
            auto ml_dir_10s = py::cast<CArray<double>>(replay_args[9]);
            auto ml_vol_10s = py::cast<CArray<double>>(replay_args[10]);
            auto ml_ret_10s = py::cast<CArray<double>>(replay_args[11]);
            auto ml_tox_bid = py::cast<CArray<double>>(replay_args[12]);
            auto ml_tox_ask = py::cast<CArray<double>>(replay_args[13]);
            auto buy_fill_static_logit_delta = py::cast<CArray<double>>(replay_args[14]);
            auto buy_fill_static_missing = py::cast<CArray<double>>(replay_args[15]);
            auto buy_fill_static_used = py::cast<CArray<double>>(replay_args[16]);
            auto bbo_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[17]);
            auto bbo_best_bid = py::cast<CArray<double>>(replay_args[18]);
            auto bbo_best_ask = py::cast<CArray<double>>(replay_args[19]);
            auto bbo_bid_qty = py::cast<CArray<double>>(replay_args[20]);
            auto bbo_ask_qty = py::cast<CArray<double>>(replay_args[21]);
            auto l2_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[22]);
            auto l2_bid_px = py::cast<CArray<double>>(replay_args[23]);
            auto l2_bid_qty = py::cast<CArray<double>>(replay_args[24]);
            auto l2_ask_px = py::cast<CArray<double>>(replay_args[25]);
            auto l2_ask_qty = py::cast<CArray<double>>(replay_args[26]);
            auto queue_base_by_trade = py::cast<CArray<double>>(replay_args[27]);
            auto queue_decay_by_trade = py::cast<CArray<double>>(replay_args[28]);
            auto buy_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[29]);
            auto sell_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[30]);
            auto buy_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[31]);
            auto sell_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[32]);

            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            set_feature_arrays(
                input,
                var_ts_ms, var_ssq, var_ti, var_retsq,
                ml_ts_ms, ml_dir_10s, ml_vol_10s, ml_ret_10s, ml_tox_bid, ml_tox_ask
            );
            set_conditional_p3_reach_gate_arrays(
                input,
                p3_reach_gate_ts_ms,
                p3_reach_gate_status
            );
            set_buy_fill_selection_static_arrays(
                input,
                buy_fill_static_logit_delta,
                buy_fill_static_missing,
                buy_fill_static_used
            );
            set_book_arrays(
                input,
                bbo_ts_ms, bbo_best_bid, bbo_best_ask, bbo_bid_qty, bbo_ask_qty,
                l2_ts_ms, l2_bid_px, l2_bid_qty, l2_ask_px, l2_ask_qty
            );
            set_per_trade_policy_arrays(
                input,
                queue_base_by_trade, queue_decay_by_trade,
                buy_fill_prob_by_trade, sell_fill_prob_by_trade,
                buy_queue_deplete_mult_by_trade, sell_queue_deplete_mult_by_trade
            );
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("replay_args"),
        py::arg("p3_reach_gate_ts_ms"),
        py::arg("p3_reach_gate_status"),
        py::arg("params")
    );

    m.def(
        "simulate_tick_arrays_ext_policy_v6",
        [](py::tuple replay_args,
           CArray<std::int64_t> p3_reach_budget_ts_ms,
           CArray<std::uint8_t> p3_reach_budget_selected_k,
           const TickReplayParams& params) {
            constexpr py::ssize_t expected_args = 33;
            if (py::len(replay_args) != expected_args) {
                throw std::invalid_argument(
                    "simulate_tick_arrays_ext_policy_v6 replay_args must contain exactly 33 arrays"
                );
            }

            auto trade_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[0]);
            auto trade_price = py::cast<CArray<double>>(replay_args[1]);
            auto trade_qty = py::cast<CArray<double>>(replay_args[2]);
            auto is_buyer_maker = py::cast<CArray<std::uint8_t>>(replay_args[3]);
            auto var_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[4]);
            auto var_ssq = py::cast<CArray<double>>(replay_args[5]);
            auto var_ti = py::cast<CArray<double>>(replay_args[6]);
            auto var_retsq = py::cast<CArray<double>>(replay_args[7]);
            auto ml_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[8]);
            auto ml_dir_10s = py::cast<CArray<double>>(replay_args[9]);
            auto ml_vol_10s = py::cast<CArray<double>>(replay_args[10]);
            auto ml_ret_10s = py::cast<CArray<double>>(replay_args[11]);
            auto ml_tox_bid = py::cast<CArray<double>>(replay_args[12]);
            auto ml_tox_ask = py::cast<CArray<double>>(replay_args[13]);
            auto buy_fill_static_logit_delta = py::cast<CArray<double>>(replay_args[14]);
            auto buy_fill_static_missing = py::cast<CArray<double>>(replay_args[15]);
            auto buy_fill_static_used = py::cast<CArray<double>>(replay_args[16]);
            auto bbo_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[17]);
            auto bbo_best_bid = py::cast<CArray<double>>(replay_args[18]);
            auto bbo_best_ask = py::cast<CArray<double>>(replay_args[19]);
            auto bbo_bid_qty = py::cast<CArray<double>>(replay_args[20]);
            auto bbo_ask_qty = py::cast<CArray<double>>(replay_args[21]);
            auto l2_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[22]);
            auto l2_bid_px = py::cast<CArray<double>>(replay_args[23]);
            auto l2_bid_qty = py::cast<CArray<double>>(replay_args[24]);
            auto l2_ask_px = py::cast<CArray<double>>(replay_args[25]);
            auto l2_ask_qty = py::cast<CArray<double>>(replay_args[26]);
            auto queue_base_by_trade = py::cast<CArray<double>>(replay_args[27]);
            auto queue_decay_by_trade = py::cast<CArray<double>>(replay_args[28]);
            auto buy_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[29]);
            auto sell_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[30]);
            auto buy_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[31]);
            auto sell_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[32]);

            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            set_feature_arrays(
                input,
                var_ts_ms, var_ssq, var_ti, var_retsq,
                ml_ts_ms, ml_dir_10s, ml_vol_10s, ml_ret_10s, ml_tox_bid, ml_tox_ask
            );
            set_conditional_p3_reach_budget_arrays(
                input,
                p3_reach_budget_ts_ms,
                p3_reach_budget_selected_k
            );
            set_buy_fill_selection_static_arrays(
                input,
                buy_fill_static_logit_delta,
                buy_fill_static_missing,
                buy_fill_static_used
            );
            set_book_arrays(
                input,
                bbo_ts_ms, bbo_best_bid, bbo_best_ask, bbo_bid_qty, bbo_ask_qty,
                l2_ts_ms, l2_bid_px, l2_bid_qty, l2_ask_px, l2_ask_qty
            );
            set_per_trade_policy_arrays(
                input,
                queue_base_by_trade, queue_decay_by_trade,
                buy_fill_prob_by_trade, sell_fill_prob_by_trade,
                buy_queue_deplete_mult_by_trade, sell_queue_deplete_mult_by_trade
            );
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("replay_args"),
        py::arg("p3_reach_budget_ts_ms"),
        py::arg("p3_reach_budget_selected_k"),
        py::arg("params")
    );

    m.def(
        "simulate_tick_arrays_ext_policy_v7",
        [](py::tuple replay_args,
           CArray<std::int64_t> fair_center_ts_ms,
           CArray<double> fair_center_price,
           CArray<double> fair_center_gain,
           CArray<std::uint8_t> fair_center_valid,
           const TickReplayParams& params) {
            constexpr py::ssize_t expected_args = 33;
            if (py::len(replay_args) != expected_args) {
                throw std::invalid_argument(
                    "simulate_tick_arrays_ext_policy_v7 replay_args must contain exactly 33 arrays"
                );
            }

            auto trade_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[0]);
            auto trade_price = py::cast<CArray<double>>(replay_args[1]);
            auto trade_qty = py::cast<CArray<double>>(replay_args[2]);
            auto is_buyer_maker = py::cast<CArray<std::uint8_t>>(replay_args[3]);
            auto var_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[4]);
            auto var_ssq = py::cast<CArray<double>>(replay_args[5]);
            auto var_ti = py::cast<CArray<double>>(replay_args[6]);
            auto var_retsq = py::cast<CArray<double>>(replay_args[7]);
            auto ml_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[8]);
            auto ml_dir_10s = py::cast<CArray<double>>(replay_args[9]);
            auto ml_vol_10s = py::cast<CArray<double>>(replay_args[10]);
            auto ml_ret_10s = py::cast<CArray<double>>(replay_args[11]);
            auto ml_tox_bid = py::cast<CArray<double>>(replay_args[12]);
            auto ml_tox_ask = py::cast<CArray<double>>(replay_args[13]);
            auto buy_fill_static_logit_delta = py::cast<CArray<double>>(replay_args[14]);
            auto buy_fill_static_missing = py::cast<CArray<double>>(replay_args[15]);
            auto buy_fill_static_used = py::cast<CArray<double>>(replay_args[16]);
            auto bbo_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[17]);
            auto bbo_best_bid = py::cast<CArray<double>>(replay_args[18]);
            auto bbo_best_ask = py::cast<CArray<double>>(replay_args[19]);
            auto bbo_bid_qty = py::cast<CArray<double>>(replay_args[20]);
            auto bbo_ask_qty = py::cast<CArray<double>>(replay_args[21]);
            auto l2_ts_ms = py::cast<CArray<std::int64_t>>(replay_args[22]);
            auto l2_bid_px = py::cast<CArray<double>>(replay_args[23]);
            auto l2_bid_qty = py::cast<CArray<double>>(replay_args[24]);
            auto l2_ask_px = py::cast<CArray<double>>(replay_args[25]);
            auto l2_ask_qty = py::cast<CArray<double>>(replay_args[26]);
            auto queue_base_by_trade = py::cast<CArray<double>>(replay_args[27]);
            auto queue_decay_by_trade = py::cast<CArray<double>>(replay_args[28]);
            auto buy_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[29]);
            auto sell_fill_prob_by_trade = py::cast<CArray<double>>(replay_args[30]);
            auto buy_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[31]);
            auto sell_queue_deplete_mult_by_trade = py::cast<CArray<double>>(replay_args[32]);

            TickReplayInput input;
            set_trade_arrays(input, trade_ts_ms, trade_price, trade_qty, is_buyer_maker);
            set_feature_arrays(
                input,
                var_ts_ms, var_ssq, var_ti, var_retsq,
                ml_ts_ms, ml_dir_10s, ml_vol_10s, ml_ret_10s, ml_tox_bid, ml_tox_ask
            );
            set_cross_venue_fair_center_arrays(
                input,
                fair_center_ts_ms,
                fair_center_price,
                fair_center_gain,
                fair_center_valid
            );
            set_buy_fill_selection_static_arrays(
                input,
                buy_fill_static_logit_delta,
                buy_fill_static_missing,
                buy_fill_static_used
            );
            set_book_arrays(
                input,
                bbo_ts_ms, bbo_best_bid, bbo_best_ask, bbo_bid_qty, bbo_ask_qty,
                l2_ts_ms, l2_bid_px, l2_bid_qty, l2_ask_px, l2_ask_qty
            );
            set_per_trade_policy_arrays(
                input,
                queue_base_by_trade, queue_decay_by_trade,
                buy_fill_prob_by_trade, sell_fill_prob_by_trade,
                buy_queue_deplete_mult_by_trade, sell_queue_deplete_mult_by_trade
            );
            py::gil_scoped_release release;
            return simulate_tick_arrays(input, params);
        },
        py::arg("replay_args"),
        py::arg("fair_center_ts_ms"),
        py::arg("fair_center_price"),
        py::arg("fair_center_gain"),
        py::arg("fair_center_valid"),
        py::arg("params")
    );
}


}  // namespace narrowgate_cpp
