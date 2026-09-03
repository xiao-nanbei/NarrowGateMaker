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

void bind_transport_contract(py::module_& m) {
    py::enum_<TransportProduct>(m, "TransportProduct")
        .value("UsdMFutures", TransportProduct::UsdMFutures);

    py::enum_<TransportBackendKind>(m, "TransportBackendKind")
        .value("Unspecified", TransportBackendKind::Unspecified)
        .value("PythonUsdmLegacy", TransportBackendKind::PythonUsdmLegacy)
        .value("CppUsdmWebSocket", TransportBackendKind::CppUsdmWebSocket)
        .value("CppUsdmRest", TransportBackendKind::CppUsdmRest)
        .value("CppUsdmFix", TransportBackendKind::CppUsdmFix);

    py::enum_<CanonicalEventKind>(m, "CanonicalEventKind")
        .value("Unspecified", CanonicalEventKind::Unspecified)
        .value("MarketTrade", CanonicalEventKind::MarketTrade)
        .value("BookTicker", CanonicalEventKind::BookTicker)
        .value("DepthDelta", CanonicalEventKind::DepthDelta)
        .value("OrderUpdate", CanonicalEventKind::OrderUpdate)
        .value("AccountUpdate", CanonicalEventKind::AccountUpdate)
        .value("SessionState", CanonicalEventKind::SessionState);

    py::enum_<CanonicalOrderType>(m, "CanonicalOrderType")
        .value("Unspecified", CanonicalOrderType::Unspecified)
        .value("Limit", CanonicalOrderType::Limit)
        .value("Market", CanonicalOrderType::Market);

    py::enum_<CanonicalSide>(m, "CanonicalSide")
        .value("Unspecified", CanonicalSide::Unspecified)
        .value("Buy", CanonicalSide::Buy)
        .value("Sell", CanonicalSide::Sell);

    py::enum_<CanonicalTimeInForce>(m, "CanonicalTimeInForce")
        .value("Unspecified", CanonicalTimeInForce::Unspecified)
        .value("Gtx", CanonicalTimeInForce::Gtx)
        .value("Ioc", CanonicalTimeInForce::Ioc);

    py::enum_<TransportPhase>(m, "TransportPhase")
        .value("Unspecified", TransportPhase::Unspecified)
        .value("LocalValidated", TransportPhase::LocalValidated)
        .value("Enqueued", TransportPhase::Enqueued)
        .value("WireDispatched", TransportPhase::WireDispatched)
        .value("ExchangeAckAccepted", TransportPhase::ExchangeAckAccepted)
        .value("ExchangeAckRejected", TransportPhase::ExchangeAckRejected)
        .value("ExchangeUpdate", TransportPhase::ExchangeUpdate)
        .value("ExchangeTerminal", TransportPhase::ExchangeTerminal);

    py::enum_<TransportUnknownState>(m, "TransportUnknownState")
        .value("None_", TransportUnknownState::None)
        .value(
            "ConfirmedNotDispatched",
            TransportUnknownState::ConfirmedNotDispatched
        )
        .value(
            "MayHaveBeenDispatched",
            TransportUnknownState::MayHaveBeenDispatched
        )
        .value(
            "AwaitingReconciliation",
            TransportUnknownState::AwaitingReconciliation
        );

    py::class_<CanonicalEventHeader>(m, "CanonicalEventHeader")
        .def(py::init<>())
        .def_readwrite("abi_version", &CanonicalEventHeader::abi_version)
        .def_readwrite("product", &CanonicalEventHeader::product)
        .def_readwrite("backend", &CanonicalEventHeader::backend)
        .def_readwrite("event_kind", &CanonicalEventHeader::event_kind)
        .def_readwrite("venue", &CanonicalEventHeader::venue)
        .def_readwrite("symbol", &CanonicalEventHeader::symbol)
        .def_readwrite("session_id", &CanonicalEventHeader::session_id)
        .def_readwrite("correlation_id", &CanonicalEventHeader::correlation_id)
        .def_readwrite("generation", &CanonicalEventHeader::generation)
        .def_readwrite(
            "exchange_event_time_ns",
            &CanonicalEventHeader::exchange_event_time_ns
        )
        .def_readwrite(
            "local_receive_time_ns",
            &CanonicalEventHeader::local_receive_time_ns
        )
        .def_readwrite(
            "feature_ready_time_ns",
            &CanonicalEventHeader::feature_ready_time_ns
        )
        .def_readwrite("source_sequence", &CanonicalEventHeader::source_sequence)
        .def_readwrite("ingress_sequence", &CanonicalEventHeader::ingress_sequence)
        .def_readwrite("snapshot", &CanonicalEventHeader::snapshot)
        .def_readwrite("reconciled", &CanonicalEventHeader::reconciled);

    py::class_<CanonicalOrderIntent>(m, "CanonicalOrderIntent")
        .def(py::init<>())
        .def_readwrite("abi_version", &CanonicalOrderIntent::abi_version)
        .def_readwrite("product", &CanonicalOrderIntent::product)
        .def_readwrite("request_id", &CanonicalOrderIntent::request_id)
        .def_readwrite("decision_id", &CanonicalOrderIntent::decision_id)
        .def_readwrite("client_order_id", &CanonicalOrderIntent::client_order_id)
        .def_readwrite("symbol", &CanonicalOrderIntent::symbol)
        .def_readwrite("side", &CanonicalOrderIntent::side)
        .def_readwrite("order_type", &CanonicalOrderIntent::order_type)
        .def_readwrite("time_in_force", &CanonicalOrderIntent::time_in_force)
        .def_readwrite("price", &CanonicalOrderIntent::price)
        .def_readwrite("quantity", &CanonicalOrderIntent::quantity)
        .def_readwrite("reduce_only", &CanonicalOrderIntent::reduce_only)
        .def_readwrite("post_only", &CanonicalOrderIntent::post_only)
        .def_readwrite("recv_window_ms", &CanonicalOrderIntent::recv_window_ms)
        .def_readwrite("deadline_time_ns", &CanonicalOrderIntent::deadline_time_ns)
        .def_readwrite(
            "expected_ownership_generation",
            &CanonicalOrderIntent::expected_ownership_generation
        )
        .def("validation_error", &CanonicalOrderIntent::validation_error)
        .def("is_structurally_valid", &CanonicalOrderIntent::is_structurally_valid);

    py::class_<CanonicalCancelIntent>(m, "CanonicalCancelIntent")
        .def(py::init<>())
        .def_readwrite("abi_version", &CanonicalCancelIntent::abi_version)
        .def_readwrite("product", &CanonicalCancelIntent::product)
        .def_readwrite("request_id", &CanonicalCancelIntent::request_id)
        .def_readwrite("decision_id", &CanonicalCancelIntent::decision_id)
        .def_readwrite("client_order_id", &CanonicalCancelIntent::client_order_id)
        .def_readwrite("exchange_order_id", &CanonicalCancelIntent::exchange_order_id)
        .def_readwrite("symbol", &CanonicalCancelIntent::symbol)
        .def_readwrite("reason", &CanonicalCancelIntent::reason)
        .def_readwrite(
            "expected_ownership_generation",
            &CanonicalCancelIntent::expected_ownership_generation
        )
        .def("validation_error", &CanonicalCancelIntent::validation_error)
        .def("is_structurally_valid", &CanonicalCancelIntent::is_structurally_valid);

    py::class_<CanonicalCancelAllIntent>(m, "CanonicalCancelAllIntent")
        .def(py::init<>())
        .def_readwrite("abi_version", &CanonicalCancelAllIntent::abi_version)
        .def_readwrite("product", &CanonicalCancelAllIntent::product)
        .def_readwrite("request_id", &CanonicalCancelAllIntent::request_id)
        .def_readwrite("decision_id", &CanonicalCancelAllIntent::decision_id)
        .def_readwrite("symbol", &CanonicalCancelAllIntent::symbol)
        .def_readwrite("reason", &CanonicalCancelAllIntent::reason)
        .def_readwrite(
            "expected_ownership_generation",
            &CanonicalCancelAllIntent::expected_ownership_generation
        )
        .def("validation_error", &CanonicalCancelAllIntent::validation_error)
        .def("is_structurally_valid", &CanonicalCancelAllIntent::is_structurally_valid);

    py::class_<TransportReceipt>(m, "TransportReceipt")
        .def(py::init<>())
        .def_readwrite("abi_version", &TransportReceipt::abi_version)
        .def_readwrite("request_id", &TransportReceipt::request_id)
        .def_readwrite("backend", &TransportReceipt::backend)
        .def_readwrite("phase", &TransportReceipt::phase)
        .def_readwrite("unknown_state", &TransportReceipt::unknown_state)
        .def_readwrite("generation", &TransportReceipt::generation)
        .def_readwrite("local_time_ns", &TransportReceipt::local_time_ns)
        .def_readwrite("exchange_time_ns", &TransportReceipt::exchange_time_ns)
        .def_readwrite("reason", &TransportReceipt::reason)
        .def(
            "allows_cross_backend_retry",
            &TransportReceipt::allows_cross_backend_retry
        );

    m.attr("TRANSPORT_CONTRACT_ABI_VERSION") = kTransportContractAbiVersion;
    m.attr("TRANSPORT_CONTRACT_SCHEMA_VERSION") =
        kTransportContractSchemaVersion;
    m.attr("DEFAULT_TRANSPORT_BACKEND") =
        py::cast(TransportBackendKind::PythonUsdmLegacy);
    m.attr("CPP_USDM_FIX_AVAILABLE") = py::bool_(false);

    m.def(
        "transport_backend_available",
        &transport_backend_available,
        py::arg("backend")
    );
    m.def(
        "transport_backend_unavailable_reason",
        [](TransportBackendKind backend) {
            return std::string(transport_backend_unavailable_reason(backend));
        },
        py::arg("backend")
    );
}

void bind_quote_core(py::module_& m) {
    py::class_<QuoteCoreConfig> config(m, "QuoteCoreConfig");
    config.def(py::init<>());

#define BIND_QUOTE_CONFIG_FIELD(field) config.def_readwrite(#field, &QuoteCoreConfig::field)
    BIND_QUOTE_CONFIG_FIELD(gamma);
    BIND_QUOTE_CONFIG_FIELD(kappa);
    BIND_QUOTE_CONFIG_FIELD(tick_size);
    BIND_QUOTE_CONFIG_FIELD(lot_size);
    BIND_QUOTE_CONFIG_FIELD(maker_fee);
    BIND_QUOTE_CONFIG_FIELD(order_size);
    BIND_QUOTE_CONFIG_FIELD(max_inventory);
    BIND_QUOTE_CONFIG_FIELD(position_timeout_s);
    BIND_QUOTE_CONFIG_FIELD(quote_horizon_s);
    BIND_QUOTE_CONFIG_FIELD(pnl_volatility_horizon_s);
    BIND_QUOTE_CONFIG_FIELD(ml_enabled);
    BIND_QUOTE_CONFIG_FIELD(vol_blend);
    BIND_QUOTE_CONFIG_FIELD(dir_threshold);
    BIND_QUOTE_CONFIG_FIELD(gamma_dir_bonus);
    BIND_QUOTE_CONFIG_FIELD(skew_strength);
    BIND_QUOTE_CONFIG_FIELD(asym_strength);
    BIND_QUOTE_CONFIG_FIELD(ret_skew);
    BIND_QUOTE_CONFIG_FIELD(ret_shift_max_pct);
    BIND_QUOTE_CONFIG_FIELD(regime_enabled);
    BIND_QUOTE_CONFIG_FIELD(vol_baseline);
    BIND_QUOTE_CONFIG_FIELD(gamma_scale_min);
    BIND_QUOTE_CONFIG_FIELD(gamma_scale_max);
    BIND_QUOTE_CONFIG_FIELD(liq_baseline);
    BIND_QUOTE_CONFIG_FIELD(gamma_liq_scale_min);
    BIND_QUOTE_CONFIG_FIELD(gamma_liq_scale_max);
    BIND_QUOTE_CONFIG_FIELD(vol_power);
    BIND_QUOTE_CONFIG_FIELD(kappa_ratio);
    BIND_QUOTE_CONFIG_FIELD(p3_delta_star);
    BIND_QUOTE_CONFIG_FIELD(p3_kappa_eff);
    BIND_QUOTE_CONFIG_FIELD(use_bar_pricing);
    BIND_QUOTE_CONFIG_FIELD(use_depth_microprice);
    BIND_QUOTE_CONFIG_FIELD(use_depth_kappa);
    BIND_QUOTE_CONFIG_FIELD(microprice_levels);
    BIND_QUOTE_CONFIG_FIELD(kappa_levels);
    BIND_QUOTE_CONFIG_FIELD(kappa_depth_baseline);
    BIND_QUOTE_CONFIG_FIELD(depth_kappa_ratio);
    BIND_QUOTE_CONFIG_FIELD(ber_spread_mult);
    BIND_QUOTE_CONFIG_FIELD(markout_spread_scale);
    BIND_QUOTE_CONFIG_FIELD(markout_side_asymmetry_sign);
    BIND_QUOTE_CONFIG_FIELD(inventory_skew_strength);
    BIND_QUOTE_CONFIG_FIELD(inventory_asym_strength);
    BIND_QUOTE_CONFIG_FIELD(inventory_signal_fade_strength);
    BIND_QUOTE_CONFIG_FIELD(book_imb_strength);
    BIND_QUOTE_CONFIG_FIELD(book_imb_levels);
    BIND_QUOTE_CONFIG_FIELD(trace_book_imb_levels);
    BIND_QUOTE_CONFIG_FIELD(depth_tox_enabled);
    BIND_QUOTE_CONFIG_FIELD(depth_tox_levels);
    BIND_QUOTE_CONFIG_FIELD(depth_tox_imbalance_threshold);
    BIND_QUOTE_CONFIG_FIELD(depth_tox_microprice_shift_bps);
    BIND_QUOTE_CONFIG_FIELD(depth_tox_spread_mult);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_enabled);
    BIND_QUOTE_CONFIG_FIELD(max_spread_bps);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_base_bps);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_alpha);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_max_mult);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_var_baseline);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_liq_beta);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_liq_baseline);
    BIND_QUOTE_CONFIG_FIELD(dynamic_cap_min_mult);
    BIND_QUOTE_CONFIG_FIELD(spread_cap_mode);
    BIND_QUOTE_CONFIG_FIELD(exit_urgency_strength);
    BIND_QUOTE_CONFIG_FIELD(urgency_time_weight);
    BIND_QUOTE_CONFIG_FIELD(urgency_pnl_weight);
    BIND_QUOTE_CONFIG_FIELD(urgency_signal_weight);
    BIND_QUOTE_CONFIG_FIELD(adverse_guard_enabled);
    BIND_QUOTE_CONFIG_FIELD(adverse_toxicity_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_markout_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_markout_pause_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_markout_pause_hybrid);
    BIND_QUOTE_CONFIG_FIELD(adverse_dir_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_ret_bps_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_microprice_shift_bps);
    BIND_QUOTE_CONFIG_FIELD(adverse_spread_mult);
    BIND_QUOTE_CONFIG_FIELD(adverse_thin_depth_threshold);
    BIND_QUOTE_CONFIG_FIELD(adverse_thin_depth_mult);
    BIND_QUOTE_CONFIG_FIELD(adverse_pause);
    BIND_QUOTE_CONFIG_FIELD(defense_guard_enabled);
    BIND_QUOTE_CONFIG_FIELD(defense_markout_threshold);
    BIND_QUOTE_CONFIG_FIELD(defense_dir_threshold);
    BIND_QUOTE_CONFIG_FIELD(defense_ret_bps_threshold);
    BIND_QUOTE_CONFIG_FIELD(defense_microprice_shift_bps);
    BIND_QUOTE_CONFIG_FIELD(defense_spread_mult);
    BIND_QUOTE_CONFIG_FIELD(defense_pause);
    BIND_QUOTE_CONFIG_FIELD(defense_emergency_inventory_ratio);
    BIND_QUOTE_CONFIG_FIELD(defense_emergency_loss);
    BIND_QUOTE_CONFIG_FIELD(inventory_reference_qty);
    BIND_QUOTE_CONFIG_FIELD(eta_inventory);
    BIND_QUOTE_CONFIG_FIELD(a_spread);
    BIND_QUOTE_CONFIG_FIELD(f03_ret_action_horizon_s);
    BIND_QUOTE_CONFIG_FIELD(f03_ret_action_compatible);
    BIND_QUOTE_CONFIG_FIELD(risk_per_order);
    BIND_QUOTE_CONFIG_FIELD(execution_intensity_slope);
    BIND_QUOTE_CONFIG_FIELD(risk_horizon_s);
    BIND_QUOTE_CONFIG_FIELD(historical_p3_scalar_adapter_enabled);
    BIND_QUOTE_CONFIG_FIELD(p3_side_bbo_floor_enabled);
    BIND_QUOTE_CONFIG_FIELD(p3_identity_required);
    BIND_QUOTE_CONFIG_FIELD(p3_event_type);
    BIND_QUOTE_CONFIG_FIELD(p3_horizon_s);
    BIND_QUOTE_CONFIG_FIELD(p3_distance_origin);
    BIND_QUOTE_CONFIG_FIELD(p3_distance_unit);
    BIND_QUOTE_CONFIG_FIELD(p3_side);
    BIND_QUOTE_CONFIG_FIELD(p3_queue_included);
    BIND_QUOTE_CONFIG_FIELD(p3_artifact_sha256);
    BIND_QUOTE_CONFIG_FIELD(trade_intensity_acceleration_spread_mult);
#undef BIND_QUOTE_CONFIG_FIELD

    py::class_<SideQuoteContext> side_context(m, "SideQuoteContext");
    side_context.def(py::init<>());

#define BIND_SIDE_CONTEXT_FIELD(field) side_context.def_readwrite(#field, &SideQuoteContext::field)
    BIND_SIDE_CONTEXT_FIELD(raw_price);
    BIND_SIDE_CONTEXT_FIELD(pre_guard_price);
    BIND_SIDE_CONTEXT_FIELD(final_price);
    BIND_SIDE_CONTEXT_FIELD(raw_quote_delta_to_bbo);
    BIND_SIDE_CONTEXT_FIELD(pre_guard_delta_to_bbo);
    BIND_SIDE_CONTEXT_FIELD(final_quote_delta_to_bbo);
    BIND_SIDE_CONTEXT_FIELD(raw_distance_to_mid);
    BIND_SIDE_CONTEXT_FIELD(final_distance_to_mid);
    BIND_SIDE_CONTEXT_FIELD(final_pair_spread);
    BIND_SIDE_CONTEXT_FIELD(final_quote_skew);
    BIND_SIDE_CONTEXT_FIELD(spread_mult);
    BIND_SIDE_CONTEXT_FIELD(side_adverse);
    BIND_SIDE_CONTEXT_FIELD(side_adverse_pause);
    BIND_SIDE_CONTEXT_FIELD(adverse_toxicity);
    BIND_SIDE_CONTEXT_FIELD(adverse_markout);
    BIND_SIDE_CONTEXT_FIELD(adverse_direction);
    BIND_SIDE_CONTEXT_FIELD(adverse_ret);
    BIND_SIDE_CONTEXT_FIELD(adverse_microprice);
    BIND_SIDE_CONTEXT_FIELD(adverse_thin_depth);
    BIND_SIDE_CONTEXT_FIELD(defense_guard);
    BIND_SIDE_CONTEXT_FIELD(defense_pause);
    BIND_SIDE_CONTEXT_FIELD(defense_reducing);
    BIND_SIDE_CONTEXT_FIELD(defense_emergency);
    BIND_SIDE_CONTEXT_FIELD(defense_markout);
    BIND_SIDE_CONTEXT_FIELD(defense_direction);
    BIND_SIDE_CONTEXT_FIELD(defense_ret);
    BIND_SIDE_CONTEXT_FIELD(defense_microprice);
    BIND_SIDE_CONTEXT_FIELD(defense_spread_mult);
    BIND_SIDE_CONTEXT_FIELD(mid_guard);
    BIND_SIDE_CONTEXT_FIELD(post_only);
    BIND_SIDE_CONTEXT_FIELD(cap_exposure_block);
    BIND_SIDE_CONTEXT_FIELD(near_depth_total);
    BIND_SIDE_CONTEXT_FIELD(order_ttl_ms);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_guard);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_pause);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_thin_depth);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_rank);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_low);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_high);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_window_s);
    BIND_SIDE_CONTEXT_FIELD(local_extreme_spread_mult);
    BIND_SIDE_CONTEXT_FIELD(l2_quote_flip_rate);
    BIND_SIDE_CONTEXT_FIELD(l2_book_refresh_ratio);
    BIND_SIDE_CONTEXT_FIELD(l2_book_cancel_ratio);
    BIND_SIDE_CONTEXT_FIELD(l2_near_depth_total);
    BIND_SIDE_CONTEXT_FIELD(buy_fill_selection_live_score);
    BIND_SIDE_CONTEXT_FIELD(buy_fill_selection_live_hit);
    BIND_SIDE_CONTEXT_FIELD(buy_fill_selection_live_missing_features);
    BIND_SIDE_CONTEXT_FIELD(final_guard_changed);
    BIND_SIDE_CONTEXT_FIELD(any_constraint_changed);
#undef BIND_SIDE_CONTEXT_FIELD

    py::class_<QuoteFlags> quote_flags(m, "QuoteFlags");
    quote_flags.def(py::init<>());

#define BIND_QUOTE_FLAGS_FIELD(field) quote_flags.def_readwrite(#field, &QuoteFlags::field)
    BIND_QUOTE_FLAGS_FIELD(cap_hit);
    BIND_QUOTE_FLAGS_FIELD(delta_cap);
    BIND_QUOTE_FLAGS_FIELD(final_compressed);
    BIND_QUOTE_FLAGS_FIELD(mid_guard);
    BIND_QUOTE_FLAGS_FIELD(post_only);
    BIND_QUOTE_FLAGS_FIELD(bid_adverse);
    BIND_QUOTE_FLAGS_FIELD(ask_adverse);
    BIND_QUOTE_FLAGS_FIELD(defense_guard);
    BIND_QUOTE_FLAGS_FIELD(cap_exposure_block);
#undef BIND_QUOTE_FLAGS_FIELD

    py::class_<QuoteCoreResult> quote_result(m, "QuoteCoreResult");
    quote_result.def(py::init<>());

#define BIND_QUOTE_RESULT_FIELD(field) quote_result.def_readwrite(#field, &QuoteCoreResult::field)
    BIND_QUOTE_RESULT_FIELD(bid_price);
    BIND_QUOTE_RESULT_FIELD(ask_price);
    BIND_QUOTE_RESULT_FIELD(spread);
    BIND_QUOTE_RESULT_FIELD(raw_half_spread);
    BIND_QUOTE_RESULT_FIELD(capped_half_spread);
    BIND_QUOTE_RESULT_FIELD(raw_mid_shift);
    BIND_QUOTE_RESULT_FIELD(fair);
    BIND_QUOTE_RESULT_FIELD(cap_bps);
    BIND_QUOTE_RESULT_FIELD(max_spread);
    BIND_QUOTE_RESULT_FIELD(reservation_price);
    BIND_QUOTE_RESULT_FIELD(sigma_sq_raw);
    BIND_QUOTE_RESULT_FIELD(sigma_sq_blended);
    BIND_QUOTE_RESULT_FIELD(delta_raw);
    BIND_QUOTE_RESULT_FIELD(delta_after_regime);
    BIND_QUOTE_RESULT_FIELD(delta_pre_cap);
    BIND_QUOTE_RESULT_FIELD(delta_after_cap);
    BIND_QUOTE_RESULT_FIELD(final_cap_excess);
    BIND_QUOTE_RESULT_FIELD(half_d);
    BIND_QUOTE_RESULT_FIELD(asym);
    BIND_QUOTE_RESULT_FIELD(raw_reservation_shift);
    BIND_QUOTE_RESULT_FIELD(raw_asym_shift);
    BIND_QUOTE_RESULT_FIELD(raw_quote_skew);
    BIND_QUOTE_RESULT_FIELD(book_imb);
    BIND_QUOTE_RESULT_FIELD(microprice_shift_bps);
    BIND_QUOTE_RESULT_FIELD(near_depth_total);
    BIND_QUOTE_RESULT_FIELD(kappa_before_depth);
    BIND_QUOTE_RESULT_FIELD(kappa_used);
    BIND_QUOTE_RESULT_FIELD(depth_tox_mult);
    BIND_QUOTE_RESULT_FIELD(final_cap_rounding);
    BIND_QUOTE_RESULT_FIELD(final_cap_mid_guard);
    BIND_QUOTE_RESULT_FIELD(final_cap_post_only);
    BIND_QUOTE_RESULT_FIELD(final_cap_delta);
    BIND_QUOTE_RESULT_FIELD(mid_guard_bid);
    BIND_QUOTE_RESULT_FIELD(mid_guard_ask);
    BIND_QUOTE_RESULT_FIELD(post_only_bid);
    BIND_QUOTE_RESULT_FIELD(post_only_ask);
    BIND_QUOTE_RESULT_FIELD(buy);
    BIND_QUOTE_RESULT_FIELD(sell);
    BIND_QUOTE_RESULT_FIELD(flags);
#undef BIND_QUOTE_RESULT_FIELD

    m.def(
        "compute_live_routing_decision",
        [](py::sequence input_values,
           py::sequence bid_policy_values,
           py::sequence ask_policy_values) {
            if (py::len(input_values) != 22 ||
                py::len(bid_policy_values) != 5 ||
                py::len(ask_policy_values) != 5) {
                throw std::invalid_argument("live routing compact input length mismatch");
            }

            LiveRoutingInput input;
            input.mid = py::cast<double>(input_values[0]);
            input.inventory = py::cast<double>(input_values[1]);
            input.base_bid_price = py::cast<double>(input_values[2]);
            input.base_ask_price = py::cast<double>(input_values[3]);
            input.best_bid = py::cast<double>(input_values[4]);
            input.best_ask = py::cast<double>(input_values[5]);
            input.tick_size = py::cast<double>(input_values[6]);
            input.lot_size = py::cast<double>(input_values[7]);
            input.min_qty = py::cast<double>(input_values[8]);
            input.min_notional = py::cast<double>(input_values[9]);
            input.order_size = py::cast<double>(input_values[10]);
            input.max_inventory = py::cast<double>(input_values[11]);
            input.eta = py::cast<double>(input_values[12]);
            input.symmetric_size = py::cast<bool>(input_values[13]);
            input.requote_threshold_bps = py::cast<double>(input_values[14]);
            input.max_spread = py::cast<double>(input_values[15]);
            input.bid_active = py::cast<bool>(input_values[16]);
            input.bid_active_price = py::cast<double>(input_values[17]);
            input.bid_age_ms = py::cast<double>(input_values[18]);
            input.ask_active = py::cast<bool>(input_values[19]);
            input.ask_active_price = py::cast<double>(input_values[20]);
            input.ask_age_ms = py::cast<double>(input_values[21]);

            const auto read_policy = [](const py::sequence& values) {
                LiveRoutingPolicy policy;
                policy.allow_post = py::cast<bool>(values[0]);
                policy.allow_exposure_increase = py::cast<bool>(values[1]);
                policy.spread_mult = py::cast<double>(values[2]);
                policy.size_mult = py::cast<double>(values[3]);
                policy.order_ttl_ms = py::cast<double>(values[4]);
                return policy;
            };
            const auto result = compute_live_routing_decision(
                input,
                read_policy(bid_policy_values),
                read_policy(ask_policy_values)
            );
            return py::make_tuple(
                result.bid_price,
                result.ask_price,
                result.post_policy_cap_hit,
                result.can_bid_after_inventory,
                result.can_ask_after_inventory,
                result.can_bid,
                result.can_ask,
                result.bid_needs_update,
                result.ask_needs_update,
                result.bid_size,
                result.ask_size
            );
        },
        py::arg("input_values"),
        py::arg("bid_policy_values"),
        py::arg("ask_policy_values")
    );
    m.def(
        "compute_quote_core",
        [](const QuoteState& state, const QuoteCoreConfig& cfg,
           const QuotePrediction& pred, const DepthSnapshot& depth) {
            return compute_quote_core(state, cfg, pred, depth);
        },
        py::arg("state"),
        py::arg("cfg"),
        py::arg("pred"),
        py::arg("depth") = DepthSnapshot{}
    );

    m.def(
        "compute_quote_core_live",
        [](py::sequence state_values,
           const QuoteCoreConfig& cfg,
           py::sequence pred_values,
           py::handle bids,
           py::handle asks) {
            // live scalar 调用用固定长度 tuple/sequence 避免 asdict/dict 字符串查找；
            // 但仍有 pybind 边界成本，不能和离线 batch benchmark 混为一谈。
            if (py::len(state_values) != 16 || py::len(pred_values) != 5) {
                throw std::invalid_argument("live quote state/pred length mismatch");
            }
            QuoteState state;
            state.mid = py::cast<double>(state_values[0]);
            state.inventory = py::cast<double>(state_values[1]);
            state.sigma_sq = py::cast<double>(state_values[2]);
            state.trade_intensity = py::cast<double>(state_values[3]);
            state.best_bid = py::cast<double>(state_values[4]);
            state.best_ask = py::cast<double>(state_values[5]);
            state.ber_active = py::cast<bool>(state_values[6]);
            state.mo_ema_all = py::cast<double>(state_values[7]);
            state.mo_ema_bid = py::cast<double>(state_values[8]);
            state.mo_ema_ask = py::cast<double>(state_values[9]);
            state.bid_adverse_markout_pause_latch = py::cast<bool>(state_values[10]);
            state.ask_adverse_markout_pause_latch = py::cast<bool>(state_values[11]);
            state.mo_ref = py::cast<double>(state_values[12]);
            state.position_open = py::cast<bool>(state_values[13]);
            state.hold_time_s = py::cast<double>(state_values[14]);
            state.unrealized_pnl = py::cast<double>(state_values[15]);

            QuotePrediction pred;
            pred.dir_10s = py::cast<double>(pred_values[0]);
            pred.vol_10s = py::cast<double>(pred_values[1]);
            pred.ret_10s = py::cast<double>(pred_values[2]);
            pred.tox_bid = py::cast<double>(pred_values[3]);
            pred.tox_ask = py::cast<double>(pred_values[4]);
            return compute_quote_core(state, cfg, pred, depth_from_python_levels(bids, asks));
        },
        py::arg("state_values"),
        py::arg("cfg"),
        py::arg("pred_values"),
        py::arg("bids") = py::tuple(),
        py::arg("asks") = py::tuple()
    );

    m.def(
        "compute_quote_core_batch",
        [](CArray<double> mid,
           CArray<double> inventory,
           CArray<double> sigma_sq,
           CArray<double> trade_intensity,
           CArray<double> best_bid,
           CArray<double> best_ask,
           CArray<double> dir_10s,
           CArray<double> vol_10s,
           CArray<double> ret_10s,
           CArray<double> tox_bid,
           CArray<double> tox_ask,
           const QuoteCoreConfig& cfg) {
            const auto n = mid.size();
            auto require = [n](const auto& array, const char* name) {
                if (array.size() != n) {
                    throw std::invalid_argument(std::string(name) + " length mismatch");
                }
            };
            require(inventory, "inventory");
            require(sigma_sq, "sigma_sq");
            require(trade_intensity, "trade_intensity");
            require(best_bid, "best_bid");
            require(best_ask, "best_ask");
            require(dir_10s, "dir_10s");
            require(vol_10s, "vol_10s");
            require(ret_10s, "ret_10s");
            require(tox_bid, "tox_bid");
            require(tox_ask, "tox_ask");

            py::array_t<double> bid_price(n);
            py::array_t<double> ask_price(n);
            py::array_t<double> spread(n);
            py::array_t<double> raw_half_spread(n);
            py::array_t<double> raw_mid_shift(n);
            py::array_t<double> fair(n);
            py::array_t<std::uint8_t> cap_hit(n);
            py::array_t<std::uint8_t> final_compressed(n);

            auto* bid_price_ptr = bid_price.mutable_data();
            auto* ask_price_ptr = ask_price.mutable_data();
            auto* spread_ptr = spread.mutable_data();
            auto* raw_half_spread_ptr = raw_half_spread.mutable_data();
            auto* raw_mid_shift_ptr = raw_mid_shift.mutable_data();
            auto* fair_ptr = fair.mutable_data();
            auto* cap_hit_ptr = cap_hit.mutable_data();
            auto* final_compressed_ptr = final_compressed.mutable_data();

            {
                py::gil_scoped_release release;
                for (py::ssize_t i = 0; i < n; ++i) {
                    QuoteState state;
                    state.mid = mid.data()[i];
                    state.inventory = inventory.data()[i];
                    state.sigma_sq = sigma_sq.data()[i];
                    state.trade_intensity = trade_intensity.data()[i];
                    state.best_bid = best_bid.data()[i];
                    state.best_ask = best_ask.data()[i];

                    QuotePrediction pred;
                    pred.dir_10s = dir_10s.data()[i];
                    pred.vol_10s = vol_10s.data()[i];
                    pred.ret_10s = ret_10s.data()[i];
                    pred.tox_bid = tox_bid.data()[i];
                    pred.tox_ask = tox_ask.data()[i];

                    const auto result = compute_quote_core(state, cfg, pred, DepthSnapshot{});
                    bid_price_ptr[i] = result.bid_price;
                    ask_price_ptr[i] = result.ask_price;
                    spread_ptr[i] = result.spread;
                    raw_half_spread_ptr[i] = result.raw_half_spread;
                    raw_mid_shift_ptr[i] = result.raw_mid_shift;
                    fair_ptr[i] = result.fair;
                    cap_hit_ptr[i] = result.flags.cap_hit ? 1 : 0;
                    final_compressed_ptr[i] = result.flags.final_compressed ? 1 : 0;
                }
            }

            py::dict out;
            out["bid_price"] = bid_price;
            out["ask_price"] = ask_price;
            out["spread"] = spread;
            out["raw_half_spread"] = raw_half_spread;
            out["raw_mid_shift"] = raw_mid_shift;
            out["fair"] = fair;
            out["cap_hit"] = cap_hit;
            out["final_compressed"] = final_compressed;
            return out;
        },
        py::arg("mid"),
        py::arg("inventory"),
        py::arg("sigma_sq"),
        py::arg("trade_intensity"),
        py::arg("best_bid"),
        py::arg("best_ask"),
        py::arg("dir_10s"),
        py::arg("vol_10s"),
        py::arg("ret_10s"),
        py::arg("tox_bid"),
        py::arg("tox_ask"),
        py::arg("cfg")
    );

    m.def(
        "compute_quote_core_batch_depth",
        [](CArray<double> mid,
           CArray<double> inventory,
           CArray<double> sigma_sq,
           CArray<double> trade_intensity,
           CArray<double> best_bid,
           CArray<double> best_ask,
           CArray<double> dir_10s,
           CArray<double> vol_10s,
           CArray<double> ret_10s,
           CArray<double> tox_bid,
           CArray<double> tox_ask,
           CArray<double> mo_ema_bid,
           CArray<double> mo_ema_ask,
           CArray<double> mo_ema_all,
           CArray<double> mo_ref,
           CArray<double> ber_active,
           CArray<double> position_open,
           CArray<double> hold_time_s,
           CArray<double> unrealized_pnl,
           CArray<double> l2_bid_px,
           CArray<double> l2_bid_qty,
           CArray<double> l2_ask_px,
           CArray<double> l2_ask_qty,
           const QuoteCoreConfig& cfg,
           int workers) {
            const auto n = mid.size();
            auto require = [n](const auto& array, const char* name) {
                if (array.size() != n) {
                    throw std::invalid_argument(std::string(name) + " length mismatch");
                }
            };
            require(inventory, "inventory");
            require(sigma_sq, "sigma_sq");
            require(trade_intensity, "trade_intensity");
            require(best_bid, "best_bid");
            require(best_ask, "best_ask");
            require(dir_10s, "dir_10s");
            require(vol_10s, "vol_10s");
            require(ret_10s, "ret_10s");
            require(tox_bid, "tox_bid");
            require(tox_ask, "tox_ask");
            require(mo_ema_bid, "mo_ema_bid");
            require(mo_ema_ask, "mo_ema_ask");
            require(mo_ema_all, "mo_ema_all");
            require(mo_ref, "mo_ref");
            require(ber_active, "ber_active");
            require(position_open, "position_open");
            require(hold_time_s, "hold_time_s");
            require(unrealized_pnl, "unrealized_pnl");

            const auto bid_px = matrix_from_array(l2_bid_px, "l2_bid_px");
            const auto bid_qty = matrix_from_array(l2_bid_qty, "l2_bid_qty");
            const auto ask_px = matrix_from_array(l2_ask_px, "l2_ask_px");
            const auto ask_qty = matrix_from_array(l2_ask_qty, "l2_ask_qty");
            auto require_rows = [n](std::size_t rows, const char* name) {
                if (rows != static_cast<std::size_t>(n)) {
                    throw std::invalid_argument(std::string(name) + " row mismatch");
                }
            };
            require_rows(bid_px.rows, "l2_bid_px");
            require_rows(bid_qty.rows, "l2_bid_qty");
            require_rows(ask_px.rows, "l2_ask_px");
            require_rows(ask_qty.rows, "l2_ask_qty");
            if (bid_px.cols != bid_qty.cols || ask_px.cols != ask_qty.cols) {
                throw std::invalid_argument("L2 price/qty column mismatch");
            }

            py::array_t<double> bid_price(n), ask_price(n), spread(n);
            py::array_t<double> raw_half_spread(n), capped_half_spread(n), raw_mid_shift(n);
            py::array_t<double> raw_reservation_shift(n), raw_asym_shift(n), asym(n);
            py::array_t<double> fair(n), raw_quote_skew(n), near_depth_total(n);
            py::array_t<double> book_imb(n), microprice_shift_bps(n), kappa_before_depth(n);
            py::array_t<double> kappa_used(n), depth_tox_mult(n), cap_bps(n), max_spread(n);
            py::array_t<double> bid_raw_price(n), ask_raw_price(n), bid_pre_guard_price(n), ask_pre_guard_price(n);
            py::array_t<double> bid_final_price(n), ask_final_price(n);
            py::array_t<double> bid_final_quote_delta_to_bbo(n), ask_final_quote_delta_to_bbo(n);
            py::array_t<double> bid_final_distance_to_mid(n), ask_final_distance_to_mid(n);
            py::array_t<double> bid_final_quote_skew(n), ask_final_quote_skew(n);
            py::array_t<double> bid_spread_mult(n), ask_spread_mult(n);
            py::array_t<double> bid_defense_spread_mult(n), ask_defense_spread_mult(n);
            py::array_t<std::uint8_t> cap_hit(n), delta_cap(n), final_compressed(n);
            py::array_t<std::uint8_t> mid_guard(n), post_only(n);
            py::array_t<std::uint8_t> bid_side_adverse(n), ask_side_adverse(n);
            py::array_t<std::uint8_t> bid_side_adverse_pause(n), ask_side_adverse_pause(n);
            py::array_t<std::uint8_t> bid_adverse_toxicity(n), ask_adverse_toxicity(n);
            py::array_t<std::uint8_t> bid_adverse_markout(n), ask_adverse_markout(n);
            py::array_t<std::uint8_t> bid_adverse_direction(n), ask_adverse_direction(n);
            py::array_t<std::uint8_t> bid_adverse_ret(n), ask_adverse_ret(n);
            py::array_t<std::uint8_t> bid_adverse_microprice(n), ask_adverse_microprice(n);
            py::array_t<std::uint8_t> bid_adverse_thin_depth(n), ask_adverse_thin_depth(n);
            py::array_t<std::uint8_t> bid_defense_guard(n), ask_defense_guard(n);
            py::array_t<std::uint8_t> bid_defense_pause(n), ask_defense_pause(n);
            py::array_t<std::uint8_t> bid_defense_reducing(n), ask_defense_reducing(n);
            py::array_t<std::uint8_t> bid_defense_emergency(n), ask_defense_emergency(n);
            py::array_t<std::uint8_t> bid_mid_guard(n), ask_mid_guard(n);
            py::array_t<std::uint8_t> bid_post_only(n), ask_post_only(n);

            auto* bid_price_ptr = bid_price.mutable_data();
            auto* ask_price_ptr = ask_price.mutable_data();
            auto* spread_ptr = spread.mutable_data();
            auto* raw_half_spread_ptr = raw_half_spread.mutable_data();
            auto* capped_half_spread_ptr = capped_half_spread.mutable_data();
            auto* raw_mid_shift_ptr = raw_mid_shift.mutable_data();
            auto* raw_reservation_shift_ptr = raw_reservation_shift.mutable_data();
            auto* raw_asym_shift_ptr = raw_asym_shift.mutable_data();
            auto* asym_ptr = asym.mutable_data();
            auto* fair_ptr = fair.mutable_data();
            auto* raw_quote_skew_ptr = raw_quote_skew.mutable_data();
            auto* near_depth_total_ptr = near_depth_total.mutable_data();
            auto* book_imb_ptr = book_imb.mutable_data();
            auto* microprice_shift_bps_ptr = microprice_shift_bps.mutable_data();
            auto* kappa_before_depth_ptr = kappa_before_depth.mutable_data();
            auto* kappa_used_ptr = kappa_used.mutable_data();
            auto* depth_tox_mult_ptr = depth_tox_mult.mutable_data();
            auto* cap_bps_ptr = cap_bps.mutable_data();
            auto* max_spread_ptr = max_spread.mutable_data();
            auto* bid_raw_price_ptr = bid_raw_price.mutable_data();
            auto* ask_raw_price_ptr = ask_raw_price.mutable_data();
            auto* bid_pre_guard_price_ptr = bid_pre_guard_price.mutable_data();
            auto* ask_pre_guard_price_ptr = ask_pre_guard_price.mutable_data();
            auto* bid_final_price_ptr = bid_final_price.mutable_data();
            auto* ask_final_price_ptr = ask_final_price.mutable_data();
            auto* bid_final_quote_delta_to_bbo_ptr = bid_final_quote_delta_to_bbo.mutable_data();
            auto* ask_final_quote_delta_to_bbo_ptr = ask_final_quote_delta_to_bbo.mutable_data();
            auto* bid_final_distance_to_mid_ptr = bid_final_distance_to_mid.mutable_data();
            auto* ask_final_distance_to_mid_ptr = ask_final_distance_to_mid.mutable_data();
            auto* bid_final_quote_skew_ptr = bid_final_quote_skew.mutable_data();
            auto* ask_final_quote_skew_ptr = ask_final_quote_skew.mutable_data();
            auto* bid_spread_mult_ptr = bid_spread_mult.mutable_data();
            auto* ask_spread_mult_ptr = ask_spread_mult.mutable_data();
            auto* bid_defense_spread_mult_ptr = bid_defense_spread_mult.mutable_data();
            auto* ask_defense_spread_mult_ptr = ask_defense_spread_mult.mutable_data();

            auto* cap_hit_ptr = cap_hit.mutable_data();
            auto* delta_cap_ptr = delta_cap.mutable_data();
            auto* final_compressed_ptr = final_compressed.mutable_data();
            auto* mid_guard_ptr = mid_guard.mutable_data();
            auto* post_only_ptr = post_only.mutable_data();
            auto* bid_side_adverse_ptr = bid_side_adverse.mutable_data();
            auto* ask_side_adverse_ptr = ask_side_adverse.mutable_data();
            auto* bid_side_adverse_pause_ptr = bid_side_adverse_pause.mutable_data();
            auto* ask_side_adverse_pause_ptr = ask_side_adverse_pause.mutable_data();
            auto* bid_adverse_toxicity_ptr = bid_adverse_toxicity.mutable_data();
            auto* ask_adverse_toxicity_ptr = ask_adverse_toxicity.mutable_data();
            auto* bid_adverse_markout_ptr = bid_adverse_markout.mutable_data();
            auto* ask_adverse_markout_ptr = ask_adverse_markout.mutable_data();
            auto* bid_adverse_direction_ptr = bid_adverse_direction.mutable_data();
            auto* ask_adverse_direction_ptr = ask_adverse_direction.mutable_data();
            auto* bid_adverse_ret_ptr = bid_adverse_ret.mutable_data();
            auto* ask_adverse_ret_ptr = ask_adverse_ret.mutable_data();
            auto* bid_adverse_microprice_ptr = bid_adverse_microprice.mutable_data();
            auto* ask_adverse_microprice_ptr = ask_adverse_microprice.mutable_data();
            auto* bid_adverse_thin_depth_ptr = bid_adverse_thin_depth.mutable_data();
            auto* ask_adverse_thin_depth_ptr = ask_adverse_thin_depth.mutable_data();
            auto* bid_defense_guard_ptr = bid_defense_guard.mutable_data();
            auto* ask_defense_guard_ptr = ask_defense_guard.mutable_data();
            auto* bid_defense_pause_ptr = bid_defense_pause.mutable_data();
            auto* ask_defense_pause_ptr = ask_defense_pause.mutable_data();
            auto* bid_defense_reducing_ptr = bid_defense_reducing.mutable_data();
            auto* ask_defense_reducing_ptr = ask_defense_reducing.mutable_data();
            auto* bid_defense_emergency_ptr = bid_defense_emergency.mutable_data();
            auto* ask_defense_emergency_ptr = ask_defense_emergency.mutable_data();
            auto* bid_mid_guard_ptr = bid_mid_guard.mutable_data();
            auto* ask_mid_guard_ptr = ask_mid_guard.mutable_data();
            auto* bid_post_only_ptr = bid_post_only.mutable_data();
            auto* ask_post_only_ptr = ask_post_only.mutable_data();

            const auto run_rows = [&](py::ssize_t begin, py::ssize_t end) {
                for (py::ssize_t i = begin; i < end; ++i) {
                    QuoteState state;
                    state.mid = mid.data()[i];
                    state.inventory = inventory.data()[i];
                    state.sigma_sq = sigma_sq.data()[i];
                    state.trade_intensity = trade_intensity.data()[i];
                    state.best_bid = best_bid.data()[i];
                    state.best_ask = best_ask.data()[i];
                    state.mo_ema_bid = mo_ema_bid.data()[i];
                    state.mo_ema_ask = mo_ema_ask.data()[i];
                    state.mo_ema_all = mo_ema_all.data()[i];
                    state.mo_ref = mo_ref.data()[i];
                    state.ber_active = ber_active.data()[i] != 0.0;
                    state.position_open = position_open.data()[i] != 0.0;
                    state.hold_time_s = hold_time_s.data()[i];
                    state.unrealized_pnl = unrealized_pnl.data()[i];

                    QuotePrediction pred;
                    pred.dir_10s = dir_10s.data()[i];
                    pred.vol_10s = vol_10s.data()[i];
                    pred.ret_10s = ret_10s.data()[i];
                    pred.tox_bid = tox_bid.data()[i];
                    pred.tox_ask = tox_ask.data()[i];

                    const DepthView depth{
                        DepthSideView{{}, bid_px.row(static_cast<std::size_t>(i)),
                                      bid_qty.row(static_cast<std::size_t>(i))},
                        DepthSideView{{}, ask_px.row(static_cast<std::size_t>(i)),
                                      ask_qty.row(static_cast<std::size_t>(i))},
                    };

                    const auto result = compute_quote_core(state, cfg, pred, depth);
                    bid_price_ptr[i] = result.bid_price;
                    ask_price_ptr[i] = result.ask_price;
                    spread_ptr[i] = result.spread;
                    raw_half_spread_ptr[i] = result.raw_half_spread;
                    capped_half_spread_ptr[i] = result.capped_half_spread;
                    raw_mid_shift_ptr[i] = result.raw_mid_shift;
                    raw_reservation_shift_ptr[i] = result.raw_reservation_shift;
                    raw_asym_shift_ptr[i] = result.raw_asym_shift;
                    asym_ptr[i] = result.asym;
                    fair_ptr[i] = result.fair;
                    raw_quote_skew_ptr[i] = result.raw_quote_skew;
                    near_depth_total_ptr[i] = result.near_depth_total;
                    book_imb_ptr[i] = result.book_imb;
                    microprice_shift_bps_ptr[i] = result.microprice_shift_bps;
                    kappa_before_depth_ptr[i] = result.kappa_before_depth;
                    kappa_used_ptr[i] = result.kappa_used;
                    depth_tox_mult_ptr[i] = result.depth_tox_mult;
                    cap_bps_ptr[i] = result.cap_bps;
                    max_spread_ptr[i] = result.max_spread;

                    bid_raw_price_ptr[i] = result.buy.raw_price;
                    ask_raw_price_ptr[i] = result.sell.raw_price;
                    bid_pre_guard_price_ptr[i] = result.buy.pre_guard_price;
                    ask_pre_guard_price_ptr[i] = result.sell.pre_guard_price;
                    bid_final_price_ptr[i] = result.buy.final_price;
                    ask_final_price_ptr[i] = result.sell.final_price;
                    bid_final_quote_delta_to_bbo_ptr[i] = result.buy.final_quote_delta_to_bbo;
                    ask_final_quote_delta_to_bbo_ptr[i] = result.sell.final_quote_delta_to_bbo;
                    bid_final_distance_to_mid_ptr[i] = result.buy.final_distance_to_mid;
                    ask_final_distance_to_mid_ptr[i] = result.sell.final_distance_to_mid;
                    bid_final_quote_skew_ptr[i] = result.buy.final_quote_skew;
                    ask_final_quote_skew_ptr[i] = result.sell.final_quote_skew;
                    bid_spread_mult_ptr[i] = result.buy.spread_mult;
                    ask_spread_mult_ptr[i] = result.sell.spread_mult;
                    bid_defense_spread_mult_ptr[i] = result.buy.defense_spread_mult;
                    ask_defense_spread_mult_ptr[i] = result.sell.defense_spread_mult;

                    cap_hit_ptr[i] = result.flags.cap_hit ? 1 : 0;
                    delta_cap_ptr[i] = result.flags.delta_cap ? 1 : 0;
                    final_compressed_ptr[i] = result.flags.final_compressed ? 1 : 0;
                    mid_guard_ptr[i] = result.flags.mid_guard ? 1 : 0;
                    post_only_ptr[i] = result.flags.post_only ? 1 : 0;
                    bid_side_adverse_ptr[i] = result.buy.side_adverse ? 1 : 0;
                    ask_side_adverse_ptr[i] = result.sell.side_adverse ? 1 : 0;
                    bid_side_adverse_pause_ptr[i] = result.buy.side_adverse_pause ? 1 : 0;
                    ask_side_adverse_pause_ptr[i] = result.sell.side_adverse_pause ? 1 : 0;
                    bid_adverse_toxicity_ptr[i] = result.buy.adverse_toxicity ? 1 : 0;
                    ask_adverse_toxicity_ptr[i] = result.sell.adverse_toxicity ? 1 : 0;
                    bid_adverse_markout_ptr[i] = result.buy.adverse_markout ? 1 : 0;
                    ask_adverse_markout_ptr[i] = result.sell.adverse_markout ? 1 : 0;
                    bid_adverse_direction_ptr[i] = result.buy.adverse_direction ? 1 : 0;
                    ask_adverse_direction_ptr[i] = result.sell.adverse_direction ? 1 : 0;
                    bid_adverse_ret_ptr[i] = result.buy.adverse_ret ? 1 : 0;
                    ask_adverse_ret_ptr[i] = result.sell.adverse_ret ? 1 : 0;
                    bid_adverse_microprice_ptr[i] = result.buy.adverse_microprice ? 1 : 0;
                    ask_adverse_microprice_ptr[i] = result.sell.adverse_microprice ? 1 : 0;
                    bid_adverse_thin_depth_ptr[i] = result.buy.adverse_thin_depth ? 1 : 0;
                    ask_adverse_thin_depth_ptr[i] = result.sell.adverse_thin_depth ? 1 : 0;
                    bid_defense_guard_ptr[i] = result.buy.defense_guard ? 1 : 0;
                    ask_defense_guard_ptr[i] = result.sell.defense_guard ? 1 : 0;
                    bid_defense_pause_ptr[i] = result.buy.defense_pause ? 1 : 0;
                    ask_defense_pause_ptr[i] = result.sell.defense_pause ? 1 : 0;
                    bid_defense_reducing_ptr[i] = result.buy.defense_reducing ? 1 : 0;
                    ask_defense_reducing_ptr[i] = result.sell.defense_reducing ? 1 : 0;
                    bid_defense_emergency_ptr[i] = result.buy.defense_emergency ? 1 : 0;
                    ask_defense_emergency_ptr[i] = result.sell.defense_emergency ? 1 : 0;
                    bid_mid_guard_ptr[i] = result.buy.mid_guard ? 1 : 0;
                    ask_mid_guard_ptr[i] = result.sell.mid_guard ? 1 : 0;
                    bid_post_only_ptr[i] = result.buy.post_only ? 1 : 0;
                    ask_post_only_ptr[i] = result.sell.post_only ? 1 : 0;
                }
            };

            {
                py::gil_scoped_release release;
                // batch depth 是离线 trace/label 加速路径：每行独立，所以可以释放 GIL 后分片。
                // live scalar 路径不要照搬这里的 worker 结论，线程调度成本会淹没单 tick 收益。
                constexpr py::ssize_t kMinRowsPerWorker = 4096;
                const std::size_t requested_workers = static_cast<std::size_t>(std::max(1, workers));
                const std::size_t useful_workers = n > 0
                    ? static_cast<std::size_t>((n + kMinRowsPerWorker - 1) / kMinRowsPerWorker)
                    : 1U;
                const std::size_t worker_count = std::min(requested_workers, useful_workers);
                if (worker_count <= 1) {
                    run_rows(0, n);
                } else {
                    std::vector<std::jthread> threads;
                    threads.reserve(worker_count);
                    for (std::size_t worker = 0; worker < worker_count; ++worker) {
                        const py::ssize_t begin = static_cast<py::ssize_t>(
                            worker * static_cast<std::size_t>(n) / worker_count);
                        const py::ssize_t end = static_cast<py::ssize_t>(
                            (worker + 1) * static_cast<std::size_t>(n) / worker_count);
                        threads.emplace_back(run_rows, begin, end);
                    }
                }
            }

            py::dict out;
            out["bid_price"] = bid_price;
            out["ask_price"] = ask_price;
            out["spread"] = spread;
            out["raw_half_spread"] = raw_half_spread;
            out["capped_half_spread"] = capped_half_spread;
            out["raw_mid_shift"] = raw_mid_shift;
            out["raw_reservation_shift"] = raw_reservation_shift;
            out["raw_asym_shift"] = raw_asym_shift;
            out["asym"] = asym;
            out["fair"] = fair;
            out["raw_quote_skew"] = raw_quote_skew;
            out["near_depth_total"] = near_depth_total;
            out["book_imb"] = book_imb;
            out["microprice_shift_bps"] = microprice_shift_bps;
            out["kappa_before_depth"] = kappa_before_depth;
            out["kappa_used"] = kappa_used;
            out["depth_tox_mult"] = depth_tox_mult;
            out["cap_bps"] = cap_bps;
            out["max_spread"] = max_spread;
            out["bid_raw_price"] = bid_raw_price;
            out["ask_raw_price"] = ask_raw_price;
            out["bid_pre_guard_price"] = bid_pre_guard_price;
            out["ask_pre_guard_price"] = ask_pre_guard_price;
            out["bid_final_price"] = bid_final_price;
            out["ask_final_price"] = ask_final_price;
            out["bid_final_quote_delta_to_bbo"] = bid_final_quote_delta_to_bbo;
            out["ask_final_quote_delta_to_bbo"] = ask_final_quote_delta_to_bbo;
            out["bid_final_distance_to_mid"] = bid_final_distance_to_mid;
            out["ask_final_distance_to_mid"] = ask_final_distance_to_mid;
            out["bid_final_quote_skew"] = bid_final_quote_skew;
            out["ask_final_quote_skew"] = ask_final_quote_skew;
            out["bid_spread_mult"] = bid_spread_mult;
            out["ask_spread_mult"] = ask_spread_mult;
            out["bid_defense_spread_mult"] = bid_defense_spread_mult;
            out["ask_defense_spread_mult"] = ask_defense_spread_mult;
            out["cap_hit"] = cap_hit;
            out["delta_cap"] = delta_cap;
            out["final_compressed"] = final_compressed;
            out["mid_guard"] = mid_guard;
            out["post_only"] = post_only;
            out["bid_side_adverse"] = bid_side_adverse;
            out["ask_side_adverse"] = ask_side_adverse;
            out["bid_side_adverse_pause"] = bid_side_adverse_pause;
            out["ask_side_adverse_pause"] = ask_side_adverse_pause;
            out["bid_adverse_toxicity"] = bid_adverse_toxicity;
            out["ask_adverse_toxicity"] = ask_adverse_toxicity;
            out["bid_adverse_markout"] = bid_adverse_markout;
            out["ask_adverse_markout"] = ask_adverse_markout;
            out["bid_adverse_direction"] = bid_adverse_direction;
            out["ask_adverse_direction"] = ask_adverse_direction;
            out["bid_adverse_ret"] = bid_adverse_ret;
            out["ask_adverse_ret"] = ask_adverse_ret;
            out["bid_adverse_microprice"] = bid_adverse_microprice;
            out["ask_adverse_microprice"] = ask_adverse_microprice;
            out["bid_adverse_thin_depth"] = bid_adverse_thin_depth;
            out["ask_adverse_thin_depth"] = ask_adverse_thin_depth;
            out["bid_defense_guard"] = bid_defense_guard;
            out["ask_defense_guard"] = ask_defense_guard;
            out["bid_defense_pause"] = bid_defense_pause;
            out["ask_defense_pause"] = ask_defense_pause;
            out["bid_defense_reducing"] = bid_defense_reducing;
            out["ask_defense_reducing"] = ask_defense_reducing;
            out["bid_defense_emergency"] = bid_defense_emergency;
            out["ask_defense_emergency"] = ask_defense_emergency;
            out["bid_mid_guard"] = bid_mid_guard;
            out["ask_mid_guard"] = ask_mid_guard;
            out["bid_post_only"] = bid_post_only;
            out["ask_post_only"] = ask_post_only;
            return out;
        },
        py::arg("mid"),
        py::arg("inventory"),
        py::arg("sigma_sq"),
        py::arg("trade_intensity"),
        py::arg("best_bid"),
        py::arg("best_ask"),
        py::arg("dir_10s"),
        py::arg("vol_10s"),
        py::arg("ret_10s"),
        py::arg("tox_bid"),
        py::arg("tox_ask"),
        py::arg("mo_ema_bid"),
        py::arg("mo_ema_ask"),
        py::arg("mo_ema_all"),
        py::arg("mo_ref"),
        py::arg("ber_active"),
        py::arg("position_open"),
        py::arg("hold_time_s"),
        py::arg("unrealized_pnl"),
        py::arg("l2_bid_px"),
        py::arg("l2_bid_qty"),
        py::arg("l2_ask_px"),
        py::arg("l2_ask_qty"),
        py::arg("cfg"),
        py::arg("workers") = 1
    );
}


}  // namespace narrowgate_cpp
