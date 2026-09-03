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
    // The live execution feed is top-20.  Reserve its normal shape once per
    // side without using the Python length as a limit: diagnostic/replay
    // callers with deeper books must still be consumed in full.
    constexpr std::size_t kLiveDepthLevels = 20;
    depth.bids.reserve(kLiveDepthLevels);
    depth.asks.reserve(kLiveDepthLevels);
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

void bind_live_runtime_core(py::module_& m) {
    py::enum_<MarketStateUpdateStatus>(m, "MarketStateUpdateStatus")
        .value("Applied", MarketStateUpdateStatus::Applied)
        .value("InvalidDepthSize", MarketStateUpdateStatus::InvalidDepthSize)
        .value("InvalidLevel", MarketStateUpdateStatus::InvalidLevel)
        .value("InvalidPriceOrder", MarketStateUpdateStatus::InvalidPriceOrder)
        .value("InvalidClock", MarketStateUpdateStatus::InvalidClock)
        .value("ClockRegressed", MarketStateUpdateStatus::ClockRegressed)
        .value("GenerationRegressed", MarketStateUpdateStatus::GenerationRegressed)
        .value("CrossedBook", MarketStateUpdateStatus::CrossedBook)
        .value("WriterBusy", MarketStateUpdateStatus::WriterBusy);

    py::enum_<NativeLiveDecisionStatus>(m, "NativeLiveDecisionStatus")
        .value("Applied", NativeLiveDecisionStatus::Applied)
        .value("Busy", NativeLiveDecisionStatus::Busy)
        .value("NoBook", NativeLiveDecisionStatus::NoBook)
        .value("InvalidInput", NativeLiveDecisionStatus::InvalidInput)
        .value(
            "DecisionClockRegressed",
            NativeLiveDecisionStatus::DecisionClockRegressed
        )
        .value("StaleBook", NativeLiveDecisionStatus::StaleBook)
        .value(
            "MarketIdentityMismatch",
            NativeLiveDecisionStatus::MarketIdentityMismatch
        )
        .value("FeedFault", NativeLiveDecisionStatus::FeedFault)
        .value("InvalidOutput", NativeLiveDecisionStatus::InvalidOutput);

    py::class_<MarketClockIdentity>(m, "MarketClockIdentity")
        .def(py::init<>())
        .def_readwrite("source_ts_ns", &MarketClockIdentity::source_ts_ns)
        .def_readwrite("exchange_ts_ns", &MarketClockIdentity::exchange_ts_ns)
        .def_readwrite("receive_ts_ns", &MarketClockIdentity::receive_ts_ns)
        .def_readwrite("visible_ts_ns", &MarketClockIdentity::visible_ts_ns)
        .def_readwrite("generation", &MarketClockIdentity::generation);

    py::class_<Depth20SideUpdate>(m, "Depth20SideUpdate")
        .def(py::init<>())
        .def_readwrite("price_ticks", &Depth20SideUpdate::price_ticks)
        .def_readwrite("quantity_lots", &Depth20SideUpdate::quantity_lots)
        .def_readwrite("clock", &Depth20SideUpdate::clock)
        .def_readwrite("size", &Depth20SideUpdate::size);

    py::class_<CommonSidePolicyInputPod>(m, "CommonSidePolicyInputPod")
        .def(py::init<>())
#define BIND_COMMON_POLICY_INPUT(field) \
        .def_readwrite(#field, &CommonSidePolicyInputPod::field)
        BIND_COMMON_POLICY_INPUT(exposure_increasing)
        BIND_COMMON_POLICY_INPUT(fill_cooldown_active)
        BIND_COMMON_POLICY_INPUT(side_adverse)
        BIND_COMMON_POLICY_INPUT(side_adverse_pause)
        BIND_COMMON_POLICY_INPUT(local_extreme_guard)
        BIND_COMMON_POLICY_INPUT(local_extreme_pause)
        BIND_COMMON_POLICY_INPUT(defense_guard)
        BIND_COMMON_POLICY_INPUT(defense_pause)
        BIND_COMMON_POLICY_INPUT(inventory_ratio)
        BIND_COMMON_POLICY_INPUT(depth_age_s)
        BIND_COMMON_POLICY_INPUT(max_book_age_s)
        BIND_COMMON_POLICY_INPUT(toxicity)
        BIND_COMMON_POLICY_INPUT(markout_ema)
        BIND_COMMON_POLICY_INPUT(markout_spread_scale)
        BIND_COMMON_POLICY_INPUT(markout_reference)
        BIND_COMMON_POLICY_INPUT(microprice_shift_bps)
        BIND_COMMON_POLICY_INPUT(l2_quote_flip_rate)
        BIND_COMMON_POLICY_INPUT(l2_book_cancel_ratio)
        BIND_COMMON_POLICY_INPUT(l2_near_depth_total)
        BIND_COMMON_POLICY_INPUT(thin_depth_threshold)
        BIND_COMMON_POLICY_INPUT(kappa_depth_baseline)
        BIND_COMMON_POLICY_INPUT(local_extreme_spread_mult)
        BIND_COMMON_POLICY_INPUT(defense_spread_mult);
#undef BIND_COMMON_POLICY_INPUT

    py::class_<CommonSidePolicyResultPod>(m, "CommonSidePolicyResultPod")
        .def_readonly("allow_post", &CommonSidePolicyResultPod::allow_post)
        .def_readonly(
            "allow_exposure_increase",
            &CommonSidePolicyResultPod::allow_exposure_increase
        )
        .def_readonly("spread_mult", &CommonSidePolicyResultPod::spread_mult)
        .def_readonly("size_mult", &CommonSidePolicyResultPod::size_mult)
        .def_readonly("reason_mask", &CommonSidePolicyResultPod::reason_mask);

    py::class_<NativeQuotePolicyStageResult>(m, "NativeQuotePolicyStageResult")
        .def_readonly("quote", &NativeQuotePolicyStageResult::quote)
        .def_readonly("buy_policy", &NativeQuotePolicyStageResult::buy_policy)
        .def_readonly("sell_policy", &NativeQuotePolicyStageResult::sell_policy);

    py::class_<NativeQuotePolicyStage>(m, "NativeQuotePolicyStage")
        .def(py::init<QuoteCoreConfig>(), py::arg("config"))
        .def(
            "compute",
            [](const NativeQuotePolicyStage& self,
               py::sequence state_values,
               py::sequence pred_values,
               py::handle bids,
               py::handle asks,
               py::sequence buy_policy_values,
               py::sequence sell_policy_values) {
                if (py::len(state_values) != 16 || py::len(pred_values) != 5 ||
                    py::len(buy_policy_values) != 23 ||
                    py::len(sell_policy_values) != 23) {
                    throw std::invalid_argument(
                        "native quote-policy stage input length mismatch"
                    );
                }
                QuoteState state;
#define READ_STAGE_STATE(index, field) \
                state.field = py::cast<decltype(state.field)>(state_values[index])
                READ_STAGE_STATE(0, mid);
                READ_STAGE_STATE(1, inventory);
                READ_STAGE_STATE(2, sigma_sq);
                READ_STAGE_STATE(3, trade_intensity);
                READ_STAGE_STATE(4, best_bid);
                READ_STAGE_STATE(5, best_ask);
                READ_STAGE_STATE(6, ber_active);
                READ_STAGE_STATE(7, mo_ema_all);
                READ_STAGE_STATE(8, mo_ema_bid);
                READ_STAGE_STATE(9, mo_ema_ask);
                READ_STAGE_STATE(10, bid_adverse_markout_pause_latch);
                READ_STAGE_STATE(11, ask_adverse_markout_pause_latch);
                READ_STAGE_STATE(12, mo_ref);
                READ_STAGE_STATE(13, position_open);
                READ_STAGE_STATE(14, hold_time_s);
                READ_STAGE_STATE(15, unrealized_pnl);
#undef READ_STAGE_STATE
                QuotePrediction prediction;
                prediction.dir_10s = py::cast<double>(pred_values[0]);
                prediction.vol_10s = py::cast<double>(pred_values[1]);
                prediction.ret_10s = py::cast<double>(pred_values[2]);
                prediction.tox_bid = py::cast<double>(pred_values[3]);
                prediction.tox_ask = py::cast<double>(pred_values[4]);
                const auto read_policy = [](const py::sequence& values) {
                    CommonSidePolicyInputPod policy;
#define READ_STAGE_POLICY(index, field) \
                    policy.field = py::cast<decltype(policy.field)>(values[index])
                    READ_STAGE_POLICY(0, exposure_increasing);
                    READ_STAGE_POLICY(1, fill_cooldown_active);
                    READ_STAGE_POLICY(2, side_adverse);
                    READ_STAGE_POLICY(3, side_adverse_pause);
                    READ_STAGE_POLICY(4, local_extreme_guard);
                    READ_STAGE_POLICY(5, local_extreme_pause);
                    READ_STAGE_POLICY(6, defense_guard);
                    READ_STAGE_POLICY(7, defense_pause);
                    READ_STAGE_POLICY(8, inventory_ratio);
                    READ_STAGE_POLICY(9, depth_age_s);
                    READ_STAGE_POLICY(10, max_book_age_s);
                    READ_STAGE_POLICY(11, toxicity);
                    READ_STAGE_POLICY(12, markout_ema);
                    READ_STAGE_POLICY(13, markout_spread_scale);
                    READ_STAGE_POLICY(14, markout_reference);
                    READ_STAGE_POLICY(15, microprice_shift_bps);
                    READ_STAGE_POLICY(16, l2_quote_flip_rate);
                    READ_STAGE_POLICY(17, l2_book_cancel_ratio);
                    READ_STAGE_POLICY(18, l2_near_depth_total);
                    READ_STAGE_POLICY(19, thin_depth_threshold);
                    READ_STAGE_POLICY(20, kappa_depth_baseline);
                    READ_STAGE_POLICY(21, local_extreme_spread_mult);
                    READ_STAGE_POLICY(22, defense_spread_mult);
#undef READ_STAGE_POLICY
                    return policy;
                };
                const DepthSnapshot depth = depth_from_python_levels(bids, asks);
                const CommonSidePolicyInputPod buy_policy = read_policy(
                    buy_policy_values
                );
                const CommonSidePolicyInputPod sell_policy = read_policy(
                    sell_policy_values
                );
                py::gil_scoped_release release;
                return self.compute(
                    state,
                    prediction,
                    depth.view(),
                    buy_policy,
                    sell_policy
                );
            },
            py::arg("state_values"),
            py::arg("pred_values"),
            py::arg("bids"),
            py::arg("asks"),
            py::arg("buy_policy"),
            py::arg("sell_policy")
        );

    py::class_<LiveRoutingResult>(m, "LiveRoutingResult")
        .def_readonly("bid_price", &LiveRoutingResult::bid_price)
        .def_readonly("ask_price", &LiveRoutingResult::ask_price)
        .def_readonly("bid_size", &LiveRoutingResult::bid_size)
        .def_readonly("ask_size", &LiveRoutingResult::ask_size)
        .def_readonly(
            "post_policy_cap_hit",
            &LiveRoutingResult::post_policy_cap_hit
        )
        .def_readonly(
            "can_bid_after_inventory",
            &LiveRoutingResult::can_bid_after_inventory
        )
        .def_readonly(
            "can_ask_after_inventory",
            &LiveRoutingResult::can_ask_after_inventory
        )
        .def_readonly("can_bid", &LiveRoutingResult::can_bid)
        .def_readonly("can_ask", &LiveRoutingResult::can_ask)
        .def_readonly("bid_needs_update", &LiveRoutingResult::bid_needs_update)
        .def_readonly("ask_needs_update", &LiveRoutingResult::ask_needs_update);

    py::class_<NativeLiveDecisionInput>(m, "NativeLiveDecisionInput")
        .def(py::init<>())
#define BIND_LIVE_DECISION_INPUT(field) \
        .def_readwrite(#field, &NativeLiveDecisionInput::field)
        BIND_LIVE_DECISION_INPUT(quote_state)
        BIND_LIVE_DECISION_INPUT(prediction)
        BIND_LIVE_DECISION_INPUT(buy_policy)
        BIND_LIVE_DECISION_INPUT(sell_policy)
        BIND_LIVE_DECISION_INPUT(decision_ts_ns)
        BIND_LIVE_DECISION_INPUT(max_book_age_ns)
        BIND_LIVE_DECISION_INPUT(expected_market_publication_sequence)
        BIND_LIVE_DECISION_INPUT(expected_bid_generation)
        BIND_LIVE_DECISION_INPUT(expected_ask_generation)
        BIND_LIVE_DECISION_INPUT(min_qty)
        BIND_LIVE_DECISION_INPUT(min_notional)
        BIND_LIVE_DECISION_INPUT(size_eta)
        BIND_LIVE_DECISION_INPUT(requote_threshold_bps)
        BIND_LIVE_DECISION_INPUT(routing_max_spread)
        BIND_LIVE_DECISION_INPUT(bid_active_price)
        BIND_LIVE_DECISION_INPUT(bid_age_ms)
        BIND_LIVE_DECISION_INPUT(ask_active_price)
        BIND_LIVE_DECISION_INPUT(ask_age_ms)
        BIND_LIVE_DECISION_INPUT(bid_order_ttl_ms)
        BIND_LIVE_DECISION_INPUT(ask_order_ttl_ms)
        BIND_LIVE_DECISION_INPUT(symmetric_size)
        BIND_LIVE_DECISION_INPUT(bid_active)
        BIND_LIVE_DECISION_INPUT(ask_active);
#undef BIND_LIVE_DECISION_INPUT

    py::class_<NativeLiveDecisionResult>(m, "NativeLiveDecisionResult")
        .def_readonly("status", &NativeLiveDecisionResult::status)
        .def_readonly("quote", &NativeLiveDecisionResult::quote)
        .def_readonly("buy_policy", &NativeLiveDecisionResult::buy_policy)
        .def_readonly("sell_policy", &NativeLiveDecisionResult::sell_policy)
        .def_readonly("routing", &NativeLiveDecisionResult::routing)
        .def_readonly(
            "market_publication_sequence",
            &NativeLiveDecisionResult::market_publication_sequence
        )
        .def_readonly(
            "decision_sequence",
            &NativeLiveDecisionResult::decision_sequence
        )
        .def_readonly("book_age_ns", &NativeLiveDecisionResult::book_age_ns);

    py::class_<NativeLiveRuntimeCore>(m, "NativeLiveRuntimeCore")
        .def(py::init<QuoteCoreConfig>(), py::arg("config"))
        .def(
            "publish_book",
            [](NativeLiveRuntimeCore& self,
               const Depth20SideUpdate& bids,
               const Depth20SideUpdate& asks) {
                const Depth20SideUpdate bids_copy = bids;
                const Depth20SideUpdate asks_copy = asks;
                py::gil_scoped_release release;
                return self.publish_book(bids_copy, asks_copy);
            },
            py::arg("bids"),
            py::arg("asks")
        )
        .def(
            "decide",
            [](NativeLiveRuntimeCore& self,
               const NativeLiveDecisionInput& input) {
                const NativeLiveDecisionInput input_copy = input;
                py::gil_scoped_release release;
                return self.decide(input_copy);
            },
            py::arg("input")
        )
        .def_property_readonly("decision_count", &NativeLiveRuntimeCore::decision_count)
        .def_property_readonly(
            "market_publication_sequence",
            &NativeLiveRuntimeCore::market_publication_sequence
        )
        .def_property_readonly(
            "feed_fault_epoch",
            &NativeLiveRuntimeCore::feed_fault_epoch
        )
        .def_property_readonly(
            "feed_resync_epoch",
            &NativeLiveRuntimeCore::feed_resync_epoch
        )
        .def_property_readonly(
            "feed_fault_latched",
            &NativeLiveRuntimeCore::feed_fault_latched
        )
        .def_property_readonly_static(
            "cache_line_bytes",
            [](py::object) { return NativeLiveRuntimeCore::cache_line_bytes(); }
        )
        .def_property_readonly_static(
            "core_size_bytes",
            [](py::object) { return NativeLiveRuntimeCore::core_size_bytes(); }
        )
        .def_property_readonly_static(
            "core_alignment_bytes",
            [](py::object) { return NativeLiveRuntimeCore::core_alignment_bytes(); }
        );

    m.attr("NATIVE_LIVE_RUNTIME_CORE_AVAILABLE") = py::bool_(true);
    m.attr("NATIVE_QUOTE_POLICY_STAGE_AVAILABLE") = py::bool_(true);
    m.attr("NATIVE_LIVE_RUNTIME_WIRE_ADAPTER_AVAILABLE") = py::bool_(false);
}

void bind_live_cooldown(py::module_& m) {
    py::enum_<LiveCooldownProfile>(m, "LiveCooldownProfile")
        .value("SELL_SELECTED", LiveCooldownProfile::SellSelected)
        .value("BUY_E3", LiveCooldownProfile::BuyE3);
    py::enum_<LiveCooldownObserveStatus>(m, "LiveCooldownObserveStatus")
        .value("PENDING_ONLY", LiveCooldownObserveStatus::PendingOnly)
        .value("APPLIED", LiveCooldownObserveStatus::Applied)
        .value(
            "OUT_OF_ORDER_IGNORED",
            LiveCooldownObserveStatus::OutOfOrderIgnored
        )
        .value("GAP_RESET", LiveCooldownObserveStatus::GapReset)
        .value("INVALID_CALLBACK", LiveCooldownObserveStatus::InvalidCallback);
    py::enum_<LiveCooldownDecisionStatus>(m, "LiveCooldownDecisionStatus")
        .value("RULE_MATCHED", LiveCooldownDecisionStatus::RuleMatched)
        .value("NO_RULE_MATCHED", LiveCooldownDecisionStatus::NoRuleMatched)
        .value(
            "NO_COMPLETED_WINDOW",
            LiveCooldownDecisionStatus::NoCompletedWindow
        )
        .value("WARMUP_INCOMPLETE", LiveCooldownDecisionStatus::WarmupIncomplete)
        .value("FEATURE_STATE_STALE", LiveCooldownDecisionStatus::FeatureStateStale)
        .value(
            "LATEST_WINDOW_UNOBSERVED",
            LiveCooldownDecisionStatus::LatestWindowUnobserved
        )
        .value(
            "SELECTED_PREDICATE_UNOBSERVED",
            LiveCooldownDecisionStatus::SelectedPredicateUnobserved
        )
        .value("RULE_UNOBSERVED", LiveCooldownDecisionStatus::RuleUnobserved);

    py::class_<LiveCooldownDecisionPod>(m, "LiveCooldownDecisionPod")
        .def_readonly("duration_ms", &LiveCooldownDecisionPod::duration_ms)
        .def_readonly(
            "matched_rule_index",
            &LiveCooldownDecisionPod::matched_rule_index
        )
        .def_readonly("detail_index", &LiveCooldownDecisionPod::detail_index)
        .def_readonly(
            "feature_ready_ts_ns",
            &LiveCooldownDecisionPod::feature_ready_ts_ns
        )
        .def_readonly("feature_age_ms", &LiveCooldownDecisionPod::feature_age_ms)
        .def_readonly("support_valid", &LiveCooldownDecisionPod::support_valid)
        .def_readonly("status", &LiveCooldownDecisionPod::status)
        .def_property_readonly(
            "predicate_values",
            [](const LiveCooldownDecisionPod& self) {
                py::tuple output(self.predicate_count);
                for (std::size_t index = 0; index < self.predicate_count; ++index) {
                    output[index] = py::int_(self.predicate_values[index]);
                }
                return output;
            }
        );

    py::class_<LiveCooldownAuditPod>(m, "LiveCooldownAuditPod")
#define BIND_LIVE_COOLDOWN_AUDIT(field) \
        .def_readonly(#field, &LiveCooldownAuditPod::field)
        BIND_LIVE_COOLDOWN_AUDIT(updates)
        BIND_LIVE_COOLDOWN_AUDIT(completed_windows)
        BIND_LIVE_COOLDOWN_AUDIT(gap_windows)
        BIND_LIVE_COOLDOWN_AUDIT(resets)
        BIND_LIVE_COOLDOWN_AUDIT(invalid_updates)
        BIND_LIVE_COOLDOWN_AUDIT(out_of_order_updates)
        BIND_LIVE_COOLDOWN_AUDIT(gap_resets)
        BIND_LIVE_COOLDOWN_AUDIT(warmup_admitted)
        BIND_LIVE_COOLDOWN_AUDIT(feature_ready_ts_ns)
        BIND_LIVE_COOLDOWN_AUDIT(warmup_start_right_ts_ns)
        BIND_LIVE_COOLDOWN_AUDIT(last_window_right_ts_ns);
#undef BIND_LIVE_COOLDOWN_AUDIT

    py::class_<LiveCooldownFeatureSnapshotPod>(
        m,
        "LiveCooldownFeatureSnapshotPod"
    )
        .def_readonly(
            "current_window_observed",
            &LiveCooldownFeatureSnapshotPod::current_window_observed
        )
        .def_readonly(
            "ema_initialized",
            &LiveCooldownFeatureSnapshotPod::ema_initialized
        )
        .def_readonly(
            "last_observed_ts_ns",
            &LiveCooldownFeatureSnapshotPod::last_observed_ts_ns
        )
        .def_property_readonly("ema", [](const LiveCooldownFeatureSnapshotPod& self) {
            return std::vector<double>(self.ema.begin(), self.ema.begin() + self.ema_count);
        })
        .def_property_readonly(
            "velocity",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<double>(
                    self.velocity.begin(), self.velocity.begin() + self.ema_count
                );
            }
        )
        .def_property_readonly(
            "acceleration",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<double>(
                    self.acceleration.begin(),
                    self.acceleration.begin() + self.ema_count
                );
            }
        )
        .def_property_readonly(
            "effective_sign",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<std::int8_t>(
                    self.effective_sign.begin(),
                    self.effective_sign.begin() + self.pair_count
                );
            }
        )
        .def_property_readonly(
            "last_cross_direction",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<std::int8_t>(
                    self.last_cross_direction.begin(),
                    self.last_cross_direction.begin() + self.pair_count
                );
            }
        )
        .def_property_readonly(
            "arrangement_start_ts_ns",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<std::int64_t>(
                    self.arrangement_start_ts_ns.begin(),
                    self.arrangement_start_ts_ns.begin() + self.pair_count
                );
            }
        )
        .def_property_readonly(
            "last_cross_ts_ns",
            [](const LiveCooldownFeatureSnapshotPod& self) {
                return std::vector<std::int64_t>(
                    self.last_cross_ts_ns.begin(),
                    self.last_cross_ts_ns.begin() + self.pair_count
                );
            }
        );

    py::class_<NativeLiveCooldownHotPath>(m, "NativeLiveCooldownHotPath")
        .def(
            py::init<
                LiveCooldownProfile,
                const F05BooleanPolicy&,
                double,
                double>(),
            py::arg("profile"),
            py::arg("policy"),
            py::arg("warmup_s"),
            py::arg("max_feature_age_s")
        )
        .def(
            "observe_depth",
            [](NativeLiveCooldownHotPath& self,
               std::int64_t receive_ts_ns,
               double best_bid,
               double best_ask) {
                py::gil_scoped_release release;
                return self.observe_depth(receive_ts_ns, best_bid, best_ask);
            },
            py::arg("receive_ts_ns"),
            py::arg("best_bid"),
            py::arg("best_ask")
        )
        .def(
            "evaluate",
            [](NativeLiveCooldownHotPath& self,
               std::int64_t decision_ts_ns,
               double campaign_age_s,
               std::int64_t baseline_duration_ms) {
                py::gil_scoped_release release;
                return self.evaluate(
                    decision_ts_ns,
                    campaign_age_s,
                    baseline_duration_ms
                );
            },
            py::arg("decision_ts_ns"),
            py::arg("campaign_age_s"),
            py::arg("baseline_duration_ms")
        )
        .def("reset", &NativeLiveCooldownHotPath::reset)
        .def("audit", &NativeLiveCooldownHotPath::audit)
        .def("feature_snapshot", &NativeLiveCooldownHotPath::feature_snapshot)
        .def_property_readonly("profile", &NativeLiveCooldownHotPath::profile)
        .def_property_readonly(
            "core_size_bytes",
            &NativeLiveCooldownHotPath::core_size_bytes
        )
        .def_property_readonly_static(
            "cache_line_bytes",
            [](py::object) { return NativeLiveCooldownHotPath::cache_line_bytes(); }
        );

    m.attr("NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE") = py::bool_(true);
}

}  // namespace narrowgate_cpp
