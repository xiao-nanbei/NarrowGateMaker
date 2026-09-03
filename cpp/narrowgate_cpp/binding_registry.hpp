#pragma once

#include <pybind11/pybind11.h>

namespace narrowgate_cpp {

void bind_common(pybind11::module_& m);
void bind_transport_contract(pybind11::module_& m);
void bind_dynamic_fill_hazard(pybind11::module_& m);
void bind_quote_core(pybind11::module_& m);
void bind_f05_policy_types(pybind11::module_& m);
void bind_tick_replay(pybind11::module_& m);
void bind_global_flow(pybind11::module_& m);
void bind_streaming_features(pybind11::module_& m);
void bind_f03_causal_v12_one_second_features(pybind11::module_& m);
void bind_request_state_features(pybind11::module_& m);
void bind_risk_set_expansion(pybind11::module_& m);
void bind_sparse_order_lifecycle(pybind11::module_& m);
void bind_active_order_competing_risk_cif(pybind11::module_& m);
void bind_order_lifecycle_journal_v2_mirror(pybind11::module_& m);
void bind_live_order_state(pybind11::module_& m);
void bind_live_order_action_plan(pybind11::module_& m);
void bind_replace_continuation(pybind11::module_& m);
void bind_order_gateway_core(pybind11::module_& m);
void bind_live_runtime_core(pybind11::module_& m);
void bind_live_cooldown(pybind11::module_& m);

}  // namespace narrowgate_cpp
