#include <pybind11/pybind11.h>

#include "binding_registry.hpp"

namespace py = pybind11;

#ifndef NARROWGATE_LIVE_CPU_PROFILE_NAME
#define NARROWGATE_LIVE_CPU_PROFILE_NAME "unknown"
#endif

#ifndef NARROWGATE_LIVE_CPU_COMPILE_OPTIONS
#define NARROWGATE_LIVE_CPU_COMPILE_OPTIONS "unknown"
#endif

#ifndef NARROWGATE_LIVE_BUILD_IS_PRODUCTION
#define NARROWGATE_LIVE_BUILD_IS_PRODUCTION 0
#endif

#ifndef NARROWGATE_LIVE_VECTOR_WIDTH_BITS
#define NARROWGATE_LIVE_VECTOR_WIDTH_BITS 0
#endif

PYBIND11_MODULE(narrowgate_cpp, m) {
    m.doc() = "C++ acceleration hooks for NarrowGate.";
    m.attr("NATIVE_LIVE_BUILD_PROFILE") = py::str(NARROWGATE_LIVE_CPU_PROFILE_NAME);
    m.attr("NATIVE_LIVE_BUILD_COMPILE_OPTIONS") =
        py::str(NARROWGATE_LIVE_CPU_COMPILE_OPTIONS);
    m.attr("NATIVE_LIVE_BUILD_IS_PRODUCTION") =
        py::bool_(NARROWGATE_LIVE_BUILD_IS_PRODUCTION != 0);
    m.attr("NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS") =
        py::int_(NARROWGATE_LIVE_VECTOR_WIDTH_BITS);
    narrowgate_cpp::bind_common(m);
    narrowgate_cpp::bind_transport_contract(m);
    narrowgate_cpp::bind_dynamic_fill_hazard(m);
    narrowgate_cpp::bind_quote_core(m);
    narrowgate_cpp::bind_tick_replay(m);
    narrowgate_cpp::bind_global_flow(m);
    narrowgate_cpp::bind_streaming_features(m);
    narrowgate_cpp::bind_f03_causal_v12_one_second_features(m);
    narrowgate_cpp::bind_request_state_features(m);
    narrowgate_cpp::bind_risk_set_expansion(m);
    narrowgate_cpp::bind_sparse_order_lifecycle(m);
    narrowgate_cpp::bind_active_order_competing_risk_cif(m);
    narrowgate_cpp::bind_order_lifecycle_journal_v2_mirror(m);
    narrowgate_cpp::bind_live_order_state(m);
    narrowgate_cpp::bind_live_order_action_plan(m);
    narrowgate_cpp::bind_replace_continuation(m);
    narrowgate_cpp::bind_order_gateway_core(m);
    narrowgate_cpp::bind_live_runtime_core(m);
    narrowgate_cpp::bind_live_cooldown(m);
}
