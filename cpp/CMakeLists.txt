cmake_minimum_required(VERSION 3.18)
project(narrowgate_cpp LANGUAGES CXX)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(
    NARROWGATE_LIVE_CPU_PROFILE
    "portable"
    CACHE STRING
    "Native extension CPU profile: portable, ec2-cascadelake-avx2, or host-native"
)
set_property(
    CACHE NARROWGATE_LIVE_CPU_PROFILE
    PROPERTY STRINGS portable ec2-cascadelake-avx2 host-native
)
option(
    NARROWGATE_BUILD_NATIVE_BENCHMARKS
    "Build standalone live-runtime selftest and benchmark binaries"
    OFF
)

set(NARROWGATE_LIVE_BUILD_IS_PRODUCTION 0)
set(NARROWGATE_LIVE_VECTOR_WIDTH_BITS 0)
set(NARROWGATE_LIVE_COMPILE_OPTIONS -ffp-contract=off)
set(NARROWGATE_LIVE_LINK_OPTIONS)
if(NARROWGATE_LIVE_CPU_PROFILE STREQUAL "ec2-cascadelake-avx2")
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(FATAL_ERROR "EC2 Cascade Lake profile requires Linux")
    endif()
    if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
        message(FATAL_ERROR "EC2 Cascade Lake profile requires x86_64")
    endif()
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR
       NOT CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "11.5.0")
        message(FATAL_ERROR "EC2 Cascade Lake profile requires GNU C++ 11.5.0")
    endif()
    string(TOUPPER "${CMAKE_BUILD_TYPE}" NARROWGATE_BUILD_TYPE_UPPER)
    if(CMAKE_CONFIGURATION_TYPES OR
       NOT NARROWGATE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
        message(FATAL_ERROR "EC2 Cascade Lake profile requires a single-config Release build")
    endif()
    # pybind11_add_module enables LTO implicitly when this variable is
    # undefined.  The matched-host benchmark measured the non-LTO
    # Haswell-ISA/Cascade-Lake-tuned build as faster, so make the production
    # choice explicit before creating the extension target.  This is part of
    # the EC2 profile, not a portability concession.
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)
    set(NARROWGATE_LIVE_BUILD_IS_PRODUCTION 1)
    set(NARROWGATE_LIVE_VECTOR_WIDTH_BITS 256)
    set(NARROWGATE_LIVE_COMPILE_OPTIONS
        -O3
        -march=haswell
        -mtune=cascadelake
        -mprefer-vector-width=256
        -fno-fast-math
        -ffp-contract=off
        -fno-lto
    )
    set(NARROWGATE_LIVE_LINK_OPTIONS -fno-lto)
elseif(NARROWGATE_LIVE_CPU_PROFILE STREQUAL "host-native")
    if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
        message(FATAL_ERROR "host-native live profile is reserved for x86_64 release hosts")
    endif()
    list(APPEND NARROWGATE_LIVE_COMPILE_OPTIONS
        -O3
        -march=native
        -mtune=native
    )
elseif(NOT NARROWGATE_LIVE_CPU_PROFILE STREQUAL "portable")
    message(FATAL_ERROR "Unknown NARROWGATE_LIVE_CPU_PROFILE=${NARROWGATE_LIVE_CPU_PROFILE}")
endif()

string(JOIN " " NARROWGATE_LIVE_COMPILE_OPTIONS_TEXT ${NARROWGATE_LIVE_COMPILE_OPTIONS})

set(NARROWGATE_LIVE_CORE_SOURCES
    narrowgate_cpp/live_cooldown.cpp
    narrowgate_cpp/live_order_action_plan.cpp
    narrowgate_cpp/live_order_state.cpp
    narrowgate_cpp/live_policy.cpp
    narrowgate_cpp/live_risk_state.cpp
    narrowgate_cpp/live_runtime_core.cpp
    narrowgate_cpp/order_gateway_core.cpp
    narrowgate_cpp/quote_core.cpp
    narrowgate_cpp/replace_continuation.cpp
)

find_package(Python COMPONENTS Interpreter REQUIRED)

execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_path('include') or '')"
    OUTPUT_VARIABLE NARROWGATE_PYTHON_INCLUDE
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(APPLE AND NARROWGATE_PYTHON_INCLUDE AND NOT EXISTS "${NARROWGATE_PYTHON_INCLUDE}/Python.h")
    set(NARROWGATE_CLT_PYTHON_HEADERS
        "/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/${Python_VERSION_MAJOR}.${Python_VERSION_MINOR}/Headers"
    )
    if(EXISTS "${NARROWGATE_CLT_PYTHON_HEADERS}/Python.h")
        message(STATUS "Using CLT Python headers: ${NARROWGATE_CLT_PYTHON_HEADERS}")
        set(Python_INCLUDE_DIR "${NARROWGATE_CLT_PYTHON_HEADERS}" CACHE PATH "Python include directory" FORCE)
        set(Python3_INCLUDE_DIR "${NARROWGATE_CLT_PYTHON_HEADERS}" CACHE PATH "Python3 include directory" FORCE)
    endif()
endif()

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(narrowgate_cpp
    narrowgate_cpp/bindings.cpp
    narrowgate_cpp/dynamic_fill_hazard.cpp
    narrowgate_cpp/global_flow.cpp
    ${NARROWGATE_LIVE_CORE_SOURCES}
    ../research/families/f06_placement_fill_cif/cpp/request_state_features.cpp
    ../research/families/f06_placement_fill_cif/cpp/risk_set_expansion.cpp
    ../research/families/f06_placement_fill_cif/cpp/sparse_order_lifecycle.cpp
    ../research/families/f07_active_order_continuation/cpp/active_order_competing_risk_cif.cpp
    ../research/families/f07_active_order_continuation/cpp/order_lifecycle_journal_v2_mirror.cpp
    ../research/families/f03_causal_13_head/cpp/causal_v12_1s_features.cpp
    narrowgate_cpp/tick_replay.cpp
    narrowgate_cpp/streaming_features.cpp
)

if(NARROWGATE_LIVE_BUILD_IS_PRODUCTION)
    # Keep the target property explicit as well as the directory variable
    # above.  This prevents a later project-wide IPO default from silently
    # changing the measured production profile.
    set_target_properties(
        narrowgate_cpp
        PROPERTIES INTERPROCEDURAL_OPTIMIZATION FALSE
    )
endif()

target_compile_definitions(
    narrowgate_cpp
    PRIVATE
        NARROWGATE_LIVE_CPU_PROFILE_NAME="${NARROWGATE_LIVE_CPU_PROFILE}"
        NARROWGATE_LIVE_CPU_COMPILE_OPTIONS="${NARROWGATE_LIVE_COMPILE_OPTIONS_TEXT}"
        NARROWGATE_LIVE_BUILD_IS_PRODUCTION=${NARROWGATE_LIVE_BUILD_IS_PRODUCTION}
        NARROWGATE_LIVE_VECTOR_WIDTH_BITS=${NARROWGATE_LIVE_VECTOR_WIDTH_BITS}
)

if(NARROWGATE_LIVE_COMPILE_OPTIONS)
    # A production wheel is an x86 deployment artifact, not a portable wheel
    # with a few tuned translation units.  Apply the measured CPU profile to
    # the complete extension so signal features, pybind boundary code and the
    # native order path are built for the same target as the fused quote core.
    # The default portable profile remains available only for semantic tests
    # on non-production developer hosts.
    target_compile_options(
        narrowgate_cpp
        PRIVATE ${NARROWGATE_LIVE_COMPILE_OPTIONS}
    )
endif()
if(NARROWGATE_LIVE_LINK_OPTIONS)
    target_link_options(narrowgate_cpp PRIVATE ${NARROWGATE_LIVE_LINK_OPTIONS})
endif()

target_compile_features(narrowgate_cpp PRIVATE cxx_std_20)

target_include_directories(narrowgate_cpp
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/narrowgate_cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../research/families/f03_causal_13_head/cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../research/families/f06_placement_fill_cif/cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../research/families/f07_active_order_continuation/cpp
)

install(TARGETS narrowgate_cpp
    LIBRARY DESTINATION .
)

if(NARROWGATE_BUILD_NATIVE_BENCHMARKS)
    add_executable(
        narrowgate_live_runtime_selftest
        selftests/live_runtime_core_selftest.cpp
        narrowgate_cpp/live_runtime_core.cpp
        narrowgate_cpp/live_policy.cpp
        narrowgate_cpp/quote_core.cpp
    )
    add_executable(
        narrowgate_live_runtime_bench
        bench/live_runtime_core_bench.cpp
        narrowgate_cpp/live_runtime_core.cpp
        narrowgate_cpp/live_policy.cpp
        narrowgate_cpp/quote_core.cpp
    )
    add_executable(
        narrowgate_live_risk_state_selftest
        selftests/live_risk_state_selftest.cpp
        narrowgate_cpp/live_risk_state.cpp
    )
    add_executable(
        narrowgate_live_market_state_selftest
        selftests/live_market_state_selftest.cpp
    )
    add_executable(
        narrowgate_live_order_state_concurrency_selftest
        selftests/live_order_state_concurrency_selftest.cpp
        narrowgate_cpp/live_order_state.cpp
        narrowgate_cpp/order_gateway_core.cpp
    )
    add_executable(
        narrowgate_live_order_action_plan_selftest
        selftests/live_order_action_plan_selftest.cpp
        narrowgate_cpp/live_order_action_plan.cpp
    )
    add_executable(
        narrowgate_live_native_hot_path_bench
        bench/live_native_hot_path_bench.cpp
        narrowgate_cpp/live_order_action_plan.cpp
        narrowgate_cpp/live_runtime_core.cpp
        narrowgate_cpp/live_policy.cpp
        narrowgate_cpp/order_gateway_core.cpp
        narrowgate_cpp/quote_core.cpp
    )
    target_compile_definitions(
        narrowgate_live_order_state_concurrency_selftest
        PRIVATE
            NARROWGATE_LIVE_ORDER_STATE_CORE_ONLY=1
            NARROWGATE_ORDER_GATEWAY_CORE_ONLY=1
    )
    target_link_libraries(
        narrowgate_live_order_state_concurrency_selftest
        PRIVATE Threads::Threads
    )
    target_compile_definitions(
        narrowgate_live_native_hot_path_bench
        PRIVATE
            NARROWGATE_ORDER_GATEWAY_CORE_ONLY=1
            NARROWGATE_LIVE_CPU_PROFILE_NAME="${NARROWGATE_LIVE_CPU_PROFILE}"
    )
    target_link_libraries(
        narrowgate_live_native_hot_path_bench
        PRIVATE Threads::Threads
    )
    foreach(
        target
        narrowgate_live_runtime_selftest
        narrowgate_live_runtime_bench
        narrowgate_live_risk_state_selftest
        narrowgate_live_market_state_selftest
        narrowgate_live_order_state_concurrency_selftest
        narrowgate_live_order_action_plan_selftest
        narrowgate_live_native_hot_path_bench
    )
        target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_compile_features(${target} PRIVATE cxx_std_20)
        target_compile_options(${target} PRIVATE ${NARROWGATE_LIVE_COMPILE_OPTIONS})
        if(NARROWGATE_LIVE_LINK_OPTIONS)
            target_link_options(${target} PRIVATE ${NARROWGATE_LIVE_LINK_OPTIONS})
        endif()
    endforeach()
endif()
