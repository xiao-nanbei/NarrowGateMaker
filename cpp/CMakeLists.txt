cmake_minimum_required(VERSION 3.18)
project(narrowgate_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
    narrowgate_cpp/quote_core.cpp
    ../research/families/f06_placement_fill_cif/cpp/request_state_features.cpp
    ../research/families/f06_placement_fill_cif/cpp/risk_set_expansion.cpp
    ../research/families/f06_placement_fill_cif/cpp/sparse_order_lifecycle.cpp
    ../research/families/f07_active_order_continuation/cpp/active_order_competing_risk_cif.cpp
    ../research/families/f07_active_order_continuation/cpp/order_lifecycle_journal_v2_mirror.cpp
    ../research/families/f03_causal_13_head/cpp/causal_v12_1s_features.cpp
    narrowgate_cpp/tick_replay.cpp
    narrowgate_cpp/streaming_features.cpp
)

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
