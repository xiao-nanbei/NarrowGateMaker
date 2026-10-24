#pragma once

#include "common.hpp"

#include <cstdint>
#include <vector>

namespace narrowgate_cpp {

struct RiskSetExpansionResult {
    std::vector<std::int64_t> row_index;
    std::vector<std::int64_t> bin_index;
    std::vector<double> interval_start_ms;
    std::vector<double> interval_end_ms;
    std::vector<double> exposure_fraction;
    std::vector<std::uint8_t> fill_target;
    std::vector<std::uint8_t> ack_target;
};

[[nodiscard]] RiskSetExpansionResult expand_competing_risk_intervals(
    ArrayView<double> duration_ms,
    ArrayView<std::uint8_t> event_kind,
    ArrayView<double> bin_edges_ms
);

}  // namespace narrowgate_cpp
