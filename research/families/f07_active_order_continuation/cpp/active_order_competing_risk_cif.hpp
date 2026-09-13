#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::size_t kActiveOrderCifCauseCount = 4;
inline constexpr double kActiveOrderCifGridIntervalSeconds = 0.1;

struct ActiveOrderCifBatchResult {
    std::vector<std::int64_t> edges;
    std::vector<double> hazards;
    std::vector<double> no_event_probability;
    std::vector<double> survival_before;
    std::vector<double> survival_after;
    std::vector<double> cif_before;
    std::vector<double> cif_after;
    std::int64_t final_last_edge = 0;
    double final_survival = 1.0;
    std::array<double, kActiveOrderCifCauseCount> final_cif{};
};

ActiveOrderCifBatchResult update_active_order_competing_risk_cif(
    const std::vector<std::int64_t>& edges,
    const std::vector<double>& rates_per_second,
    std::int64_t initial_last_edge,
    double initial_survival,
    const std::array<double, kActiveOrderCifCauseCount>& initial_cif
);

}  // namespace narrowgate_cpp
