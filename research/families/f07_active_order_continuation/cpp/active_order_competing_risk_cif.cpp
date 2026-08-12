#include "active_order_competing_risk_cif.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace narrowgate_cpp {
namespace {

constexpr double kMassTolerance = 64.0 * std::numeric_limits<double>::epsilon();

double precise_sum(const double* values, const std::size_t size) {
    std::vector<double> partials;
    partials.reserve(size);
    for (std::size_t value_index = 0; value_index < size; ++value_index) {
        double value = values[value_index];
        std::size_t retained = 0;
        for (const double partial_value : partials) {
            double partial = partial_value;
            if (std::abs(value) < std::abs(partial)) {
                std::swap(value, partial);
            }
            const double high = value + partial;
            const double low = partial - (high - value);
            if (low != 0.0) {
                partials[retained++] = low;
            }
            value = high;
        }
        partials.resize(retained);
        partials.push_back(value);
    }
    double total = 0.0;
    for (const double partial : partials) {
        total += partial;
    }
    return total;
}

double precise_sum(const std::array<double, kActiveOrderCifCauseCount>& values) {
    return precise_sum(values.data(), values.size());
}

double exact_complement(
    const std::array<double, kActiveOrderCifCauseCount>& probabilities
) {
    const double total = precise_sum(probabilities);
    if (total < 0.0 || total > 1.0 + kMassTolerance) {
        throw std::runtime_error("cumulative incidence leaves unit interval");
    }
    double complement = std::max(0.0, 1.0 - total);
    std::array<double, kActiveOrderCifCauseCount + 1> mass_values{};
    std::copy(probabilities.begin(), probabilities.end(), mass_values.begin() + 1);
    for (int attempt = 0; attempt < 3; ++attempt) {
        mass_values[0] = complement;
        const double mass = precise_sum(mass_values.data(), mass_values.size());
        if (mass == 1.0) {
            return complement;
        }
        complement += 1.0 - mass;
    }
    throw std::runtime_error("unable to represent exact CIF probability mass");
}

void validate_probability_state(
    const double survival,
    const std::array<double, kActiveOrderCifCauseCount>& cif
) {
    if (!std::isfinite(survival) || survival < 0.0 || survival > 1.0) {
        throw std::invalid_argument("initial_survival must be finite and within [0, 1]");
    }
    for (const double value : cif) {
        if (!std::isfinite(value) || value < 0.0 || value > 1.0) {
            throw std::invalid_argument("initial_cif must be finite and within [0, 1]");
        }
    }
    std::array<double, kActiveOrderCifCauseCount + 1> mass_values{};
    mass_values[0] = survival;
    std::copy(cif.begin(), cif.end(), mass_values.begin() + 1);
    if (precise_sum(mass_values.data(), mass_values.size()) != 1.0) {
        throw std::invalid_argument("initial survival and CIF must conserve probability mass");
    }
}

std::pair<std::array<double, kActiveOrderCifCauseCount>, double>
jointly_normalized_hazards(
    const std::array<double, kActiveOrderCifCauseCount>& rates
) {
    std::size_t residual_index = 0;
    bool any_positive = false;
    for (std::size_t index = 0; index < rates.size(); ++index) {
        const double rate = rates[index];
        if (!std::isfinite(rate) || rate < 0.0) {
            throw std::invalid_argument("rates_per_s must be finite and non-negative");
        }
        if (rate > 0.0) {
            residual_index = index;
            any_positive = true;
        }
    }
    std::array<double, kActiveOrderCifCauseCount> hazards{};
    const double total = precise_sum(rates);
    if (!std::isfinite(total)) {
        throw std::invalid_argument("sum of rates_per_s must be finite");
    }
    if (!any_positive || total == 0.0) {
        return {hazards, 1.0};
    }

    const double event_probability = -std::expm1(-kActiveOrderCifGridIntervalSeconds * total);
    for (std::size_t index = 0; index < rates.size(); ++index) {
        hazards[index] = rates[index] / total * event_probability;
    }
    std::array<double, kActiveOrderCifCauseCount - 1> other_hazards{};
    std::size_t other_index = 0;
    for (std::size_t index = 0; index < hazards.size(); ++index) {
        if (index != residual_index) {
            other_hazards[other_index++] = hazards[index];
        }
    }
    hazards[residual_index] = event_probability - precise_sum(
        other_hazards.data(), other_hazards.size()
    );
    if (hazards[residual_index] < 0.0 && std::abs(hazards[residual_index]) <= kMassTolerance) {
        hazards[residual_index] = 0.0;
    }
    for (const double probability : hazards) {
        if (!std::isfinite(probability) || probability < 0.0) {
            throw std::runtime_error("joint hazard normalization produced an invalid probability");
        }
    }
    const double no_event_probability = exact_complement(hazards);
    if (!std::isfinite(no_event_probability) || no_event_probability < 0.0 ||
        no_event_probability > 1.0) {
        throw std::runtime_error("joint no-event probability leaves unit interval");
    }
    return {hazards, no_event_probability};
}

}  // namespace

ActiveOrderCifBatchResult update_active_order_competing_risk_cif(
    const std::vector<std::int64_t>& edges,
    const std::vector<double>& rates_per_second,
    const std::int64_t initial_last_edge,
    const double initial_survival,
    const std::array<double, kActiveOrderCifCauseCount>& initial_cif
) {
    if (initial_last_edge < 0) {
        throw std::invalid_argument("initial_last_edge must be non-negative");
    }
    if (rates_per_second.size() != edges.size() * kActiveOrderCifCauseCount) {
        throw std::invalid_argument("rates_per_s must have shape (n_edges, 4)");
    }
    validate_probability_state(initial_survival, initial_cif);

    ActiveOrderCifBatchResult result;
    result.edges = edges;
    result.hazards.reserve(rates_per_second.size());
    result.no_event_probability.reserve(edges.size());
    result.survival_before.reserve(edges.size());
    result.survival_after.reserve(edges.size());
    result.cif_before.reserve(rates_per_second.size());
    result.cif_after.reserve(rates_per_second.size());

    std::int64_t last_edge = initial_last_edge;
    double survival = initial_survival;
    auto cif = initial_cif;
    for (std::size_t row = 0; row < edges.size(); ++row) {
        const std::int64_t edge = edges[row];
        const std::int64_t expected = last_edge + 1;
        if (edge != expected) {
            const std::string kind = edge > expected ? "missed" : "duplicate or non-monotone";
            throw std::invalid_argument(
                kind + " 100ms grid edge: expected " + std::to_string(expected) +
                ", got " + std::to_string(edge)
            );
        }

        std::array<double, kActiveOrderCifCauseCount> rates{};
        std::copy_n(
            rates_per_second.begin() + static_cast<std::ptrdiff_t>(row * rates.size()),
            rates.size(),
            rates.begin()
        );
        const auto [hazards, no_event] = jointly_normalized_hazards(rates);

        result.survival_before.push_back(survival);
        result.cif_before.insert(result.cif_before.end(), cif.begin(), cif.end());
        result.hazards.insert(result.hazards.end(), hazards.begin(), hazards.end());
        result.no_event_probability.push_back(no_event);

        auto next_cif = cif;
        for (std::size_t cause = 0; cause < next_cif.size(); ++cause) {
            next_cif[cause] += survival * hazards[cause];
        }
        const double next_survival = exact_complement(next_cif);
        if (next_survival < 0.0 || next_survival > survival + kMassTolerance) {
            throw std::runtime_error("survival update leaves its monotone probability support");
        }
        const double product_survival = survival * no_event;
        if (std::abs(next_survival - product_survival) >
            std::max(kMassTolerance, 1e-14 * std::abs(product_survival))) {
            throw std::runtime_error("survival update disagrees with joint no-event probability");
        }
        for (std::size_t cause = 0; cause < next_cif.size(); ++cause) {
            if (next_cif[cause] + kMassTolerance < cif[cause]) {
                throw std::runtime_error("cumulative incidence must be monotone non-decreasing");
            }
        }

        survival = next_survival;
        cif = next_cif;
        last_edge = edge;
        result.survival_after.push_back(survival);
        result.cif_after.insert(result.cif_after.end(), cif.begin(), cif.end());
    }

    result.final_last_edge = last_edge;
    result.final_survival = survival;
    result.final_cif = cif;
    return result;
}

}  // namespace narrowgate_cpp
