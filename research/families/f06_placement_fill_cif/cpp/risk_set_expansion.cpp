#include "risk_set_expansion.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace narrowgate_cpp {

RiskSetExpansionResult expand_competing_risk_intervals(
    ArrayView<double> duration_ms,
    ArrayView<std::uint8_t> event_kind,
    ArrayView<double> bin_edges_ms
) {
    if (duration_ms.size() != event_kind.size()) {
        throw std::invalid_argument("duration_ms and event_kind must align");
    }
    if (bin_edges_ms.size() < 2 || bin_edges_ms.front() != 0.0) {
        throw std::invalid_argument("bin_edges_ms must start at zero and contain two edges");
    }
    if (!std::is_sorted(bin_edges_ms.begin(), bin_edges_ms.end())) {
        throw std::invalid_argument("bin_edges_ms must be sorted ascending");
    }
    for (std::size_t i = 1; i < bin_edges_ms.size(); ++i) {
        if (!(bin_edges_ms[i] > bin_edges_ms[i - 1])) {
            throw std::invalid_argument("bin_edges_ms must be strictly increasing");
        }
    }

    RiskSetExpansionResult out;
    const std::size_t bins = bin_edges_ms.size() - 1;
    const double support_end = bin_edges_ms.back();
    for (std::size_t row = 0; row < duration_ms.size(); ++row) {
        const double raw_duration = duration_ms[row];
        const std::uint8_t kind = event_kind[row];
        if (!std::isfinite(raw_duration) || raw_duration < 0.0 || kind > 2) {
            throw std::invalid_argument("risk-set duration/event kind is invalid");
        }
        const double observed_duration = std::min(raw_duration, support_end);
        const bool event_observed = kind != 0 && raw_duration <= support_end;
        std::size_t event_bin = bins;
        if (event_observed) {
            const auto position = std::lower_bound(
                bin_edges_ms.begin() + 1,
                bin_edges_ms.end(),
                raw_duration
            );
            event_bin = static_cast<std::size_t>(position - bin_edges_ms.begin() - 1);
        }
        for (std::size_t bin = 0; bin < bins; ++bin) {
            const double start = bin_edges_ms[bin];
            const double end = bin_edges_ms[bin + 1];
            const double exposure = std::max(
                0.0, std::min(observed_duration, end) - start
            );
            const bool is_event_bin = event_observed && bin == event_bin;
            if (!(exposure > 0.0) && !is_event_bin) {
                break;
            }
            out.row_index.push_back(static_cast<std::int64_t>(row));
            out.bin_index.push_back(static_cast<std::int64_t>(bin));
            out.interval_start_ms.push_back(start);
            out.interval_end_ms.push_back(end);
            out.exposure_fraction.push_back(
                std::clamp(exposure / (end - start), 0.0, 1.0)
            );
            out.fill_target.push_back(
                static_cast<std::uint8_t>(is_event_bin && kind == 1)
            );
            out.ack_target.push_back(
                static_cast<std::uint8_t>(is_event_bin && kind == 2)
            );
            if (is_event_bin || observed_duration <= end) {
                break;
            }
        }
    }
    return out;
}

}  // namespace narrowgate_cpp
