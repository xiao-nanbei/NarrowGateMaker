#include "order_lifecycle_journal_v2_mirror.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>
#include <unordered_set>

namespace narrowgate_cpp {
namespace {

bool is_fill_risk_phase(const std::string& phase) {
    return phase == "ACTIVE" || phase == "PARTIALLY_FILLED" ||
        phase == "CANCEL_PENDING";
}

bool quantities_equal(const double left, const double right) {
    return std::abs(left - right) <=
        std::max(1e-15, 1e-12 * std::max(std::abs(left), std::abs(right)));
}

void require_finite_nonnegative(const double value, const char* label) {
    if (!std::isfinite(value) || value < 0.0) {
        throw std::invalid_argument(
            std::string(label) + " must be finite and non-negative"
        );
    }
}

void require_nonempty(const std::string& value, const char* label) {
    if (value.empty()) {
        throw std::invalid_argument(std::string(label) + " is required");
    }
}

bool is_local_shutdown_reason(const std::string& reason) {
    return reason == "administrative_cancel" ||
        reason == "local_shutdown_cancel" || reason == "shutdown";
}

bool is_exchange_terminal_reason(const std::string& reason) {
    return reason == "cancel_ack" || reason == "cancel_ack_reconciled" ||
        reason == "expired" || reason == "filled_before_cancel_ack" ||
        reason == "full_fill" || reason == "rejected";
}

std::string expected_phase_after(const OrderLifecycleJournalV2MirrorInput& row) {
    const auto& event = row.lifecycle_event;
    const auto& before = row.phase_before;
    if (event == "submit") {
        if (before != "SUBMITTED") {
            throw std::invalid_argument("submit must start in SUBMITTED");
        }
        return "SUBMITTED";
    }
    if (event == "activate") {
        if (before != "SUBMITTED" && before != "ACTIVE" &&
            before != "CANCEL_PENDING") {
            throw std::invalid_argument("activate has unsupported source phase");
        }
        return "ACTIVE";
    }
    if (event == "cancel_request") {
        if (before != "ACTIVE" && before != "PARTIALLY_FILLED") {
            throw std::invalid_argument("cancel_request requires an active risk phase");
        }
        return "CANCEL_PENDING";
    }
    if (event == "cancel_rejected") {
        if (before != "CANCEL_PENDING") {
            throw std::invalid_argument("cancel_rejected requires CANCEL_PENDING");
        }
        return row.remaining_quantity_after <
                row.initial_quantity - kPartialFillProgressAbsToleranceBtc
            ? "PARTIALLY_FILLED"
            : "ACTIVE";
    }
    if (event == "partial_fill") {
        if (before == "CANCEL_PENDING") {
            return "CANCEL_PENDING";
        }
        if (before != "ACTIVE" && before != "PARTIALLY_FILLED") {
            throw std::invalid_argument("partial_fill requires an active risk phase");
        }
        return "PARTIALLY_FILLED";
    }
    if (event == "full_fill") {
        if (!is_fill_risk_phase(before)) {
            throw std::invalid_argument("full_fill requires an active risk phase");
        }
        return "EXCHANGE_TERMINAL";
    }
    if (event == "exchange_terminal") {
        if (before != "SUBMITTED" && !is_fill_risk_phase(before)) {
            throw std::invalid_argument("exchange_terminal has unsupported source phase");
        }
        return "EXCHANGE_TERMINAL";
    }
    if (event == "local_shutdown_censor") {
        if (before != "SUBMITTED" && !is_fill_risk_phase(before)) {
            throw std::invalid_argument("local_shutdown_censor has unsupported source phase");
        }
        return before;
    }
    if (event == "post_cancel_recovery") {
        if (before != "EXCHANGE_TERMINAL") {
            throw std::invalid_argument("post_cancel_recovery requires EXCHANGE_TERMINAL");
        }
        return "POST_CANCEL_RECOVERY";
    }
    if (event == "reentry_eligible") {
        if (before != "POST_CANCEL_RECOVERY") {
            throw std::invalid_argument("reentry_eligible requires POST_CANCEL_RECOVERY");
        }
        return "REENTRY_ELIGIBLE";
    }
    throw std::invalid_argument("unsupported journal-v2 lifecycle event: " + event);
}

void validate_reason(const OrderLifecycleJournalV2MirrorInput& row) {
    const auto& event = row.lifecycle_event;
    const auto& reason = row.event_reason;
    if (event == "exchange_terminal") {
        if (!is_exchange_terminal_reason(reason)) {
            throw std::invalid_argument("unsupported exchange-terminal reason");
        }
        return;
    }
    if (event == "local_shutdown_censor") {
        if (!is_local_shutdown_reason(reason)) {
            throw std::invalid_argument("unsupported local-shutdown censor reason");
        }
        return;
    }
    if (event == "post_cancel_recovery") {
        if (reason != "old_order_risk_set_ended") {
            throw std::invalid_argument("post-cancel recovery reason mismatch");
        }
        return;
    }
    if (event == "reentry_eligible") {
        if (reason != "prospective_placement_state_supported") {
            throw std::invalid_argument("reentry eligibility reason mismatch");
        }
        return;
    }
    if (!reason.empty()) {
        throw std::invalid_argument("non-terminal lifecycle event has a reason");
    }
}

std::string terminal_policy_route(const OrderLifecycleJournalV2MirrorInput& row) {
    if (row.lifecycle_event == "local_shutdown_censor") {
        return "SHUTDOWN_NO_REENTRY";
    }
    if (row.lifecycle_event == "full_fill") {
        return "TERMINAL_COMPLETE";
    }
    if (row.lifecycle_event != "exchange_terminal") {
        return "NONE";
    }
    const auto& reason = row.event_reason;
    if (row.remaining_quantity_after <= kTerminalRemainderAbsToleranceBtc) {
        return "TERMINAL_COMPLETE";
    }
    if (reason == "cancel_ack" || reason == "cancel_ack_reconciled") {
        return "PROSPECTIVE_CANCEL_REENTRY";
    }
    if (reason == "expired" || reason == "rejected") {
        return "BASELINE_RESUBMIT";
    }
    throw std::invalid_argument(
        "fill terminal reason cannot retain positive remaining quantity"
    );
}

void validate_terminal_projection(
    const OrderLifecycleJournalV2MirrorInput& row,
    const std::string& route
) {
    std::string expected_observation = "NONE";
    std::string expected_exchange_reason;
    std::string expected_censor_reason;
    if (row.lifecycle_event == "full_fill") {
        expected_observation = "EXCHANGE_TERMINAL";
        expected_exchange_reason = "full_fill";
    } else if (row.lifecycle_event == "exchange_terminal") {
        expected_observation = "EXCHANGE_TERMINAL";
        expected_exchange_reason = row.event_reason;
    } else if (row.lifecycle_event == "local_shutdown_censor") {
        expected_observation = "LOCAL_SHUTDOWN_CENSOR";
        expected_censor_reason = row.event_reason;
    }
    if (row.terminal_observation != expected_observation ||
        row.exchange_terminal_reason != expected_exchange_reason ||
        row.local_censor_reason != expected_censor_reason) {
        throw std::invalid_argument("journal terminal projection mismatch");
    }
    if ((route == "TERMINAL_COMPLETE" &&
         (row.lifecycle_event == "full_fill" ||
          row.event_reason == "full_fill" ||
          row.event_reason == "filled_before_cancel_ack")) &&
        row.remaining_quantity_after != 0.0) {
        throw std::invalid_argument(
            "full-fill terminal row must persist exact zero remaining quantity"
        );
    }
}

}  // namespace

OrderLifecycleJournalV2MirrorResult mirror_order_lifecycle_journal_v2_event_stream(
    const std::vector<OrderLifecycleJournalV2MirrorInput>& events
) {
    if (events.empty()) {
        throw std::invalid_argument("journal-v2 event stream is empty");
    }

    OrderLifecycleJournalV2MirrorResult result;
    result.rows.reserve(events.size());
    const auto lifecycle_id = events.front().lifecycle_id;
    const auto client_order_id = events.front().client_order_id;
    require_nonempty(lifecycle_id, "lifecycle id");
    require_nonempty(client_order_id, "client order id");

    std::string previous_phase_after;
    double previous_remaining_after = 0.0;
    std::int64_t previous_visibility_ts_ns = 0;
    std::int64_t previous_exchange_ts_ns = 0;
    bool local_censor_seen = false;
    std::unordered_set<std::string> event_ids;
    event_ids.reserve(events.size());

    for (std::size_t index = 0; index < events.size(); ++index) {
        const auto& row = events[index];
        const auto expected_sequence = static_cast<std::int64_t>(index + 1);
        require_nonempty(row.event_id, "event id");
        if (!event_ids.insert(row.event_id).second) {
            throw std::invalid_argument("duplicate journal-v2 event id");
        }
        if (row.lifecycle_id != lifecycle_id || row.client_order_id != client_order_id) {
            throw std::invalid_argument("event stream mixes lifecycle identities");
        }
        if (row.lifecycle_sequence != expected_sequence) {
            throw std::invalid_argument("lifecycle sequence must be contiguous from one");
        }
        if (index == 0 && row.lifecycle_event != "submit") {
            throw std::invalid_argument("first lifecycle event must be submit");
        }
        if (local_censor_seen) {
            throw std::invalid_argument("event observed after local shutdown censor");
        }
        if (!previous_phase_after.empty() && row.phase_before != previous_phase_after) {
            throw std::invalid_argument("lifecycle phase chain is discontinuous");
        }
        if (row.event_visibility_ts_ns <= 0 ||
            row.event_visibility_ts_ns < previous_visibility_ts_ns) {
            throw std::invalid_argument("lifecycle visibility timestamp regressed");
        }
        if (row.event_exchange_ts_ns.has_value()) {
            const auto exchange_ts = row.event_exchange_ts_ns.value();
            if (exchange_ts <= 0 || exchange_ts > row.event_visibility_ts_ns ||
                exchange_ts < previous_exchange_ts_ns) {
                throw std::invalid_argument("lifecycle exchange timestamp is invalid");
            }
            previous_exchange_ts_ns = exchange_ts;
        }

        require_finite_nonnegative(row.initial_quantity, "initial quantity");
        require_nonempty(row.simulator_queue_source, "simulator queue source");
        if (row.exact_queue_path_valid &&
            row.simulator_queue_source != "native_exchange_book") {
            throw std::invalid_argument(
                "exact queue path requires native exchange-book source"
            );
        }
        require_finite_nonnegative(
            row.remaining_quantity_before, "remaining quantity before"
        );
        require_finite_nonnegative(
            row.remaining_quantity_after, "remaining quantity after"
        );
        if (index == 0 &&
            !quantities_equal(row.initial_quantity, row.remaining_quantity_before)) {
            throw std::invalid_argument("submit remaining quantity differs from initial quantity");
        }
        if (index > 0 &&
            !quantities_equal(row.remaining_quantity_before, previous_remaining_after)) {
            throw std::invalid_argument("remaining quantity chain is discontinuous");
        }
        if (row.remaining_quantity_after >
            row.remaining_quantity_before + kQuantityIncreaseAbsToleranceBtc) {
            throw std::invalid_argument("remaining quantity increased within event");
        }
        const bool is_fill = row.lifecycle_event == "partial_fill" ||
            row.lifecycle_event == "full_fill";
        if (!is_fill &&
            !quantities_equal(row.remaining_quantity_before, row.remaining_quantity_after)) {
            throw std::invalid_argument("non-fill event changed remaining quantity");
        }
        if (row.lifecycle_event == "partial_fill") {
            if (!(row.remaining_quantity_after > kTerminalRemainderAbsToleranceBtc &&
                  row.remaining_quantity_after <
                      row.remaining_quantity_before -
                          kPartialFillProgressAbsToleranceBtc)) {
                throw std::invalid_argument(
                    "partial fill must retain positive quantity and make progress"
                );
            }
        }
        if (row.lifecycle_event == "full_fill" &&
            row.remaining_quantity_after != 0.0) {
            throw std::invalid_argument(
                "full-fill terminal row must persist exact zero remaining quantity"
            );
        }
        if (row.lifecycle_event == "exchange_terminal" &&
            (row.event_reason == "full_fill" ||
             row.event_reason == "filled_before_cancel_ack") &&
            row.remaining_quantity_after != 0.0) {
            throw std::invalid_argument(
                "fill terminal reason must persist exact zero remaining quantity"
            );
        }

        validate_reason(row);
        const auto expected_phase = expected_phase_after(row);
        if (row.phase_after != expected_phase) {
            throw std::invalid_argument(
                "journal phase differs from independently derived phase"
            );
        }
        const bool expected_risk = is_fill_risk_phase(expected_phase) &&
            row.remaining_quantity_after > kTerminalRemainderAbsToleranceBtc;
        if (row.lifecycle_event == "local_shutdown_censor") {
            if (row.fill_risk_active_after.has_value()) {
                throw std::invalid_argument(
                    "local shutdown censor must report unknown physical fill risk"
                );
            }
        } else if (!row.fill_risk_active_after.has_value() ||
                   row.fill_risk_active_after.value() != expected_risk) {
            throw std::invalid_argument("journal fill-risk projection mismatch");
        }

        const auto route = terminal_policy_route(row);
        validate_terminal_projection(row, route);
        if (row.lifecycle_event == "cancel_rejected") {
            if (expected_phase == "PARTIALLY_FILLED") {
                ++result.cancel_reject_partially_filled_count;
            } else {
                ++result.cancel_reject_active_count;
            }
        }
        if (row.lifecycle_event == "full_fill" ||
            row.lifecycle_event == "exchange_terminal") {
            ++result.exchange_terminal_count;
        }
        if (row.lifecycle_event == "local_shutdown_censor") {
            ++result.local_shutdown_censor_count;
            local_censor_seen = true;
        }

        result.rows.push_back(OrderLifecycleJournalV2MirrorRow{
            .event_id = row.event_id,
            .lifecycle_id = row.lifecycle_id,
            .client_order_id = row.client_order_id,
            .lifecycle_sequence = row.lifecycle_sequence,
            .lifecycle_event = row.lifecycle_event,
            .event_visibility_ts_ns = row.event_visibility_ts_ns,
            .event_exchange_ts_ns = row.event_exchange_ts_ns,
            .phase_before = row.phase_before,
            .phase_after = expected_phase,
            .event_reason = row.event_reason,
            .terminal_observation = row.terminal_observation,
            .exchange_terminal_reason = row.exchange_terminal_reason,
            .local_censor_reason = row.local_censor_reason,
            .terminal_policy_route = route,
            .initial_quantity = row.initial_quantity,
            .remaining_quantity_before = row.remaining_quantity_before,
            .remaining_quantity_after = row.remaining_quantity_after,
            .fill_risk_active_after = row.fill_risk_active_after,
            .simulator_queue_source = row.simulator_queue_source,
            .exact_queue_path_valid = row.exact_queue_path_valid,
        });

        previous_phase_after = expected_phase;
        previous_remaining_after = row.remaining_quantity_after;
        previous_visibility_ts_ns = row.event_visibility_ts_ns;
    }
    return result;
}

}  // namespace narrowgate_cpp
