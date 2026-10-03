#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::string_view kOrderLifecycleJournalV2MirrorAbi =
    "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2";
inline constexpr std::string_view kOrderLifecycleJournalV2SchemaVersion =
    "order_lifecycle_journal.v2";
inline constexpr std::string_view kOrderLifecycleQuantityContractId =
    "order_lifecycle_terminal_remainder_zero_abs_1e-12.v1";
inline constexpr double kTerminalRemainderAbsToleranceBtc = 1e-12;
inline constexpr double kQuantityIncreaseAbsToleranceBtc = 1e-10;
inline constexpr double kPartialFillProgressAbsToleranceBtc = 1e-12;

inline constexpr std::array<std::string_view, 43> kOrderLifecycleJournalV2Columns = {
    "schema_version",
    "event_id",
    "lifecycle_id",
    "runtime_source",
    "source_callback_id",
    "source_callback_type",
    "source_callback_event_ordinal",
    "source_callback_event_count",
    "source_callback_received_ts_ns",
    "source_callback_exchange_ts_ns",
    "source_callback_exchange_clock_valid",
    "client_order_id",
    "exchange_order_id",
    "symbol",
    "side",
    "lifecycle_sequence",
    "lifecycle_event",
    "event_visibility_ts_ns",
    "event_exchange_ts_ns",
    "event_exchange_clock_valid",
    "phase_before",
    "phase_after",
    "event_reason",
    "observation_origin",
    "left_truncated",
    "left_truncation_reason",
    "terminal_observation",
    "exchange_terminal_reason",
    "local_censor_reason",
    "initial_quantity",
    "remaining_quantity_before",
    "remaining_quantity_after",
    "fill_risk_active_after",
    "simulator_queue_source",
    "exact_queue_path_valid",
    "quantity_time_exposure_visible_btc_s",
    "visible_exposure_valid",
    "visible_exposure_complete",
    "visible_exposure_invalid_reason",
    "quantity_time_exposure_exchange_btc_s",
    "exchange_exposure_valid",
    "exchange_exposure_complete",
    "exchange_exposure_invalid_reason",
};

inline constexpr std::array<std::string_view, 41>
    kHistoricalOrderLifecycleJournalV2Columns = {
        "schema_version",
        "event_id",
        "lifecycle_id",
        "runtime_source",
        "source_callback_id",
        "source_callback_type",
        "source_callback_event_ordinal",
        "source_callback_event_count",
        "source_callback_received_ts_ns",
        "source_callback_exchange_ts_ns",
        "source_callback_exchange_clock_valid",
        "client_order_id",
        "exchange_order_id",
        "symbol",
        "side",
        "lifecycle_sequence",
        "lifecycle_event",
        "event_visibility_ts_ns",
        "event_exchange_ts_ns",
        "event_exchange_clock_valid",
        "phase_before",
        "phase_after",
        "event_reason",
        "observation_origin",
        "left_truncated",
        "left_truncation_reason",
        "terminal_observation",
        "exchange_terminal_reason",
        "local_censor_reason",
        "initial_quantity",
        "remaining_quantity_before",
        "remaining_quantity_after",
        "fill_risk_active_after",
        "quantity_time_exposure_visible_btc_s",
        "visible_exposure_valid",
        "visible_exposure_complete",
        "visible_exposure_invalid_reason",
        "quantity_time_exposure_exchange_btc_s",
        "exchange_exposure_valid",
        "exchange_exposure_complete",
        "exchange_exposure_invalid_reason",
};

struct OrderLifecycleJournalV2MirrorInput {
    std::string event_id;
    std::string lifecycle_id;
    std::string client_order_id;
    std::int64_t lifecycle_sequence = 0;
    std::string lifecycle_event;
    std::int64_t event_visibility_ts_ns = 0;
    std::optional<std::int64_t> event_exchange_ts_ns;
    std::string phase_before;
    std::string phase_after;
    std::string event_reason;
    std::string terminal_observation;
    std::string exchange_terminal_reason;
    std::string local_censor_reason;
    double initial_quantity = 0.0;
    double remaining_quantity_before = 0.0;
    double remaining_quantity_after = 0.0;
    std::optional<bool> fill_risk_active_after;
    std::string simulator_queue_source;
    bool exact_queue_path_valid = false;
};

struct OrderLifecycleJournalV2MirrorRow {
    std::string event_id;
    std::string lifecycle_id;
    std::string client_order_id;
    std::int64_t lifecycle_sequence = 0;
    std::string lifecycle_event;
    std::int64_t event_visibility_ts_ns = 0;
    std::optional<std::int64_t> event_exchange_ts_ns;
    std::string phase_before;
    std::string phase_after;
    std::string event_reason;
    std::string terminal_observation;
    std::string exchange_terminal_reason;
    std::string local_censor_reason;
    std::string terminal_policy_route;
    double initial_quantity = 0.0;
    double remaining_quantity_before = 0.0;
    double remaining_quantity_after = 0.0;
    std::optional<bool> fill_risk_active_after;
    std::string simulator_queue_source;
    bool exact_queue_path_valid = false;
};

struct OrderLifecycleJournalV2MirrorResult {
    std::vector<OrderLifecycleJournalV2MirrorRow> rows;
    std::int64_t cancel_reject_active_count = 0;
    std::int64_t cancel_reject_partially_filled_count = 0;
    std::int64_t exchange_terminal_count = 0;
    std::int64_t local_shutdown_censor_count = 0;
};

OrderLifecycleJournalV2MirrorResult mirror_order_lifecycle_journal_v2_event_stream(
    const std::vector<OrderLifecycleJournalV2MirrorInput>& events
);

}  // namespace narrowgate_cpp
