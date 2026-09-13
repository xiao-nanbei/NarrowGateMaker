#include "live_order_state.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <thread>
#include <utility>

#ifndef NARROWGATE_LIVE_ORDER_STATE_CORE_ONLY
#include <pybind11/pybind11.h>

namespace py = pybind11;
#endif

namespace narrowgate_cpp {
namespace {

[[nodiscard]] constexpr bool is_terminal_state(
    NativeLiveOrderState state
) noexcept {
    return state == NativeLiveOrderState::Filled ||
        state == NativeLiveOrderState::Canceled ||
        state == NativeLiveOrderState::Expired ||
        state == NativeLiveOrderState::Rejected;
}

[[nodiscard]] constexpr bool owns_exchange_risk(
    NativeLiveOrderState state
) noexcept {
    return state == NativeLiveOrderState::PendingNew ||
        state == NativeLiveOrderState::Open ||
        state == NativeLiveOrderState::PartiallyFilled ||
        state == NativeLiveOrderState::PendingCancel;
}

[[nodiscard]] constexpr NativeLiveOrderState terminal_state_for(
    NativeExchangeOrderStatus status
) noexcept {
    switch (status) {
        case NativeExchangeOrderStatus::Canceled:
            return NativeLiveOrderState::Canceled;
        case NativeExchangeOrderStatus::Expired:
            return NativeLiveOrderState::Expired;
        case NativeExchangeOrderStatus::Rejected:
            return NativeLiveOrderState::Rejected;
        default:
            std::terminate();
    }
}

[[nodiscard]] constexpr bool is_valid_exchange_status(
    NativeExchangeOrderStatus status
) noexcept {
    switch (status) {
        case NativeExchangeOrderStatus::New:
        case NativeExchangeOrderStatus::PartiallyFilled:
        case NativeExchangeOrderStatus::Filled:
        case NativeExchangeOrderStatus::Canceled:
        case NativeExchangeOrderStatus::Expired:
        case NativeExchangeOrderStatus::Rejected:
            return true;
    }
    return false;
}

void validate_side(CanonicalSide side) {
    if (side != CanonicalSide::Buy && side != CanonicalSide::Sell) {
        throw std::invalid_argument("native live order side must be BUY or SELL");
    }
}

void validate_symbol(std::string_view symbol) {
    if (symbol.empty()) {
        throw std::invalid_argument("native live order symbol cannot be empty");
    }
    const bool canonical = std::all_of(
        symbol.begin(),
        symbol.end(),
        [](unsigned char value) {
            return std::isdigit(value) != 0 ||
                (std::isalpha(value) != 0 && std::toupper(value) == value);
        }
    );
    if (!canonical) {
        throw std::invalid_argument(
            "native live order symbol must be canonical uppercase alphanumeric"
        );
    }
}

void validate_identity_input(
    std::string_view client_order_id,
    std::uint64_t ownership_generation
) {
    if (client_order_id.empty()) {
        throw std::invalid_argument("native live order client ID cannot be empty");
    }
    if (ownership_generation == 0) {
        throw std::invalid_argument(
            "native live ownership generation must be positive"
        );
    }
}

void validate_visibility_time(std::uint64_t visibility_ts_ns) {
    if (visibility_ts_ns == 0) {
        throw std::invalid_argument(
            "native live order visibility timestamp must be positive"
        );
    }
}

}  // namespace

std::string_view native_live_order_transition_reason_text(
    NativeLiveOrderTransitionReason reason
) noexcept {
    using Reason = NativeLiveOrderTransitionReason;
    switch (reason) {
        case Reason::Unspecified:
            return "unspecified";
        case Reason::AdmittedPendingNew:
            return "admitted_pending_new";
        case Reason::DuplicateAdmission:
            return "duplicate_admission";
        case Reason::LateOrDuplicateRestAck:
            return "late_or_duplicate_rest_ack";
        case Reason::ActivateUnknownPrefix:
            return "activate_unknown_prefix";
        case Reason::RestAck:
            return "rest_ack";
        case Reason::DuplicateReject:
            return "duplicate_reject";
        case Reason::Rejected:
            return "rejected";
        case Reason::DuplicateAckUnknown:
            return "duplicate_ack_unknown";
        case Reason::SubmitAckUnknown:
            return "submit_ack_unknown";
        case Reason::CancelAckUnknown:
            return "cancel_ack_unknown";
        case Reason::DuplicateCancelRequest:
            return "duplicate_cancel_request";
        case Reason::CancelRequested:
            return "cancel_requested";
        case Reason::DuplicateCancelReject:
            return "duplicate_cancel_reject";
        case Reason::CancelRejected:
            return "cancel_rejected";
        case Reason::OpenOrderAbsenceUnresolved:
            return "open_order_absence_unresolved";
        case Reason::StaleTerminalCumulativeFill:
            return "stale_terminal_cumulative_fill";
        case Reason::DuplicateTerminalUpdate:
            return "duplicate_terminal_update";
        case Reason::StaleCumulativeFill:
            return "stale_cumulative_fill";
        case Reason::Activate:
            return "activate";
        case Reason::StatusLaggedFullFill:
            return "status_lagged_full_fill";
        case Reason::PartialFill:
            return "partial_fill";
        case Reason::FullFill:
            return "full_fill";
        case Reason::CancelAck:
            return "cancel_ack";
        case Reason::Expired:
            return "expired";
    }
    return "unknown_transition_reason";
}

NativeLiveOrderStateCore::MutationLease
NativeLiveOrderStateCore::acquire_mutation_lease() {
    auto observed = mutation_gate_.load(std::memory_order_acquire);
    for (;;) {
        if ((observed & kMutationGateClosedBit) != 0) {
            wait_for_reconciliation_and_throw();
        }
        if ((observed & kMutationGateLeaseMask) == kMutationGateLeaseMask) {
            throw std::overflow_error(
                "native live order mutation lease count overflow"
            );
        }
        if (mutation_gate_.compare_exchange_weak(
                observed,
                observed + 1,
                std::memory_order_acq_rel,
                std::memory_order_acquire
            )) {
            return MutationLease(*this);
        }
    }
}

void NativeLiveOrderStateCore::release_mutation_lease() noexcept {
    const auto previous = mutation_gate_.fetch_sub(1, std::memory_order_release);
    // A release without a matching lease is a programming error.  Keep this
    // branch allocation-free and fail hard rather than corrupting the gate.
    if ((previous & kMutationGateLeaseMask) == 0) {
        std::terminate();
    }
}

NativeLiveOrderStateCore::MutationLease::~MutationLease() {
    release();
}

void NativeLiveOrderStateCore::MutationLease::release() noexcept {
    if (owner_ == nullptr) {
        return;
    }
    owner_->release_mutation_lease();
    owner_ = nullptr;
}

template <typename Operation>
NativeLiveOrderTransition NativeLiveOrderStateCore::with_mutation_lease(
    Operation&& operation
) {
    auto lease = acquire_mutation_lease();
    try {
        auto result = std::forward<Operation>(operation)();
        lease.release();
        return result;
    } catch (const ReconciliationRequest& request) {
        const auto reason = request.reason();
        const bool publisher = request.publisher();
        lease.release();
        publish_reconciliation_and_throw(reason, publisher);
    }
}

[[noreturn]] void NativeLiveOrderStateCore::wait_for_reconciliation_and_throw()
    const {
    // Closing the lease gate precedes publishing the reason.  This window is
    // bounded by already-running native mutations; waiting here prevents a
    // caller from observing a half-published fatal state.
    while (!reconciliation_required()) {
        std::this_thread::yield();
    }
    throw std::logic_error(
        "native live order state is blocked pending process restart/exact "
        "reconciliation: " + reconciliation_reason()
    );
}

[[noreturn]] void NativeLiveOrderStateCore::latch_and_throw(
    std::string_view reason
) {
    // Close mutation admission before the SideCell lock unwinds.  A snapshot
    // that subsequently acquires this same cell can then wait for the fatal
    // latch publication instead of observing the partial invalid transition
    // during the former unlock->gate-close gap.
    const auto previous = mutation_gate_.fetch_or(
        kMutationGateClosedBit,
        std::memory_order_acq_rel
    );
    throw ReconciliationRequest(
        reason,
        (previous & kMutationGateClosedBit) == 0
    );
}

[[noreturn]] void NativeLiveOrderStateCore::publish_reconciliation_and_throw(
    std::string_view reason,
    bool publisher
) {
    static constexpr std::string_view kFallbackReason =
        "native live order invariant violation";
    if (reason.empty() || reason.size() > kNativeLiveOrderReasonBytes) {
        reason = kFallbackReason;
    }

    if (publisher) {
        // Existing leases are allowed to linearize before the fatal latch.
        // New leases are already excluded by the closed bit.  Publishing the
        // externally-visible latch only after the count reaches zero makes it
        // impossible for either side to commit after that publication.
        while (
            (mutation_gate_.load(std::memory_order_acquire) &
             kMutationGateLeaseMask) != 0
        ) {
            std::this_thread::yield();
        }
        {
            std::lock_guard lock(reconciliation_mutex_);
            if (!reconciliation_reason_.assign(reason)) {
                // publish_reconciliation_and_throw bounds/falls back above;
                // this is therefore an internal contract violation, and the
                // branch itself remains allocation-free.
                std::terminate();
            }
        }
        telemetry_.reconciliation_latch_count.fetch_add(
            1,
            std::memory_order_relaxed
        );
        reconciliation_required_.store(true, std::memory_order_release);
    } else {
        while (!reconciliation_required()) {
            std::this_thread::yield();
        }
    }
    throw std::logic_error(
        "native live order reconciliation required: " + std::string(reason)
    );
}

std::string NativeLiveOrderStateCore::reconciliation_reason() const {
    std::lock_guard lock(reconciliation_mutex_);
    return reconciliation_reason_.size == 0
        ? std::string{}
        : reconciliation_reason_.str();
}

template <CanonicalSide S>
NativeLiveOrderSnapshot NativeLiveOrderStateCore::snapshot_cell(
    const SideCell& current
) noexcept {
    return NativeLiveOrderSnapshot{
        .abi_version = kNativeLiveOrderResultAbiVersion,
        .side = S,
        .state = current.state,
        .ack_unknown_kind = current.ack_unknown_kind,
        .transport_unknown_state = current.transport_unknown_state,
        .client_order_id = current.client_order_id,
        .symbol = current.symbol,
        .ownership_generation = current.ownership_generation,
        .exchange_order_id = current.exchange_order_id,
        .price_ticks = current.price_ticks,
        .quantity_lots = current.quantity_lots,
        .filled_lots = current.filled_lots,
        .submitted_ts_ns = current.submitted_ts_ns,
        .activated_ts_ns = current.activated_ts_ns,
        .cancel_requested_ts_ns = current.cancel_requested_ts_ns,
        .terminal_ts_ns = current.terminal_ts_ns,
        .last_visibility_ts_ns = current.last_visibility_ts_ns,
        .last_exchange_ts_ns = current.last_exchange_ts_ns,
        .activation_unknown_prefix = current.activation_unknown_prefix,
        .ownership_active = owns_exchange_risk(current.state),
        .terminal = is_terminal_state(current.state),
    };
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::make_transition(
    const SideCell& current,
    NativeLiveOrderState previous,
    bool accepted,
    bool idempotent,
    bool stale,
    NativeLiveOrderTransitionReason reason
) const noexcept {
    return NativeLiveOrderTransition{
        .abi_version = kNativeLiveOrderResultAbiVersion,
        .accepted = accepted,
        .idempotent = idempotent,
        .stale = stale,
        .reconciliation_required = reconciliation_required(),
        .previous_state = previous,
        .state = current.state,
        .reason = reason,
        .order = snapshot_cell<S>(current),
    };
}

template <CanonicalSide S>
void NativeLiveOrderStateCore::require_identity(
    const SideCell& current,
    std::string_view client_order_id,
    std::uint64_t ownership_generation
) {
    if (current.state == NativeLiveOrderState::Empty) {
        latch_and_throw("event references empty same-side ownership");
    }
    if (!current.client_order_id.equals(client_order_id)) {
        latch_and_throw("client order ID disagrees with same-side ownership");
    }
    if (current.ownership_generation != ownership_generation) {
        latch_and_throw(
            "ownership generation disagrees with same-side ownership"
        );
    }
}

void NativeLiveOrderStateCore::validate_visibility_progress(
    const SideCell& current,
    std::uint64_t visibility_ts_ns
) {
    validate_visibility_time(visibility_ts_ns);
    if (visibility_ts_ns < current.last_visibility_ts_ns) {
        latch_and_throw("local visibility timestamp regressed");
    }
}

void NativeLiveOrderStateCore::validate_or_bind_exchange_order_id(
    SideCell& current,
    std::uint64_t exchange_order_id
) {
    if (exchange_order_id == 0) {
        latch_and_throw("authoritative exchange update has no positive order ID");
    }
    if (current.exchange_order_id == 0) {
        if (current.state != NativeLiveOrderState::PendingNew) {
            latch_and_throw(
                "only PENDING_NEW may bind its first exchange order ID"
            );
        }
        current.exchange_order_id = exchange_order_id;
        return;
    }
    if (current.exchange_order_id != exchange_order_id) {
        latch_and_throw("exchange order ID changed for active ownership");
    }
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::admit_side(
    std::string_view client_order_id,
    std::string_view symbol,
    std::uint64_t ownership_generation,
    std::int64_t price_ticks,
    std::int64_t quantity_lots,
    std::uint64_t submitted_ts_ns
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_symbol(symbol);
    validate_visibility_time(submitted_ts_ns);
    // Validate every fixed-capacity field before touching the prior side
    // state.  In particular, a valid client ID followed by an oversized
    // symbol must not partially overwrite a terminal cell.
    if (client_order_id.size() > kNativeLiveOrderClientIdBytes) {
        throw std::invalid_argument(
            "client order ID exceeds fixed native capacity"
        );
    }
    if (symbol.size() > kNativeLiveOrderSymbolBytes) {
        throw std::invalid_argument("symbol exceeds fixed native capacity");
    }
    if (price_ticks < 0) {
        throw std::invalid_argument("native live order price ticks must be non-negative");
    }
    if (quantity_lots <= 0) {
        throw std::invalid_argument("native live order quantity lots must be positive");
    }
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    const auto previous = current.state;
    if (owns_exchange_risk(current.state)) {
        if (
            current.client_order_id.equals(client_order_id) &&
            current.ownership_generation == ownership_generation &&
            current.symbol.equals(symbol) &&
            current.price_ticks == price_ticks &&
            current.quantity_lots == quantity_lots &&
            current.submitted_ts_ns == submitted_ts_ns
        ) {
            telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
            return make_transition<S>(
                current,
                previous,
                true,
                true,
                false,
                NativeLiveOrderTransitionReason::DuplicateAdmission
            );
        }
        latch_and_throw("second non-terminal order attempted on the same side");
    }
    if (ownership_generation <= current.last_generation) {
        latch_and_throw("ownership generation did not strictly advance");
    }

    if (!current.client_order_id.assign(client_order_id) ||
        !current.symbol.assign(symbol)) {
        // All capacity/emptiness checks ran before the lock and before state
        // mutation.  Keep this impossible branch allocation-free.
        std::terminate();
    }
    current.state = NativeLiveOrderState::PendingNew;
    current.ack_unknown_kind = NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = TransportUnknownState::None;
    current.activation_unknown_prefix = false;
    current.last_generation = ownership_generation;
    current.ownership_generation = ownership_generation;
    current.exchange_order_id = 0;
    current.price_ticks = price_ticks;
    current.quantity_lots = quantity_lots;
    current.filled_lots = 0;
    current.submitted_ts_ns = submitted_ts_ns;
    current.activated_ts_ns = 0;
    current.cancel_requested_ts_ns = 0;
    current.terminal_ts_ns = 0;
    current.last_visibility_ts_ns = submitted_ts_ns;
    current.last_exchange_ts_ns = 0;
    telemetry_.admitted_count.fetch_add(1, std::memory_order_relaxed);
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        NativeLiveOrderTransitionReason::AdmittedPendingNew
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::confirm_new_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    if (exchange_order_id == 0) {
        throw std::invalid_argument("exchange order ID must be positive");
    }
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    const auto previous = current.state;

    if (current.state != NativeLiveOrderState::PendingNew) {
        if (current.exchange_order_id == exchange_order_id) {
            telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
            return make_transition<S>(
                current,
                previous,
                true,
                true,
                false,
                NativeLiveOrderTransitionReason::LateOrDuplicateRestAck
            );
        }
        latch_and_throw("REST ACK exchange order ID disagrees with ledger");
    }

    validate_or_bind_exchange_order_id(current, exchange_order_id);
    if (current.ack_unknown_kind == NativeOrderAckUnknownKind::Submit) {
        current.activation_unknown_prefix = true;
    }
    current.state = current.filled_lots > 0
        ? NativeLiveOrderState::PartiallyFilled
        : NativeLiveOrderState::Open;
    current.ack_unknown_kind = NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = TransportUnknownState::None;
    current.activated_ts_ns = visibility_ts_ns;
    current.last_visibility_ts_ns = visibility_ts_ns;
    current.last_exchange_ts_ns = exchange_ts_ns;
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        current.activation_unknown_prefix
            ? NativeLiveOrderTransitionReason::ActivateUnknownPrefix
            : NativeLiveOrderTransitionReason::RestAck
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::confirm_rejected_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns,
    bool authoritative_not_accepted
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    const auto previous = current.state;
    if (current.state == NativeLiveOrderState::Rejected) {
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        return make_transition<S>(
            current,
            previous,
            true,
            true,
            false,
            NativeLiveOrderTransitionReason::DuplicateReject
        );
    }
    if (current.state != NativeLiveOrderState::PendingNew) {
        latch_and_throw("submit rejection arrived outside PENDING_NEW");
    }
    if (!authoritative_not_accepted) {
        latch_and_throw(
            "ambiguous transport failure cannot be converted into rejection"
        );
    }

    current.state = NativeLiveOrderState::Rejected;
    current.ack_unknown_kind = NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = TransportUnknownState::ConfirmedNotDispatched;
    current.terminal_ts_ns = visibility_ts_ns;
    current.last_visibility_ts_ns = visibility_ts_ns;
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    telemetry_.terminal_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        NativeLiveOrderTransitionReason::Rejected
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::mark_ack_unknown_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns,
    NativeOrderAckUnknownKind kind
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    const auto expected_state = kind == NativeOrderAckUnknownKind::Submit
        ? NativeLiveOrderState::PendingNew
        : NativeLiveOrderState::PendingCancel;
    const auto previous = current.state;
    if (current.state != expected_state) {
        latch_and_throw(
            kind == NativeOrderAckUnknownKind::Submit
                ? "submit ACK became unknown outside PENDING_NEW"
                : "cancel ACK became unknown outside PENDING_CANCEL"
        );
    }
    if (current.ack_unknown_kind == kind) {
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        return make_transition<S>(
            current,
            previous,
            true,
            true,
            false,
            NativeLiveOrderTransitionReason::DuplicateAckUnknown
        );
    }
    if (current.ack_unknown_kind != NativeOrderAckUnknownKind::None) {
        latch_and_throw("conflicting ACK-unknown phase");
    }

    current.ack_unknown_kind = kind;
    current.transport_unknown_state = TransportUnknownState::AwaitingReconciliation;
    current.last_visibility_ts_ns = visibility_ts_ns;
    if (kind == NativeOrderAckUnknownKind::Submit) {
        telemetry_.submit_unknown_count.fetch_add(1, std::memory_order_relaxed);
    } else {
        telemetry_.cancel_unknown_count.fetch_add(1, std::memory_order_relaxed);
    }
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        kind == NativeOrderAckUnknownKind::Submit
            ? NativeLiveOrderTransitionReason::SubmitAckUnknown
            : NativeLiveOrderTransitionReason::CancelAckUnknown
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::request_cancel_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    const auto previous = current.state;
    if (current.state == NativeLiveOrderState::PendingCancel) {
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        return make_transition<S>(
            current,
            previous,
            true,
            true,
            false,
            NativeLiveOrderTransitionReason::DuplicateCancelRequest
        );
    }
    if (
        current.state != NativeLiveOrderState::Open &&
        current.state != NativeLiveOrderState::PartiallyFilled
    ) {
        latch_and_throw("cancel request requires OPEN or PARTIALLY_FILLED");
    }

    current.state = NativeLiveOrderState::PendingCancel;
    current.ack_unknown_kind = NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = TransportUnknownState::None;
    current.cancel_requested_ts_ns = visibility_ts_ns;
    current.last_visibility_ts_ns = visibility_ts_ns;
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        NativeLiveOrderTransitionReason::CancelRequested
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::cancel_rejected_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    if (exchange_order_id == 0) {
        throw std::invalid_argument("cancel reject must carry exchange order ID");
    }
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    const auto previous = current.state;
    if (
        current.state == NativeLiveOrderState::Open ||
        current.state == NativeLiveOrderState::PartiallyFilled
    ) {
        if (current.exchange_order_id != exchange_order_id) {
            latch_and_throw(
                "duplicate cancel reject exchange order ID disagrees with ledger"
            );
        }
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        return make_transition<S>(
            current,
            previous,
            true,
            true,
            false,
            NativeLiveOrderTransitionReason::DuplicateCancelReject
        );
    }
    if (current.state != NativeLiveOrderState::PendingCancel) {
        latch_and_throw("cancel rejection arrived outside PENDING_CANCEL");
    }
    if (current.exchange_order_id != exchange_order_id) {
        latch_and_throw("cancel rejection exchange order ID disagrees with ledger");
    }

    current.state = current.filled_lots > 0
        ? NativeLiveOrderState::PartiallyFilled
        : NativeLiveOrderState::Open;
    current.ack_unknown_kind = NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = TransportUnknownState::None;
    current.last_visibility_ts_ns = visibility_ts_ns;
    current.last_exchange_ts_ns = exchange_ts_ns;
    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        NativeLiveOrderTransitionReason::CancelRejected
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::reconcile_pending_cancel_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    bool exchange_open,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns
) {
    if (exchange_open) {
        return cancel_rejected_side<S>(
            client_order_id,
            ownership_generation,
            exchange_order_id,
            visibility_ts_ns,
            0
        );
    }

    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    if (current.state != NativeLiveOrderState::PendingCancel) {
        latch_and_throw("pending-cancel reconciliation requires PENDING_CANCEL");
    }
    telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        current.state,
        false,
        true,
        false,
        NativeLiveOrderTransitionReason::OpenOrderAbsenceUnresolved
    );
}

template <CanonicalSide S>
NativeLiveOrderTransition NativeLiveOrderStateCore::apply_exchange_update_side(
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    NativeExchangeOrderStatus status,
    std::int64_t cumulative_filled_lots,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    validate_identity_input(client_order_id, ownership_generation);
    validate_visibility_time(visibility_ts_ns);
    if (exchange_order_id == 0) {
        throw std::invalid_argument(
            "exchange update must carry a positive exchange order ID"
        );
    }
    if (cumulative_filled_lots < 0) {
        throw std::invalid_argument(
            "cumulative filled lots must be non-negative"
        );
    }
    if (!is_valid_exchange_status(status)) {
        throw std::invalid_argument("exchange order status is outside the native ABI");
    }
    auto& current = cell<S>();
    std::lock_guard lock(current.mutex);
    require_identity<S>(current, client_order_id, ownership_generation);
    validate_visibility_progress(current, visibility_ts_ns);
    validate_or_bind_exchange_order_id(current, exchange_order_id);
    const auto previous = current.state;
    if (cumulative_filled_lots > current.quantity_lots) {
        latch_and_throw("cumulative filled lots exceed original quantity");
    }

    if (is_terminal_state(current.state)) {
        if (cumulative_filled_lots > current.filled_lots) {
            current.filled_lots = cumulative_filled_lots;
            current.last_visibility_ts_ns = visibility_ts_ns;
            current.last_exchange_ts_ns = exchange_ts_ns;
            latch_and_throw(
                "positive cumulative fill arrived after exchange terminal"
            );
        }
        const bool stale = cumulative_filled_lots < current.filled_lots;
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        if (stale) {
            telemetry_.stale_count.fetch_add(1, std::memory_order_relaxed);
        }
        return make_transition<S>(
            current,
            previous,
            false,
            true,
            stale,
            stale
                ? NativeLiveOrderTransitionReason::StaleTerminalCumulativeFill
                : NativeLiveOrderTransitionReason::DuplicateTerminalUpdate
        );
    }

    if (cumulative_filled_lots < current.filled_lots) {
        telemetry_.idempotent_count.fetch_add(1, std::memory_order_relaxed);
        telemetry_.stale_count.fetch_add(1, std::memory_order_relaxed);
        return make_transition<S>(
            current,
            previous,
            false,
            true,
            true,
            NativeLiveOrderTransitionReason::StaleCumulativeFill
        );
    }

    const bool submit_unknown =
        current.ack_unknown_kind == NativeOrderAckUnknownKind::Submit;
    const bool cancel_unknown =
        current.ack_unknown_kind == NativeOrderAckUnknownKind::Cancel;
    current.filled_lots = cumulative_filled_lots;
    current.activation_unknown_prefix =
        current.activation_unknown_prefix || submit_unknown;
    current.last_visibility_ts_ns = visibility_ts_ns;
    current.last_exchange_ts_ns = exchange_ts_ns;
    if (
        current.activated_ts_ns == 0 &&
        previous == NativeLiveOrderState::PendingNew &&
        status != NativeExchangeOrderStatus::Rejected
    ) {
        current.activated_ts_ns = visibility_ts_ns;
    }

    auto reason = NativeLiveOrderTransitionReason::Unspecified;
    switch (status) {
        case NativeExchangeOrderStatus::New:
            if (previous == NativeLiveOrderState::PendingCancel) {
                reason = NativeLiveOrderTransitionReason::CancelRejected;
            } else if (
                previous != NativeLiveOrderState::PendingNew &&
                previous != NativeLiveOrderState::Open &&
                previous != NativeLiveOrderState::PartiallyFilled
            ) {
                latch_and_throw("NEW update is illegal from current state");
            } else {
                reason = submit_unknown
                    ? NativeLiveOrderTransitionReason::ActivateUnknownPrefix
                    : NativeLiveOrderTransitionReason::Activate;
            }
            current.state = current.filled_lots > 0
                ? NativeLiveOrderState::PartiallyFilled
                : NativeLiveOrderState::Open;
            break;

        case NativeExchangeOrderStatus::PartiallyFilled:
            if (current.filled_lots == 0) {
                latch_and_throw(
                    "PARTIALLY_FILLED has zero cumulative filled lots"
                );
            }
            if (current.filled_lots == current.quantity_lots) {
                current.state = NativeLiveOrderState::Filled;
                current.terminal_ts_ns = visibility_ts_ns;
                reason = NativeLiveOrderTransitionReason::StatusLaggedFullFill;
                telemetry_.terminal_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                current.state = previous == NativeLiveOrderState::PendingCancel
                    ? NativeLiveOrderState::PendingCancel
                    : NativeLiveOrderState::PartiallyFilled;
                reason = NativeLiveOrderTransitionReason::PartialFill;
            }
            break;

        case NativeExchangeOrderStatus::Filled:
            if (current.filled_lots != current.quantity_lots) {
                current.state = previous == NativeLiveOrderState::PendingCancel
                    ? NativeLiveOrderState::PendingCancel
                    : (
                        current.filled_lots > 0
                            ? NativeLiveOrderState::PartiallyFilled
                            : NativeLiveOrderState::Open
                    );
                latch_and_throw(
                    "exchange FILLED status has incomplete cumulative quantity"
                );
            }
            current.state = NativeLiveOrderState::Filled;
            current.terminal_ts_ns = visibility_ts_ns;
            reason = NativeLiveOrderTransitionReason::FullFill;
            telemetry_.terminal_count.fetch_add(1, std::memory_order_relaxed);
            break;

        case NativeExchangeOrderStatus::Canceled:
        case NativeExchangeOrderStatus::Expired:
        case NativeExchangeOrderStatus::Rejected:
            if (current.filled_lots == current.quantity_lots) {
                current.state = NativeLiveOrderState::Filled;
                reason = NativeLiveOrderTransitionReason::FullFill;
            } else {
                current.state = terminal_state_for(status);
                reason = status == NativeExchangeOrderStatus::Canceled
                    ? NativeLiveOrderTransitionReason::CancelAck
                    : (
                        status == NativeExchangeOrderStatus::Expired
                            ? NativeLiveOrderTransitionReason::Expired
                            : NativeLiveOrderTransitionReason::Rejected
                    );
            }
            current.terminal_ts_ns = visibility_ts_ns;
            telemetry_.terminal_count.fetch_add(1, std::memory_order_relaxed);
            break;
    }

    // A partial fill while cancel delivery is unknown proves only that the
    // order remained matchable at that instant.  It does not prove that the
    // cancel later succeeded or failed, so ownership stays PENDING_CANCEL and
    // the ambiguity must remain latched at the order level.  NEW/cancel-reject
    // or an exchange-terminal update does resolve it.
    const bool preserve_cancel_unknown =
        cancel_unknown &&
        status == NativeExchangeOrderStatus::PartiallyFilled &&
        current.state == NativeLiveOrderState::PendingCancel;
    current.ack_unknown_kind = preserve_cancel_unknown
        ? NativeOrderAckUnknownKind::Cancel
        : NativeOrderAckUnknownKind::None;
    current.transport_unknown_state = preserve_cancel_unknown
        ? TransportUnknownState::AwaitingReconciliation
        : TransportUnknownState::None;

    telemetry_.transition_count.fetch_add(1, std::memory_order_relaxed);
    return make_transition<S>(
        current,
        previous,
        true,
        false,
        false,
        reason
    );
}

#define NARROWGATE_DISPATCH_SIDE(method, ...)                         \
    do {                                                              \
        validate_side(side);                                          \
        if (side == CanonicalSide::Buy) {                             \
            return with_mutation_lease([&]() {                        \
                return method<CanonicalSide::Buy>(__VA_ARGS__);       \
            });                                                       \
        }                                                             \
        return with_mutation_lease([&]() {                            \
            return method<CanonicalSide::Sell>(__VA_ARGS__);          \
        });                                                           \
    } while (false)

NativeLiveOrderTransition NativeLiveOrderStateCore::admit(
    CanonicalSide side,
    std::string_view client_order_id,
    std::string_view symbol,
    std::uint64_t ownership_generation,
    std::int64_t price_ticks,
    std::int64_t quantity_lots,
    std::uint64_t submitted_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        admit_side,
        client_order_id,
        symbol,
        ownership_generation,
        price_ticks,
        quantity_lots,
        submitted_ts_ns
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::confirm_new(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        confirm_new_side,
        client_order_id,
        ownership_generation,
        exchange_order_id,
        visibility_ts_ns,
        exchange_ts_ns
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::confirm_rejected(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns,
    bool authoritative_not_accepted
) {
    NARROWGATE_DISPATCH_SIDE(
        confirm_rejected_side,
        client_order_id,
        ownership_generation,
        visibility_ts_ns,
        authoritative_not_accepted
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::mark_submit_ack_unknown(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns
) {
    validate_side(side);
    return with_mutation_lease([&]() {
        return side == CanonicalSide::Buy
            ? mark_ack_unknown_side<CanonicalSide::Buy>(
                client_order_id,
                ownership_generation,
                visibility_ts_ns,
                NativeOrderAckUnknownKind::Submit
            )
            : mark_ack_unknown_side<CanonicalSide::Sell>(
                client_order_id,
                ownership_generation,
                visibility_ts_ns,
                NativeOrderAckUnknownKind::Submit
            );
    });
}

NativeLiveOrderTransition NativeLiveOrderStateCore::request_cancel(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        request_cancel_side,
        client_order_id,
        ownership_generation,
        visibility_ts_ns
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::mark_cancel_ack_unknown(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t visibility_ts_ns
) {
    validate_side(side);
    return with_mutation_lease([&]() {
        return side == CanonicalSide::Buy
            ? mark_ack_unknown_side<CanonicalSide::Buy>(
                client_order_id,
                ownership_generation,
                visibility_ts_ns,
                NativeOrderAckUnknownKind::Cancel
            )
            : mark_ack_unknown_side<CanonicalSide::Sell>(
                client_order_id,
                ownership_generation,
                visibility_ts_ns,
                NativeOrderAckUnknownKind::Cancel
            );
    });
}

NativeLiveOrderTransition NativeLiveOrderStateCore::cancel_rejected(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        cancel_rejected_side,
        client_order_id,
        ownership_generation,
        exchange_order_id,
        visibility_ts_ns,
        exchange_ts_ns
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::reconcile_pending_cancel(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    bool exchange_open,
    std::uint64_t exchange_order_id,
    std::uint64_t visibility_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        reconcile_pending_cancel_side,
        client_order_id,
        ownership_generation,
        exchange_open,
        exchange_order_id,
        visibility_ts_ns
    );
}

NativeLiveOrderTransition NativeLiveOrderStateCore::apply_exchange_update(
    CanonicalSide side,
    std::string_view client_order_id,
    std::uint64_t ownership_generation,
    std::uint64_t exchange_order_id,
    NativeExchangeOrderStatus status,
    std::int64_t cumulative_filled_lots,
    std::uint64_t visibility_ts_ns,
    std::uint64_t exchange_ts_ns
) {
    NARROWGATE_DISPATCH_SIDE(
        apply_exchange_update_side,
        client_order_id,
        ownership_generation,
        exchange_order_id,
        status,
        cumulative_filled_lots,
        visibility_ts_ns,
        exchange_ts_ns
    );
}

#undef NARROWGATE_DISPATCH_SIDE

template <CanonicalSide S>
NativeLiveOrderSnapshot NativeLiveOrderStateCore::snapshot_side() const {
    const auto& current = cell<S>();
    std::unique_lock lock(current.mutex);
    const auto gate = mutation_gate_.load(std::memory_order_acquire);
    if ((gate & kMutationGateClosedBit) != 0 &&
        !reconciliation_required()) {
        // Do not hold the side mutex while the elected publisher waits for
        // pre-existing mutation leases: one of those leases may need this
        // same cell in order to drain.  After publication the closed gate
        // guarantees no further mutation, so re-locking yields a stable
        // diagnostic snapshot of the fatal state.
        lock.unlock();
        while (!reconciliation_required()) {
            std::this_thread::yield();
        }
        lock.lock();
    }
    return snapshot_cell<S>(current);
}

NativeLiveOrderSnapshot NativeLiveOrderStateCore::snapshot(
    CanonicalSide side
) const {
    validate_side(side);
    return side == CanonicalSide::Buy
        ? snapshot_side<CanonicalSide::Buy>()
        : snapshot_side<CanonicalSide::Sell>();
}

NativeLiveOrderTelemetry NativeLiveOrderStateCore::telemetry() const noexcept {
    return NativeLiveOrderTelemetry{
        .admitted_count = telemetry_.admitted_count.load(std::memory_order_relaxed),
        .transition_count = telemetry_.transition_count.load(
            std::memory_order_relaxed
        ),
        .idempotent_count = telemetry_.idempotent_count.load(
            std::memory_order_relaxed
        ),
        .stale_count = telemetry_.stale_count.load(std::memory_order_relaxed),
        .submit_unknown_count = telemetry_.submit_unknown_count.load(
            std::memory_order_relaxed
        ),
        .cancel_unknown_count = telemetry_.cancel_unknown_count.load(
            std::memory_order_relaxed
        ),
        .terminal_count = telemetry_.terminal_count.load(
            std::memory_order_relaxed
        ),
        .reconciliation_latch_count =
            telemetry_.reconciliation_latch_count.load(
                std::memory_order_relaxed
            ),
    };
}

std::size_t NativeLiveOrderStateCore::side_cell_size_bytes() noexcept {
    return sizeof(SideCell);
}

std::size_t NativeLiveOrderStateCore::side_cell_alignment_bytes() noexcept {
    return alignof(SideCell);
}

std::size_t NativeLiveOrderStateCore::core_size_bytes() noexcept {
    return sizeof(NativeLiveOrderStateCore);
}

std::size_t NativeLiveOrderStateCore::core_alignment_bytes() noexcept {
    return alignof(NativeLiveOrderStateCore);
}

#ifndef NARROWGATE_LIVE_ORDER_STATE_CORE_ONLY
void bind_live_order_state(py::module_& module) {
    py::enum_<NativeLiveOrderState>(module, "NativeLiveOrderState")
        .value("Empty", NativeLiveOrderState::Empty)
        .value("PendingNew", NativeLiveOrderState::PendingNew)
        .value("Open", NativeLiveOrderState::Open)
        .value("PartiallyFilled", NativeLiveOrderState::PartiallyFilled)
        .value("PendingCancel", NativeLiveOrderState::PendingCancel)
        .value("Filled", NativeLiveOrderState::Filled)
        .value("Canceled", NativeLiveOrderState::Canceled)
        .value("Expired", NativeLiveOrderState::Expired)
        .value("Rejected", NativeLiveOrderState::Rejected);

    py::enum_<NativeExchangeOrderStatus>(
        module,
        "NativeExchangeOrderStatus"
    )
        .value("New", NativeExchangeOrderStatus::New)
        .value("PartiallyFilled", NativeExchangeOrderStatus::PartiallyFilled)
        .value("Filled", NativeExchangeOrderStatus::Filled)
        .value("Canceled", NativeExchangeOrderStatus::Canceled)
        .value("Expired", NativeExchangeOrderStatus::Expired)
        .value("Rejected", NativeExchangeOrderStatus::Rejected);

    py::enum_<NativeOrderAckUnknownKind>(
        module,
        "NativeOrderAckUnknownKind"
    )
        .value("None_", NativeOrderAckUnknownKind::None)
        .value("Submit", NativeOrderAckUnknownKind::Submit)
        .value("Cancel", NativeOrderAckUnknownKind::Cancel);

    py::enum_<NativeLiveOrderTransitionReason>(
        module,
        "NativeLiveOrderTransitionReason"
    )
        .value(
            "Unspecified",
            NativeLiveOrderTransitionReason::Unspecified
        )
        .value(
            "AdmittedPendingNew",
            NativeLiveOrderTransitionReason::AdmittedPendingNew
        )
        .value(
            "DuplicateAdmission",
            NativeLiveOrderTransitionReason::DuplicateAdmission
        )
        .value(
            "LateOrDuplicateRestAck",
            NativeLiveOrderTransitionReason::LateOrDuplicateRestAck
        )
        .value(
            "ActivateUnknownPrefix",
            NativeLiveOrderTransitionReason::ActivateUnknownPrefix
        )
        .value("RestAck", NativeLiveOrderTransitionReason::RestAck)
        .value(
            "DuplicateReject",
            NativeLiveOrderTransitionReason::DuplicateReject
        )
        .value("Rejected", NativeLiveOrderTransitionReason::Rejected)
        .value(
            "DuplicateAckUnknown",
            NativeLiveOrderTransitionReason::DuplicateAckUnknown
        )
        .value(
            "SubmitAckUnknown",
            NativeLiveOrderTransitionReason::SubmitAckUnknown
        )
        .value(
            "CancelAckUnknown",
            NativeLiveOrderTransitionReason::CancelAckUnknown
        )
        .value(
            "DuplicateCancelRequest",
            NativeLiveOrderTransitionReason::DuplicateCancelRequest
        )
        .value(
            "CancelRequested",
            NativeLiveOrderTransitionReason::CancelRequested
        )
        .value(
            "DuplicateCancelReject",
            NativeLiveOrderTransitionReason::DuplicateCancelReject
        )
        .value(
            "CancelRejected",
            NativeLiveOrderTransitionReason::CancelRejected
        )
        .value(
            "OpenOrderAbsenceUnresolved",
            NativeLiveOrderTransitionReason::OpenOrderAbsenceUnresolved
        )
        .value(
            "StaleTerminalCumulativeFill",
            NativeLiveOrderTransitionReason::StaleTerminalCumulativeFill
        )
        .value(
            "DuplicateTerminalUpdate",
            NativeLiveOrderTransitionReason::DuplicateTerminalUpdate
        )
        .value(
            "StaleCumulativeFill",
            NativeLiveOrderTransitionReason::StaleCumulativeFill
        )
        .value("Activate", NativeLiveOrderTransitionReason::Activate)
        .value(
            "StatusLaggedFullFill",
            NativeLiveOrderTransitionReason::StatusLaggedFullFill
        )
        .value("PartialFill", NativeLiveOrderTransitionReason::PartialFill)
        .value("FullFill", NativeLiveOrderTransitionReason::FullFill)
        .value("CancelAck", NativeLiveOrderTransitionReason::CancelAck)
        .value("Expired", NativeLiveOrderTransitionReason::Expired);

    py::class_<NativeLiveOrderSnapshot>(module, "NativeLiveOrderSnapshot")
        .def_readonly("abi_version", &NativeLiveOrderSnapshot::abi_version)
        .def_readonly("side", &NativeLiveOrderSnapshot::side)
        .def_readonly("state", &NativeLiveOrderSnapshot::state)
        .def_readonly(
            "ack_unknown_kind",
            &NativeLiveOrderSnapshot::ack_unknown_kind
        )
        .def_readonly(
            "transport_unknown_state",
            &NativeLiveOrderSnapshot::transport_unknown_state
        )
        .def_property_readonly(
            "client_order_id",
            [](const NativeLiveOrderSnapshot& value) {
                const auto text = value.client_order_id.view();
                return py::str(text.data(), text.size());
            }
        )
        .def_property_readonly(
            "symbol",
            [](const NativeLiveOrderSnapshot& value) {
                const auto text = value.symbol.view();
                return py::str(text.data(), text.size());
            }
        )
        .def_readonly(
            "ownership_generation",
            &NativeLiveOrderSnapshot::ownership_generation
        )
        .def_readonly(
            "exchange_order_id",
            &NativeLiveOrderSnapshot::exchange_order_id
        )
        .def_readonly("price_ticks", &NativeLiveOrderSnapshot::price_ticks)
        .def_readonly("quantity_lots", &NativeLiveOrderSnapshot::quantity_lots)
        .def_readonly("filled_lots", &NativeLiveOrderSnapshot::filled_lots)
        .def_readonly(
            "submitted_ts_ns",
            &NativeLiveOrderSnapshot::submitted_ts_ns
        )
        .def_readonly("activated_ts_ns", &NativeLiveOrderSnapshot::activated_ts_ns)
        .def_readonly(
            "cancel_requested_ts_ns",
            &NativeLiveOrderSnapshot::cancel_requested_ts_ns
        )
        .def_readonly("terminal_ts_ns", &NativeLiveOrderSnapshot::terminal_ts_ns)
        .def_readonly(
            "last_visibility_ts_ns",
            &NativeLiveOrderSnapshot::last_visibility_ts_ns
        )
        .def_readonly(
            "last_exchange_ts_ns",
            &NativeLiveOrderSnapshot::last_exchange_ts_ns
        )
        .def_readonly(
            "activation_unknown_prefix",
            &NativeLiveOrderSnapshot::activation_unknown_prefix
        )
        .def_readonly(
            "ownership_active",
            &NativeLiveOrderSnapshot::ownership_active
        )
        .def_readonly("terminal", &NativeLiveOrderSnapshot::terminal);

    py::class_<NativeLiveOrderTransition>(
        module,
        "NativeLiveOrderTransition"
    )
        .def_readonly("abi_version", &NativeLiveOrderTransition::abi_version)
        .def_readonly("accepted", &NativeLiveOrderTransition::accepted)
        .def_readonly("idempotent", &NativeLiveOrderTransition::idempotent)
        .def_readonly("stale", &NativeLiveOrderTransition::stale)
        .def_readonly(
            "reconciliation_required",
            &NativeLiveOrderTransition::reconciliation_required
        )
        .def_readonly(
            "previous_state",
            &NativeLiveOrderTransition::previous_state
        )
        .def_readonly("state", &NativeLiveOrderTransition::state)
        .def_readonly("reason_code", &NativeLiveOrderTransition::reason)
        .def_property_readonly(
            "reason",
            [](const NativeLiveOrderTransition& value) {
                const auto text =
                    native_live_order_transition_reason_text(value.reason);
                return py::str(text.data(), text.size());
            }
        )
        .def_readonly("order", &NativeLiveOrderTransition::order);

    py::class_<NativeLiveOrderTelemetry>(module, "NativeLiveOrderTelemetry")
        .def_readonly("admitted_count", &NativeLiveOrderTelemetry::admitted_count)
        .def_readonly(
            "transition_count",
            &NativeLiveOrderTelemetry::transition_count
        )
        .def_readonly(
            "idempotent_count",
            &NativeLiveOrderTelemetry::idempotent_count
        )
        .def_readonly("stale_count", &NativeLiveOrderTelemetry::stale_count)
        .def_readonly(
            "submit_unknown_count",
            &NativeLiveOrderTelemetry::submit_unknown_count
        )
        .def_readonly(
            "cancel_unknown_count",
            &NativeLiveOrderTelemetry::cancel_unknown_count
        )
        .def_readonly("terminal_count", &NativeLiveOrderTelemetry::terminal_count)
        .def_readonly(
            "reconciliation_latch_count",
            &NativeLiveOrderTelemetry::reconciliation_latch_count
        );

    py::class_<NativeLiveOrderStateCore>(module, "NativeLiveOrderStateCore")
        .def(py::init<>())
        .def(
            "admit",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::string symbol,
                std::uint64_t ownership_generation,
                std::int64_t price_ticks,
                std::int64_t quantity_lots,
                std::uint64_t submitted_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.admit(
                    side,
                    client_order_id,
                    symbol,
                    ownership_generation,
                    price_ticks,
                    quantity_lots,
                    submitted_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("symbol"),
            py::arg("ownership_generation"),
            py::arg("price_ticks"),
            py::arg("quantity_lots"),
            py::arg("submitted_ts_ns")
        )
        .def(
            "confirm_new",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t exchange_order_id,
                std::uint64_t visibility_ts_ns,
                std::uint64_t exchange_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.confirm_new(
                    side,
                    client_order_id,
                    ownership_generation,
                    exchange_order_id,
                    visibility_ts_ns,
                    exchange_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("exchange_order_id"),
            py::arg("visibility_ts_ns"),
            py::arg("exchange_ts_ns") = 0
        )
        .def(
            "confirm_rejected",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t visibility_ts_ns,
                bool authoritative_not_accepted
            ) {
                py::gil_scoped_release release;
                return core.confirm_rejected(
                    side,
                    client_order_id,
                    ownership_generation,
                    visibility_ts_ns,
                    authoritative_not_accepted
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("visibility_ts_ns"),
            py::arg("authoritative_not_accepted")
        )
        .def(
            "mark_submit_ack_unknown",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t visibility_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.mark_submit_ack_unknown(
                    side,
                    client_order_id,
                    ownership_generation,
                    visibility_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("visibility_ts_ns")
        )
        .def(
            "request_cancel",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t visibility_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.request_cancel(
                    side,
                    client_order_id,
                    ownership_generation,
                    visibility_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("visibility_ts_ns")
        )
        .def(
            "mark_cancel_ack_unknown",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t visibility_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.mark_cancel_ack_unknown(
                    side,
                    client_order_id,
                    ownership_generation,
                    visibility_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("visibility_ts_ns")
        )
        .def(
            "cancel_rejected",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t exchange_order_id,
                std::uint64_t visibility_ts_ns,
                std::uint64_t exchange_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.cancel_rejected(
                    side,
                    client_order_id,
                    ownership_generation,
                    exchange_order_id,
                    visibility_ts_ns,
                    exchange_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("exchange_order_id"),
            py::arg("visibility_ts_ns"),
            py::arg("exchange_ts_ns") = 0
        )
        .def(
            "reconcile_pending_cancel",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                bool exchange_open,
                std::uint64_t exchange_order_id,
                std::uint64_t visibility_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.reconcile_pending_cancel(
                    side,
                    client_order_id,
                    ownership_generation,
                    exchange_open,
                    exchange_order_id,
                    visibility_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("exchange_open"),
            py::arg("exchange_order_id"),
            py::arg("visibility_ts_ns")
        )
        .def(
            "apply_exchange_update",
            [](
                NativeLiveOrderStateCore& core,
                CanonicalSide side,
                std::string client_order_id,
                std::uint64_t ownership_generation,
                std::uint64_t exchange_order_id,
                NativeExchangeOrderStatus status,
                std::int64_t cumulative_filled_lots,
                std::uint64_t visibility_ts_ns,
                std::uint64_t exchange_ts_ns
            ) {
                py::gil_scoped_release release;
                return core.apply_exchange_update(
                    side,
                    client_order_id,
                    ownership_generation,
                    exchange_order_id,
                    status,
                    cumulative_filled_lots,
                    visibility_ts_ns,
                    exchange_ts_ns
                );
            },
            py::arg("side"),
            py::arg("client_order_id"),
            py::arg("ownership_generation"),
            py::arg("exchange_order_id"),
            py::arg("status"),
            py::arg("cumulative_filled_lots"),
            py::arg("visibility_ts_ns"),
            py::arg("exchange_ts_ns") = 0
        )
        .def(
            "snapshot",
            [](const NativeLiveOrderStateCore& core, CanonicalSide side) {
                py::gil_scoped_release release;
                return core.snapshot(side);
            },
            py::arg("side")
        )
        .def(
            "telemetry",
            [](const NativeLiveOrderStateCore& core) {
                py::gil_scoped_release release;
                return core.telemetry();
            }
        )
        .def_property_readonly(
            "reconciliation_required",
            [](const NativeLiveOrderStateCore& core) {
                py::gil_scoped_release release;
                return core.reconciliation_required();
            }
        )
        .def_property_readonly(
            "reconciliation_reason",
            [](const NativeLiveOrderStateCore& core) {
                py::gil_scoped_release release;
                return core.reconciliation_reason();
            }
        )
        .def_property_readonly_static(
            "isolation_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::isolation_bytes();
            }
        )
        .def_property_readonly_static(
            "max_client_order_id_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::max_client_order_id_bytes();
            }
        )
        .def_property_readonly_static(
            "side_cell_size_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::side_cell_size_bytes();
            }
        )
        .def_property_readonly_static(
            "side_cell_alignment_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::side_cell_alignment_bytes();
            }
        )
        .def_property_readonly_static(
            "core_size_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::core_size_bytes();
            }
        )
        .def_property_readonly_static(
            "core_alignment_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::core_alignment_bytes();
            }
        )
        .def_property_readonly_static(
            "result_abi_version",
            [](py::object) {
                return NativeLiveOrderStateCore::result_abi_version();
            }
        )
        .def_property_readonly_static(
            "snapshot_result_size_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::snapshot_result_size_bytes();
            }
        )
        .def_property_readonly_static(
            "transition_result_size_bytes",
            [](py::object) {
                return NativeLiveOrderStateCore::transition_result_size_bytes();
            }
        );

    module.attr("NATIVE_LIVE_ORDER_STATE_CORE_AVAILABLE") = py::bool_(true);
    module.attr("NATIVE_LIVE_ORDER_STATE_ISOLATION_BYTES") =
        py::int_(kNativeLiveOrderStateIsolationBytes);
    module.attr("NATIVE_LIVE_ORDER_STATE_RESULT_ABI_VERSION") =
        py::int_(kNativeLiveOrderResultAbiVersion);
}
#endif

}  // namespace narrowgate_cpp
