#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "transport_contract.hpp"

namespace pybind11 {
class module_;
}

namespace narrowgate_cpp {

struct NativeLiveOrderStateCoreTestAccess;

// Isolate independently-mutated BUY/SELL state at the architecture-specific
// destructive-interference boundary (128 B on the Apple-M4 build, 64 B on
// the current amd64 live build). Do not align sequential queue elements this
// way: doing so would inflate the working set.
inline constexpr std::size_t kNativeLiveOrderStateIsolationBytes =
    kDestructiveInterferenceBytes;
inline constexpr std::size_t kNativeLiveOrderClientIdBytes = 64;
inline constexpr std::size_t kNativeLiveOrderSymbolBytes = 32;
inline constexpr std::size_t kNativeLiveOrderReasonBytes = 192;
inline constexpr std::uint16_t kNativeLiveOrderResultAbiVersion = 1;

// A mutation result is part of the authoritative order-state boundary.  It
// must remain publishable after the side cell has committed even when the
// process cannot allocate a diagnostic std::string.  InlineText deliberately
// has no owning dynamic storage; Python/string materialization is a later,
// diagnostic-only operation on an already-published immutable result.
template <std::size_t Capacity>
struct NativeLiveOrderInlineText {
    static_assert(Capacity > 1 && Capacity <= 255);

    std::array<char, Capacity> bytes{};
    std::uint8_t size = 0;

    [[nodiscard]] bool assign(std::string_view value) noexcept {
        if (value.empty() || value.size() > Capacity) {
            return false;
        }
        bytes.fill('\0');
        std::copy(value.begin(), value.end(), bytes.begin());
        size = static_cast<std::uint8_t>(value.size());
        return true;
    }

    void assign_truncated(std::string_view value) noexcept {
        bytes.fill('\0');
        const auto copied = std::min(value.size(), Capacity);
        std::copy_n(value.begin(), copied, bytes.begin());
        size = static_cast<std::uint8_t>(copied);
    }

    [[nodiscard]] bool equals(std::string_view value) const noexcept {
        return value.size() == size &&
            std::equal(value.begin(), value.end(), bytes.begin());
    }

    [[nodiscard]] std::string_view view() const noexcept {
        return std::string_view(bytes.data(), size);
    }

    // This is intentionally diagnostic-only.  Core mutation/snapshot paths
    // copy NativeLiveOrderInlineText and never call str().
    [[nodiscard]] std::string str() const {
        return std::string(view());
    }

    void clear() noexcept {
        bytes.fill('\0');
        size = 0;
    }
};

template <std::size_t Capacity>
[[nodiscard]] bool operator==(
    const NativeLiveOrderInlineText<Capacity>& lhs,
    std::string_view rhs
) noexcept {
    return lhs.equals(rhs);
}

template <std::size_t Capacity>
[[nodiscard]] bool operator==(
    std::string_view lhs,
    const NativeLiveOrderInlineText<Capacity>& rhs
) noexcept {
    return rhs.equals(lhs);
}

static_assert(
    std::is_trivially_copyable_v<
        NativeLiveOrderInlineText<kNativeLiveOrderReasonBytes>
    >
);
static_assert(
    std::is_standard_layout_v<
        NativeLiveOrderInlineText<kNativeLiveOrderReasonBytes>
    >
);

enum class NativeLiveOrderState : std::uint8_t {
    Empty = 0,
    PendingNew = 1,
    Open = 2,
    PartiallyFilled = 3,
    PendingCancel = 4,
    Filled = 5,
    Canceled = 6,
    Expired = 7,
    Rejected = 8,
};

enum class NativeExchangeOrderStatus : std::uint8_t {
    New = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Canceled = 4,
    Expired = 5,
    Rejected = 6,
};

enum class NativeOrderAckUnknownKind : std::uint8_t {
    None = 0,
    Submit = 1,
    Cancel = 2,
};

// Stable, allocation-free transition identity.  Human-readable diagnostics
// are derived from this code only after the result has crossed the mutation
// boundary.
enum class NativeLiveOrderTransitionReason : std::uint8_t {
    Unspecified = 0,
    AdmittedPendingNew = 1,
    DuplicateAdmission = 2,
    LateOrDuplicateRestAck = 3,
    ActivateUnknownPrefix = 4,
    RestAck = 5,
    DuplicateReject = 6,
    Rejected = 7,
    DuplicateAckUnknown = 8,
    SubmitAckUnknown = 9,
    CancelAckUnknown = 10,
    DuplicateCancelRequest = 11,
    CancelRequested = 12,
    DuplicateCancelReject = 13,
    CancelRejected = 14,
    OpenOrderAbsenceUnresolved = 15,
    StaleTerminalCumulativeFill = 16,
    DuplicateTerminalUpdate = 17,
    StaleCumulativeFill = 18,
    Activate = 19,
    StatusLaggedFullFill = 20,
    PartialFill = 21,
    FullFill = 22,
    CancelAck = 23,
    Expired = 24,
};

[[nodiscard]] std::string_view native_live_order_transition_reason_text(
    NativeLiveOrderTransitionReason reason
) noexcept;

struct NativeLiveOrderSnapshot {
    std::uint16_t abi_version = kNativeLiveOrderResultAbiVersion;
    CanonicalSide side = CanonicalSide::Unspecified;
    NativeLiveOrderState state = NativeLiveOrderState::Empty;
    NativeOrderAckUnknownKind ack_unknown_kind = NativeOrderAckUnknownKind::None;
    TransportUnknownState transport_unknown_state = TransportUnknownState::None;
    NativeLiveOrderInlineText<kNativeLiveOrderClientIdBytes> client_order_id;
    NativeLiveOrderInlineText<kNativeLiveOrderSymbolBytes> symbol;
    std::uint64_t ownership_generation = 0;
    std::uint64_t exchange_order_id = 0;
    std::int64_t price_ticks = 0;
    std::int64_t quantity_lots = 0;
    std::int64_t filled_lots = 0;
    std::uint64_t submitted_ts_ns = 0;
    std::uint64_t activated_ts_ns = 0;
    std::uint64_t cancel_requested_ts_ns = 0;
    std::uint64_t terminal_ts_ns = 0;
    std::uint64_t last_visibility_ts_ns = 0;
    std::uint64_t last_exchange_ts_ns = 0;
    bool activation_unknown_prefix = false;
    bool ownership_active = false;
    bool terminal = false;
};

struct NativeLiveOrderTransition {
    std::uint16_t abi_version = kNativeLiveOrderResultAbiVersion;
    bool accepted = false;
    bool idempotent = false;
    bool stale = false;
    bool reconciliation_required = false;
    NativeLiveOrderState previous_state = NativeLiveOrderState::Empty;
    NativeLiveOrderState state = NativeLiveOrderState::Empty;
    NativeLiveOrderTransitionReason reason =
        NativeLiveOrderTransitionReason::Unspecified;
    NativeLiveOrderSnapshot order;
};

static_assert(std::is_trivially_copyable_v<NativeLiveOrderSnapshot>);
static_assert(std::is_standard_layout_v<NativeLiveOrderSnapshot>);
static_assert(std::is_trivially_copyable_v<NativeLiveOrderTransition>);
static_assert(std::is_standard_layout_v<NativeLiveOrderTransition>);

struct NativeLiveOrderTelemetry {
    std::uint64_t admitted_count = 0;
    std::uint64_t transition_count = 0;
    std::uint64_t idempotent_count = 0;
    std::uint64_t stale_count = 0;
    std::uint64_t submit_unknown_count = 0;
    std::uint64_t cancel_unknown_count = 0;
    std::uint64_t terminal_count = 0;
    std::uint64_t reconciliation_latch_count = 0;
};

// Bounded same-side ownership state for the live USD-M path.
//
// Exact scope:
//   PENDING_NEW -> OPEN/PARTIALLY_FILLED -> PENDING_CANCEL -> terminal
// plus submit/cancel ACK-unknown preservation and affirmative reconciliation.
// One non-terminal order per side is an invariant.  Rich fill economics,
// trade-ID de-duplication, callback ordering, tombstones and orphan adoption
// deliberately remain outside this bounded slice.
class NativeLiveOrderStateCore {
public:
    NativeLiveOrderStateCore() = default;
    NativeLiveOrderStateCore(const NativeLiveOrderStateCore&) = delete;
    NativeLiveOrderStateCore& operator=(const NativeLiveOrderStateCore&) = delete;

    [[nodiscard]] NativeLiveOrderTransition admit(
        CanonicalSide side,
        std::string_view client_order_id,
        std::string_view symbol,
        std::uint64_t ownership_generation,
        std::int64_t price_ticks,
        std::int64_t quantity_lots,
        std::uint64_t submitted_ts_ns
    );

    [[nodiscard]] NativeLiveOrderTransition confirm_new(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns = 0
    );

    [[nodiscard]] NativeLiveOrderTransition confirm_rejected(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns,
        bool authoritative_not_accepted
    );

    [[nodiscard]] NativeLiveOrderTransition mark_submit_ack_unknown(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns
    );

    [[nodiscard]] NativeLiveOrderTransition request_cancel(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns
    );

    [[nodiscard]] NativeLiveOrderTransition mark_cancel_ack_unknown(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns
    );

    [[nodiscard]] NativeLiveOrderTransition cancel_rejected(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns = 0
    );

    [[nodiscard]] NativeLiveOrderTransition reconcile_pending_cancel(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        bool exchange_open,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns
    );

    [[nodiscard]] NativeLiveOrderTransition apply_exchange_update(
        CanonicalSide side,
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        NativeExchangeOrderStatus status,
        std::int64_t cumulative_filled_lots,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns = 0
    );

    [[nodiscard]] NativeLiveOrderSnapshot snapshot(CanonicalSide side) const;
    [[nodiscard]] NativeLiveOrderTelemetry telemetry() const noexcept;

    [[nodiscard]] bool reconciliation_required() const noexcept {
        return reconciliation_required_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::string reconciliation_reason() const;

    static constexpr std::size_t isolation_bytes() noexcept {
        return kNativeLiveOrderStateIsolationBytes;
    }
    static constexpr std::size_t max_client_order_id_bytes() noexcept {
        return kNativeLiveOrderClientIdBytes;
    }
    static std::size_t side_cell_size_bytes() noexcept;
    static std::size_t side_cell_alignment_bytes() noexcept;
    static std::size_t core_size_bytes() noexcept;
    static std::size_t core_alignment_bytes() noexcept;
    static constexpr std::uint16_t result_abi_version() noexcept {
        return kNativeLiveOrderResultAbiVersion;
    }
    static constexpr std::size_t snapshot_result_size_bytes() noexcept {
        return sizeof(NativeLiveOrderSnapshot);
    }
    static constexpr std::size_t transition_result_size_bytes() noexcept {
        return sizeof(NativeLiveOrderTransition);
    }

private:
    // The high bit closes admission for new mutations.  The low bits count
    // mutations that already hold a lease.  A reconciliation fault first
    // closes the gate, waits for those leases to drain, and only then
    // publishes reconciliation_required_.  Consequently every successful
    // mutation is linearized before the published reconciliation latch; no
    // side can commit after callers observe that latch.
    static constexpr std::uint64_t kMutationGateClosedBit =
        std::uint64_t{1} << 63;
    static constexpr std::uint64_t kMutationGateLeaseMask =
        ~kMutationGateClosedBit;

    class MutationLease {
    public:
        explicit MutationLease(NativeLiveOrderStateCore& owner) noexcept
            : owner_(&owner) {}
        MutationLease(const MutationLease&) = delete;
        MutationLease& operator=(const MutationLease&) = delete;
        MutationLease(MutationLease&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)) {}
        MutationLease& operator=(MutationLease&&) = delete;
        ~MutationLease();

        void release() noexcept;

    private:
        NativeLiveOrderStateCore* owner_;
    };

    class ReconciliationRequest {
    public:
        explicit ReconciliationRequest(
            std::string_view reason,
            bool publisher
        ) noexcept : reason_(reason), publisher_(publisher) {}

        [[nodiscard]] std::string_view reason() const noexcept {
            return reason_;
        }
        [[nodiscard]] bool publisher() const noexcept { return publisher_; }

    private:
        std::string_view reason_;
        bool publisher_ = false;
    };

    struct alignas(kNativeLiveOrderStateIsolationBytes) SideCell {
        mutable std::mutex mutex;
        NativeLiveOrderInlineText<kNativeLiveOrderClientIdBytes> client_order_id;
        NativeLiveOrderInlineText<kNativeLiveOrderSymbolBytes> symbol;
        NativeLiveOrderState state = NativeLiveOrderState::Empty;
        NativeOrderAckUnknownKind ack_unknown_kind = NativeOrderAckUnknownKind::None;
        TransportUnknownState transport_unknown_state = TransportUnknownState::None;
        bool activation_unknown_prefix = false;
        std::uint64_t last_generation = 0;
        std::uint64_t ownership_generation = 0;
        std::uint64_t exchange_order_id = 0;
        std::int64_t price_ticks = 0;
        std::int64_t quantity_lots = 0;
        std::int64_t filled_lots = 0;
        std::uint64_t submitted_ts_ns = 0;
        std::uint64_t activated_ts_ns = 0;
        std::uint64_t cancel_requested_ts_ns = 0;
        std::uint64_t terminal_ts_ns = 0;
        std::uint64_t last_visibility_ts_ns = 0;
        std::uint64_t last_exchange_ts_ns = 0;
    };

    static_assert(alignof(SideCell) == kNativeLiveOrderStateIsolationBytes);
    static_assert(sizeof(SideCell) % kNativeLiveOrderStateIsolationBytes == 0);

    struct alignas(kNativeLiveOrderStateIsolationBytes) AtomicTelemetry {
        std::atomic<std::uint64_t> admitted_count{0};
        std::atomic<std::uint64_t> transition_count{0};
        std::atomic<std::uint64_t> idempotent_count{0};
        std::atomic<std::uint64_t> stale_count{0};
        std::atomic<std::uint64_t> submit_unknown_count{0};
        std::atomic<std::uint64_t> cancel_unknown_count{0};
        std::atomic<std::uint64_t> terminal_count{0};
        std::atomic<std::uint64_t> reconciliation_latch_count{0};
    };

    template <CanonicalSide S>
    [[nodiscard]] SideCell& cell() noexcept {
        if constexpr (S == CanonicalSide::Buy) {
            return buy_;
        } else {
            return sell_;
        }
    }

    template <CanonicalSide S>
    [[nodiscard]] const SideCell& cell() const noexcept {
        if constexpr (S == CanonicalSide::Buy) {
            return buy_;
        } else {
            return sell_;
        }
    }

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition admit_side(
        std::string_view client_order_id,
        std::string_view symbol,
        std::uint64_t ownership_generation,
        std::int64_t price_ticks,
        std::int64_t quantity_lots,
        std::uint64_t submitted_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition confirm_new_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition confirm_rejected_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns,
        bool authoritative_not_accepted
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition mark_ack_unknown_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns,
        NativeOrderAckUnknownKind kind
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition request_cancel_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t visibility_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition cancel_rejected_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition reconcile_pending_cancel_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        bool exchange_open,
        std::uint64_t exchange_order_id,
        std::uint64_t visibility_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition apply_exchange_update_side(
        std::string_view client_order_id,
        std::uint64_t ownership_generation,
        std::uint64_t exchange_order_id,
        NativeExchangeOrderStatus status,
        std::int64_t cumulative_filled_lots,
        std::uint64_t visibility_ts_ns,
        std::uint64_t exchange_ts_ns
    );

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderSnapshot snapshot_side() const;

    template <CanonicalSide S>
    [[nodiscard]] static NativeLiveOrderSnapshot snapshot_cell(
        const SideCell& cell
    ) noexcept;

    template <CanonicalSide S>
    [[nodiscard]] NativeLiveOrderTransition make_transition(
        const SideCell& cell,
        NativeLiveOrderState previous_state,
        bool accepted,
        bool idempotent,
        bool stale,
        NativeLiveOrderTransitionReason reason
    ) const noexcept;

    [[nodiscard]] MutationLease acquire_mutation_lease();
    void release_mutation_lease() noexcept;

    template <typename Operation>
    [[nodiscard]] NativeLiveOrderTransition with_mutation_lease(
        Operation&& operation
    );

    // Internal validators close the shared gate while the affected SideCell
    // is still locked, then throw this bounded allocation-free signal.  The
    // outer mutation lease is unwound before the global latch is published,
    // avoiding lock upgrades and cross-side deadlocks without exposing a
    // partially committed fatal transition to snapshot readers.
    [[noreturn]] void latch_and_throw(std::string_view reason);
    [[noreturn]] void publish_reconciliation_and_throw(
        std::string_view reason,
        bool publisher
    );
    [[noreturn]] void wait_for_reconciliation_and_throw() const;

    template <CanonicalSide S>
    void require_identity(
        const SideCell& cell,
        std::string_view client_order_id,
        std::uint64_t ownership_generation
    );

    void validate_visibility_progress(
        const SideCell& cell,
        std::uint64_t visibility_ts_ns
    );
    void validate_or_bind_exchange_order_id(
        SideCell& cell,
        std::uint64_t exchange_order_id
    );

    SideCell buy_;
    SideCell sell_;
    AtomicTelemetry telemetry_;
    alignas(kNativeLiveOrderStateIsolationBytes)
        std::atomic<std::uint64_t> mutation_gate_{0};
    std::atomic<bool> reconciliation_required_{false};
    mutable std::mutex reconciliation_mutex_;
    NativeLiveOrderInlineText<kNativeLiveOrderReasonBytes>
        reconciliation_reason_;

    friend struct NativeLiveOrderStateCoreTestAccess;
};

void bind_live_order_state(pybind11::module_& module);

}  // namespace narrowgate_cpp
