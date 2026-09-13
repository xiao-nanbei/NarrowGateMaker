#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common.hpp"
#include "transport_contract.hpp"

namespace pybind11 {
class module_;
}

namespace narrowgate_cpp {

struct NativeUsdMOrderGatewayCoreTestAccess;

inline constexpr std::size_t kNativeGatewayCacheLineBytes =
    kDestructiveInterferenceBytes;
inline constexpr std::size_t kNativeGatewayQueueCapacity = 256;
// PLACE traffic can consume at most this much of the shared ring.  The tail
// remains available to risk-reducing CANCEL/CANCEL_ALL work even when a burst
// of quote decisions outruns the wire worker.
inline constexpr std::size_t kNativeGatewaySafetyReserve = 16;

// Operation-specific entry points call a compile-time specialization.  The
// wire worker therefore receives one compact tagged request without running
// Python or allocating strings on the producer/consumer hot path.
enum class NativeGatewayOperation : std::uint8_t {
    Place = 1,
    Cancel = 2,
    CancelAll = 3,
};

template <std::size_t Capacity>
struct InlineText {
    static_assert(Capacity > 1);

    std::array<char, Capacity> bytes{};
    std::uint16_t size = 0;

    [[nodiscard]] bool assign(std::string_view value) noexcept {
        if (value.size() >= Capacity) {
            return false;
        }
        size = static_cast<std::uint16_t>(value.size());
        for (std::size_t index = 0; index < value.size(); ++index) {
            bytes[index] = value[index];
        }
        bytes[value.size()] = '\0';
        return true;
    }

    // Copies the largest representable prefix and reports whether truncation
    // occurred. Runtime diagnostics must never throw before publishing a
    // safety/reconciliation state transition merely because an upstream
    // TLS/HTTP error string is long.
    [[nodiscard]] bool assign_truncated(std::string_view value) noexcept {
        const auto copied = std::min(value.size(), Capacity - 1);
        size = static_cast<std::uint16_t>(copied);
        for (std::size_t index = 0; index < copied; ++index) {
            bytes[index] = value[index];
        }
        bytes[copied] = '\0';
        return copied != value.size();
    }

    [[nodiscard]] std::string str() const {
        return std::string(bytes.data(), size);
    }
};

// Inline strings make a queue push/pop a bounded copy. Queue cursors, rather
// than every slot, are cache-line isolated: per-slot alignment would inflate
// this fixed array without preventing producer/consumer payload sharing.
struct NativeGatewayWireRequest {
    NativeGatewayOperation operation = NativeGatewayOperation::Place;
    CanonicalSide side = CanonicalSide::Unspecified;
    CanonicalOrderType order_type = CanonicalOrderType::Unspecified;
    CanonicalTimeInForce time_in_force = CanonicalTimeInForce::Unspecified;
    bool reduce_only = false;
    bool post_only = false;
    bool safety_fence = false;
    double price = 0.0;
    double quantity = 0.0;
    std::uint64_t exchange_order_id = 0;
    std::uint64_t recv_window_ms = 0;
    std::uint64_t deadline_time_ns = 0;
    std::uint64_t expected_ownership_generation = 0;
    std::uint64_t admission_epoch = 0;
    std::uint64_t decision_time_ns = 0;
    std::uint64_t enqueue_time_ns = 0;
    std::uint64_t place_epoch = 0;
    InlineText<48> request_id;
    InlineText<64> decision_id;
    InlineText<48> client_order_id;
    InlineText<24> symbol;
    InlineText<64> reason;
};

static_assert(std::is_trivially_copyable_v<NativeGatewayWireRequest>);

struct NativeGatewayRequestSnapshot {
    NativeGatewayOperation operation = NativeGatewayOperation::Place;
    InlineText<48> request_id;
    InlineText<64> decision_id;
    InlineText<48> client_order_id;
    InlineText<24> symbol;
    InlineText<64> reason;
    CanonicalSide side = CanonicalSide::Unspecified;
    CanonicalOrderType order_type = CanonicalOrderType::Unspecified;
    CanonicalTimeInForce time_in_force = CanonicalTimeInForce::Unspecified;
    bool reduce_only = false;
    bool post_only = false;
    bool safety_fence = false;
    double price = 0.0;
    double quantity = 0.0;
    std::uint64_t exchange_order_id = 0;
    std::uint64_t recv_window_ms = 0;
    std::uint64_t deadline_time_ns = 0;
    std::uint64_t expected_ownership_generation = 0;
    std::uint64_t decision_time_ns = 0;
    std::uint64_t enqueue_time_ns = 0;
    std::uint64_t dequeue_time_ns = 0;
    std::uint64_t generation = 0;
};

struct NativeGatewayReceiptSnapshot {
    TransportBackendKind backend = TransportBackendKind::Unspecified;
    NativeGatewayOperation operation = NativeGatewayOperation::Place;
    TransportPhase phase = TransportPhase::Unspecified;
    TransportUnknownState unknown_state = TransportUnknownState::None;
    bool admitted = false;
    bool reconciliation_required = false;
    bool safety_barrier_latched = false;
    bool send_attempted = false;
    bool retry_permitted = false;
    bool safety_escalation_required = false;
    bool reason_truncated = false;
    InlineText<48> request_id;
    InlineText<64> decision_id;
    InlineText<48> client_order_id;
    InlineText<128> reason;
    std::uint64_t generation = 0;
    // decision..completion are one local monotonic clock domain.  The venue
    // timestamp is retained independently and is never numerically compared
    // with a local timestamp.
    std::uint64_t decision_time_ns = 0;
    std::uint64_t enqueue_time_ns = 0;
    std::uint64_t dequeue_time_ns = 0;
    std::uint64_t dispatch_time_ns = 0;
    std::uint64_t wire_time_ns = 0;
    std::uint64_t response_time_ns = 0;
    std::uint64_t completion_time_ns = 0;
    std::uint64_t exchange_time_ns = 0;

    [[nodiscard]] bool allows_cross_backend_retry() const noexcept {
        return retry_permitted && !reconciliation_required &&
               !safety_barrier_latched &&
               unknown_state == TransportUnknownState::ConfirmedNotDispatched &&
               static_cast<std::uint8_t>(phase) <
                   static_cast<std::uint8_t>(TransportPhase::WireDispatched);
    }
};

struct NativeGatewayDequeueResult {
    std::optional<NativeGatewayRequestSnapshot> request;
    std::vector<NativeGatewayReceiptSnapshot> invalidations;
};

static_assert(std::is_trivially_copyable_v<NativeGatewayRequestSnapshot>);
static_assert(std::is_trivially_copyable_v<NativeGatewayReceiptSnapshot>);

template <typename Value, std::size_t Capacity>
class SpscRing {
    static_assert(Capacity >= 2);
    static_assert((Capacity & (Capacity - 1)) == 0);

public:
    [[nodiscard]] bool try_push(
        const Value& value,
        std::size_t reserved_slots = 0
    ) noexcept {
        if (reserved_slots >= Capacity) {
            return false;
        }
        const auto write = producer_.value.load(std::memory_order_relaxed);
        const auto read = consumer_.value.load(std::memory_order_acquire);
        if (write - read >= Capacity - reserved_slots) {
            return false;
        }
        slots_[write & (Capacity - 1)] = value;
        producer_.value.store(write + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool try_pop(Value& value) noexcept {
        const auto read = consumer_.value.load(std::memory_order_relaxed);
        const auto write = producer_.value.load(std::memory_order_acquire);
        if (read == write) {
            return false;
        }
        value = slots_[read & (Capacity - 1)];
        consumer_.value.store(read + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool try_peek(Value& value) const noexcept {
        const auto read = consumer_.value.load(std::memory_order_relaxed);
        const auto write = producer_.value.load(std::memory_order_acquire);
        if (read == write) {
            return false;
        }
        value = slots_[read & (Capacity - 1)];
        return true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        const auto read = consumer_.value.load(std::memory_order_acquire);
        const auto write = producer_.value.load(std::memory_order_acquire);
        // Loading producer before consumer permits consumer to advance past
        // the sampled producer value and underflow this subtraction.  The
        // read-first order above makes write >= read for the SPSC contract;
        // retain the guard for counter wrap and diagnostic callers.
        return write >= read ? static_cast<std::size_t>(write - read) : 0;
    }

private:
    struct alignas(kNativeGatewayCacheLineBytes) Cursor {
        std::atomic<std::uint64_t> value{0};
    };

    // Producer and consumer mutate different cache lines.  Requests remain in
    // a fixed array so neither thread performs a heap allocation in steady
    // state.
    Cursor producer_;
    Cursor consumer_;
    std::array<Value, Capacity> slots_{};
};

// Producer calls may originate from multiple native policy/risk callbacks and
// are serialized immediately around ring publication.  Consumer calls are a
// single wire-worker state machine: overlapping consumer calls are rejected
// at runtime. A handoff between consumer threads is valid only after the prior
// call returns; no two consumers may access the ring/active request together.
class NativeUsdMOrderGatewayCore {
public:
    explicit NativeUsdMOrderGatewayCore(TransportBackendKind backend);

    NativeUsdMOrderGatewayCore(const NativeUsdMOrderGatewayCore&) = delete;
    NativeUsdMOrderGatewayCore& operator=(const NativeUsdMOrderGatewayCore&) = delete;

    [[nodiscard]] NativeGatewayReceiptSnapshot enqueue_order(
        const CanonicalOrderIntent& intent,
        std::uint64_t decision_time_ns,
        std::uint64_t enqueue_time_ns
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot enqueue_cancel(
        const CanonicalCancelIntent& intent,
        std::uint64_t decision_time_ns,
        std::uint64_t enqueue_time_ns,
        bool safety_fence = false
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot enqueue_cancel_all(
        const CanonicalCancelAllIntent& intent,
        std::uint64_t decision_time_ns,
        std::uint64_t enqueue_time_ns
    );

    [[nodiscard]] NativeGatewayDequeueResult begin_next(
        std::uint64_t dequeue_time_ns,
        std::uint64_t generation,
        std::uint64_t current_ownership_generation
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot mark_send_attempted(
        std::uint64_t dispatch_time_ns
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot mark_wire_dispatched(
        std::uint64_t dispatch_time_ns,
        std::uint64_t wire_time_ns
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot mark_exchange_ack(
        bool accepted,
        std::uint64_t response_time_ns,
        std::uint64_t completion_time_ns,
        std::uint64_t exchange_time_ns,
        const std::string& reason
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot mark_transport_unknown(
        std::uint64_t completion_time_ns,
        const std::string& reason
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot mark_confirmed_not_dispatched(
        std::uint64_t completion_time_ns,
        const std::string& reason
    );

    void mark_reconciled(
        std::uint64_t generation,
        std::uint64_t expected_reconciliation_epoch
    );
    void release_safety_barrier(
        std::uint64_t generation,
        bool safety_action_resolved = false
    );

    [[nodiscard]] TransportBackendKind backend() const noexcept {
        return backend_;
    }
    [[nodiscard]] std::size_t pending_count() const noexcept {
        return queue_.size();
    }
    [[nodiscard]] bool has_active_request() const noexcept {
        return has_active_request_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool reconciliation_required() const noexcept {
        return reconciled_epoch_.load(std::memory_order_acquire) <
               reconciliation_epoch_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t reconciled_generation() const noexcept {
        return reconciled_generation_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t reconciliation_epoch() const noexcept {
        return reconciliation_epoch_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool safety_barrier_latched() const noexcept {
        return safety_barrier_latched_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t safety_release_generation() const noexcept {
        return safety_release_generation_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool safety_escalation_pending() const noexcept {
        return safety_escalation_pending_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t enqueued_count() const noexcept;
    [[nodiscard]] std::uint64_t dequeued_count() const noexcept;
    [[nodiscard]] std::uint64_t queue_full_count() const noexcept;
    [[nodiscard]] std::uint64_t dispatched_count() const noexcept;
    [[nodiscard]] std::uint64_t accepted_count() const noexcept;
    [[nodiscard]] std::uint64_t rejected_count() const noexcept;
    [[nodiscard]] std::uint64_t unknown_count() const noexcept;
    [[nodiscard]] std::uint64_t invalidated_count() const noexcept;

private:
    template <NativeGatewayOperation Operation, typename Intent>
    [[nodiscard]] NativeGatewayReceiptSnapshot enqueue_impl(
        const Intent& intent,
        std::uint64_t decision_time_ns,
        std::uint64_t enqueue_time_ns,
        bool latch_safety
    );

    [[nodiscard]] NativeGatewayReceiptSnapshot receipt_for(
        const NativeGatewayWireRequest& request,
        TransportPhase phase,
        TransportUnknownState unknown_state,
        bool admitted,
        std::string_view reason,
        bool retry_permitted = false,
        std::uint64_t dequeue_time_ns = 0,
        std::uint64_t dispatch_time_ns = 0,
        std::uint64_t wire_time_ns = 0,
        bool send_attempted = false,
        std::uint64_t response_time_ns = 0,
        std::uint64_t completion_time_ns = 0,
        std::uint64_t exchange_time_ns = 0
    ) const noexcept;

    [[nodiscard]] NativeGatewayWireRequest& require_active();
    void lock_safety_transition() noexcept;
    void unlock_safety_transition() noexcept;
    void latch_safety_barrier_locked() noexcept;
    void validate_enqueue_times(
        std::uint64_t decision_time_ns,
        std::uint64_t enqueue_time_ns
    ) const;

    struct alignas(kNativeGatewayCacheLineBytes) ProducerCounters {
        std::atomic<std::uint64_t> enqueued{0};
        std::atomic<std::uint64_t> queue_full{0};
    };

    struct alignas(kNativeGatewayCacheLineBytes) ConsumerCounters {
        std::atomic<std::uint64_t> dequeued{0};
        std::atomic<std::uint64_t> dispatched{0};
        std::atomic<std::uint64_t> accepted{0};
        std::atomic<std::uint64_t> rejected{0};
        std::atomic<std::uint64_t> unknown{0};
        std::atomic<std::uint64_t> invalidated{0};
    };

    TransportBackendKind backend_;
    SpscRing<NativeGatewayWireRequest, kNativeGatewayQueueCapacity> queue_;
    std::optional<NativeGatewayWireRequest> active_;
    std::uint64_t active_dequeue_time_ns_ = 0;
    std::uint64_t active_dispatch_time_ns_ = 0;
    std::uint64_t active_wire_time_ns_ = 0;
    std::uint64_t active_generation_ = 0;
    bool active_send_attempted_ = false;
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<bool> has_active_request_{false};
    // Reconciliation is represented by two monotonic epochs, not a boolean.
    // Clearing an observed epoch can therefore never erase a newer unknown
    // transport outcome that raced with reconciliation completion.
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> reconciliation_epoch_{0};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> reconciled_epoch_{0};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> reconciled_generation_{0};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> admission_epoch_{1};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> place_epoch_{1};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<bool> safety_barrier_latched_{false};
    // The safety transition lock covers latch->cancel publication and barrier
    // release. It closes the otherwise possible ABA window where a release
    // observes an empty queue between a new latch and its cancel publication.
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic_flag safety_transition_lock_ = ATOMIC_FLAG_INIT;
    // Multiple native decision/risk/callback sources may enqueue concurrently
    // once Python is removed. Serializing only the producer publication keeps
    // the compact SPSC ring valid without putting a mutex on the wire reader.
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic_flag producer_transition_lock_ = ATOMIC_FLAG_INIT;
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic_flag consumer_transition_lock_ = ATOMIC_FLAG_INIT;
#ifdef NARROWGATE_ORDER_GATEWAY_CORE_ONLY
    // Deterministic standalone regression hook for the PLACE peek -> safety
    // latch interleaving. It is absent from production extension builds.
    std::atomic<bool>* begin_next_place_peek_observed_test_ = nullptr;
    std::atomic<bool>* begin_next_place_continue_test_ = nullptr;
#endif
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<std::uint64_t> safety_release_generation_{0};
    alignas(kNativeGatewayCacheLineBytes)
        std::atomic<bool> safety_escalation_pending_{false};
    ProducerCounters producer_counters_;
    ConsumerCounters consumer_counters_;

    friend struct NativeUsdMOrderGatewayCoreTestAccess;
};

void bind_order_gateway_core(pybind11::module_& module);

}  // namespace narrowgate_cpp
