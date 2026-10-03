#include "order_gateway_core.hpp"

#include <stdexcept>
#include <thread>
#include <utility>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

#ifndef NARROWGATE_ORDER_GATEWAY_CORE_ONLY
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
#endif

namespace narrowgate_cpp {
namespace {

void bounded_contention_wait(std::uint32_t& attempts) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    constexpr std::uint32_t kPauseAttemptsBeforeYield = 64;
    if (attempts < kPauseAttemptsBeforeYield) {
        ++attempts;
        _mm_pause();
        return;
    }
#else
    (void)attempts;
#endif
    std::this_thread::yield();
}

class AtomicFlagLease final {
public:
    explicit AtomicFlagLease(
        std::atomic_flag& flag,
        bool acquire = true
    ) noexcept : flag_(acquire ? &flag : nullptr) {
        if (flag_ == nullptr) {
            return;
        }
        std::uint32_t attempts = 0;
        while (flag_->test_and_set(std::memory_order_acquire)) {
            bounded_contention_wait(attempts);
        }
    }
    AtomicFlagLease(const AtomicFlagLease&) = delete;
    AtomicFlagLease& operator=(const AtomicFlagLease&) = delete;
    ~AtomicFlagLease() {
        if (flag_ != nullptr) {
            flag_->clear(std::memory_order_release);
        }
    }

private:
    std::atomic_flag* flag_;
};

class SingleConsumerLease final {
public:
    explicit SingleConsumerLease(std::atomic_flag& flag) : flag_(&flag) {
        if (flag_->test_and_set(std::memory_order_acquire)) {
            flag_ = nullptr;
            throw std::logic_error(
                "native order gateway requires one non-overlapping consumer"
            );
        }
    }
    SingleConsumerLease(const SingleConsumerLease&) = delete;
    SingleConsumerLease& operator=(const SingleConsumerLease&) = delete;
    ~SingleConsumerLease() {
        if (flag_ != nullptr) {
            flag_->clear(std::memory_order_release);
        }
    }

private:
    std::atomic_flag* flag_;
};

template <NativeGatewayOperation Operation>
struct OperationTraits;

template <>
struct OperationTraits<NativeGatewayOperation::Place> {
    static constexpr std::string_view queue_full_reason = "place_queue_full";
};

template <>
struct OperationTraits<NativeGatewayOperation::Cancel> {
    static constexpr std::string_view queue_full_reason = "cancel_queue_full";
};

template <>
struct OperationTraits<NativeGatewayOperation::CancelAll> {
    static constexpr std::string_view queue_full_reason =
        "cancel_all_queue_full";
};

template <std::size_t Capacity>
void assign_inline_or_throw(
    InlineText<Capacity>& destination,
    const std::string& value,
    const char* field_name
) {
    if (!destination.assign(value)) {
        throw std::invalid_argument(
            std::string(field_name) + " exceeds native inline capacity"
        );
    }
}

NativeGatewayRequestSnapshot request_snapshot(
    const NativeGatewayWireRequest& request,
    std::uint64_t dequeue_time_ns,
    std::uint64_t generation
) {
    NativeGatewayRequestSnapshot snapshot;
    snapshot.operation = request.operation;
    snapshot.request_id = request.request_id;
    snapshot.decision_id = request.decision_id;
    snapshot.client_order_id = request.client_order_id;
    snapshot.symbol = request.symbol;
    snapshot.reason = request.reason;
    snapshot.side = request.side;
    snapshot.order_type = request.order_type;
    snapshot.time_in_force = request.time_in_force;
    snapshot.reduce_only = request.reduce_only;
    snapshot.post_only = request.post_only;
    snapshot.safety_fence = request.safety_fence;
    snapshot.price = request.price;
    snapshot.quantity = request.quantity;
    snapshot.exchange_order_id = request.exchange_order_id;
    snapshot.recv_window_ms = request.recv_window_ms;
    snapshot.deadline_time_ns = request.deadline_time_ns;
    snapshot.expected_ownership_generation =
        request.expected_ownership_generation;
    snapshot.decision_time_ns = request.decision_time_ns;
    snapshot.enqueue_time_ns = request.enqueue_time_ns;
    snapshot.dequeue_time_ns = dequeue_time_ns;
    snapshot.generation = generation;
    return snapshot;
}

}  // namespace

NativeUsdMOrderGatewayCore::NativeUsdMOrderGatewayCore(
    TransportBackendKind backend
) : backend_(backend) {
    if (backend != TransportBackendKind::CppUsdmRest &&
        backend != TransportBackendKind::CppUsdmWebSocket) {
        throw std::invalid_argument(
            "native USD-M gateway core requires CppUsdmRest or "
            "CppUsdmWebSocket; USD-M FIX is not an official Binance product"
        );
    }
}

void NativeUsdMOrderGatewayCore::validate_enqueue_times(
    std::uint64_t decision_time_ns,
    std::uint64_t enqueue_time_ns
) const {
    if (enqueue_time_ns == 0 ||
        (decision_time_ns != 0 && decision_time_ns > enqueue_time_ns)) {
        throw std::invalid_argument(
            "timestamps must satisfy decision<=enqueue with nonzero enqueue"
        );
    }
}

void NativeUsdMOrderGatewayCore::lock_safety_transition() noexcept {
    std::uint32_t attempts = 0;
    while (safety_transition_lock_.test_and_set(std::memory_order_acquire)) {
        bounded_contention_wait(attempts);
    }
}

void NativeUsdMOrderGatewayCore::unlock_safety_transition() noexcept {
    safety_transition_lock_.clear(std::memory_order_release);
}

void NativeUsdMOrderGatewayCore::latch_safety_barrier_locked() noexcept {
    safety_barrier_latched_.store(true, std::memory_order_release);
    // Only PLACE work is invalidated.  Previously queued cancel work remains
    // eligible because dropping it would weaken a risk response.
    place_epoch_.fetch_add(1, std::memory_order_acq_rel);
}

template <NativeGatewayOperation Operation, typename Intent>
NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::enqueue_impl(
    const Intent& intent,
    std::uint64_t decision_time_ns,
    std::uint64_t enqueue_time_ns,
    bool latch_safety
) {
    if (!intent.is_structurally_valid()) {
        throw std::invalid_argument(intent.validation_error());
    }
    validate_enqueue_times(decision_time_ns, enqueue_time_ns);
    AtomicFlagLease producer_lease(producer_transition_lock_);
    const auto admission_epoch = admission_epoch_.load(std::memory_order_acquire);
    const auto place_epoch = place_epoch_.load(std::memory_order_acquire);

    NativeGatewayWireRequest request;
    request.operation = Operation;
    request.decision_time_ns = decision_time_ns;
    request.enqueue_time_ns = enqueue_time_ns;
    request.admission_epoch = admission_epoch;
    request.place_epoch = place_epoch;
    request.expected_ownership_generation = intent.expected_ownership_generation;
    request.safety_fence = latch_safety;
    assign_inline_or_throw(request.request_id, intent.request_id, "request_id");
    assign_inline_or_throw(request.decision_id, intent.decision_id, "decision_id");
    assign_inline_or_throw(request.symbol, intent.symbol, "symbol");

    if constexpr (Operation == NativeGatewayOperation::Place) {
        request.side = intent.side;
        request.order_type = intent.order_type;
        request.time_in_force = intent.time_in_force;
        request.reduce_only = intent.reduce_only;
        request.post_only = intent.post_only;
        request.price = intent.price;
        request.quantity = intent.quantity;
        request.recv_window_ms = intent.recv_window_ms;
        request.deadline_time_ns = intent.deadline_time_ns;
        assign_inline_or_throw(
            request.client_order_id,
            intent.client_order_id,
            "client_order_id"
        );
    } else if constexpr (Operation == NativeGatewayOperation::Cancel) {
        request.exchange_order_id = intent.exchange_order_id;
        assign_inline_or_throw(
            request.client_order_id,
            intent.client_order_id,
            "client_order_id"
        );
        assign_inline_or_throw(request.reason, intent.reason, "reason");
    } else {
        static_assert(Operation == NativeGatewayOperation::CancelAll);
        assign_inline_or_throw(request.reason, intent.reason, "reason");
    }

    // A safety fence is published only after all fallible validation and
    // fixed-request construction has completed. Invalid timestamps or an
    // oversized field can therefore never latch the gateway without also
    // producing a cancel intent or an explicit escalation result.
    // Build the complete fixed request before any blocked receipt is emitted;
    // diagnostics must retain symbol, client ID, operation and ownership.
    if constexpr (Operation == NativeGatewayOperation::Place) {
        if (reconciliation_required()) {
            return receipt_for(
                request,
                TransportPhase::LocalValidated,
                TransportUnknownState::ConfirmedNotDispatched,
                false,
                "reconciliation_required",
                false
            );
        }
        if (safety_barrier_latched()) {
            return receipt_for(
                request,
                TransportPhase::LocalValidated,
                TransportUnknownState::ConfirmedNotDispatched,
                false,
                "safety_barrier_latched",
                false
            );
        }
    }

    // Recheck immediately before publication.  If a wire-ambiguous request
    // latched reconciliation while this producer was preparing the slot, the
    // old epoch is never admitted.  A request racing after this check retains
    // the old epoch and begin_next() invalidates it before execution.
    if ((Operation == NativeGatewayOperation::Place &&
         reconciliation_required()) ||
        (Operation == NativeGatewayOperation::Place &&
         admission_epoch != admission_epoch_.load(std::memory_order_acquire)) ||
        (Operation == NativeGatewayOperation::Place &&
         (safety_barrier_latched() ||
          place_epoch != place_epoch_.load(std::memory_order_acquire)))) {
        return receipt_for(
            request,
            TransportPhase::LocalValidated,
            TransportUnknownState::ConfirmedNotDispatched,
            false,
            reconciliation_required() ? "reconciliation_required"
                                      : "safety_barrier_latched",
            false
        );
    }
    auto enqueued_receipt = receipt_for(
        request,
        TransportPhase::Enqueued,
        TransportUnknownState::None,
        true,
        "enqueued"
    );
    constexpr std::size_t reserved_slots =
        Operation == NativeGatewayOperation::Place
        ? kNativeGatewaySafetyReserve
        : 0;
    if (latch_safety) {
        // Latch publication and the matching safety request form one
        // linearized transaction with release_safety_barrier().  A release
        // can never clear the new fence between these two operations.
        lock_safety_transition();
        latch_safety_barrier_locked();
        const bool pushed = queue_.try_push(request, reserved_slots);
        if (!pushed) {
            safety_escalation_pending_.store(true, std::memory_order_release);
        }
        unlock_safety_transition();
        enqueued_receipt.safety_barrier_latched = true;
        if (pushed) {
            producer_counters_.enqueued.fetch_add(1, std::memory_order_relaxed);
            return enqueued_receipt;
        }
    } else if (queue_.try_push(request, reserved_slots)) {
        producer_counters_.enqueued.fetch_add(1, std::memory_order_relaxed);
        return enqueued_receipt;
    }

    {
        producer_counters_.queue_full.fetch_add(1, std::memory_order_relaxed);
        auto receipt = receipt_for(
            request,
            TransportPhase::LocalValidated,
            TransportUnknownState::ConfirmedNotDispatched,
            false,
            OperationTraits<Operation>::queue_full_reason,
            Operation == NativeGatewayOperation::Place &&
                !reconciliation_required() && !safety_barrier_latched()
        );
        if constexpr (Operation != NativeGatewayOperation::Place) {
            // The native wire adapter must synchronously escalate this event;
            // a risk-reducing request may never disappear behind a full queue.
            receipt.safety_escalation_required = true;
            safety_escalation_pending_.store(true, std::memory_order_release);
        }
        return receipt;
    }
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::enqueue_order(
    const CanonicalOrderIntent& intent,
    std::uint64_t decision_time_ns,
    std::uint64_t enqueue_time_ns
) {
    return enqueue_impl<NativeGatewayOperation::Place>(
        intent,
        decision_time_ns,
        enqueue_time_ns,
        false
    );
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::enqueue_cancel(
    const CanonicalCancelIntent& intent,
    std::uint64_t decision_time_ns,
    std::uint64_t enqueue_time_ns,
    bool safety_fence
) {
    if (!intent.is_structurally_valid()) {
        throw std::invalid_argument(intent.validation_error());
    }
    return enqueue_impl<NativeGatewayOperation::Cancel>(
        intent,
        decision_time_ns,
        enqueue_time_ns,
        safety_fence
    );
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::enqueue_cancel_all(
    const CanonicalCancelAllIntent& intent,
    std::uint64_t decision_time_ns,
    std::uint64_t enqueue_time_ns
) {
    if (!intent.is_structurally_valid()) {
        throw std::invalid_argument(intent.validation_error());
    }
    return enqueue_impl<NativeGatewayOperation::CancelAll>(
        intent,
        decision_time_ns,
        enqueue_time_ns,
        true
    );
}

NativeGatewayDequeueResult
NativeUsdMOrderGatewayCore::begin_next(
    std::uint64_t dequeue_time_ns,
    std::uint64_t generation,
    std::uint64_t current_ownership_generation
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    if (has_active_request()) {
        throw std::runtime_error("native order gateway already has an active request");
    }
    NativeGatewayDequeueResult result;
    for (;;) {
        NativeGatewayWireRequest request;
        if (!queue_.try_peek(request)) {
            return result;
        }
#ifdef NARROWGATE_ORDER_GATEWAY_CORE_ONLY
        if (request.operation == NativeGatewayOperation::Place &&
            begin_next_place_peek_observed_test_ != nullptr &&
            begin_next_place_continue_test_ != nullptr) {
            begin_next_place_peek_observed_test_->store(
                true,
                std::memory_order_release
            );
            while (!begin_next_place_continue_test_->load(
                std::memory_order_acquire
            )) {
                std::this_thread::yield();
            }
        }
#endif
        // A safety latch increments place_epoch_ while holding the same lock.
        // Keep the fresh epoch/barrier check and PLACE pop/active publication
        // in one critical section so a request observed before the latch cannot
        // become active after it. Safety CANCEL dequeue shares this lock with
        // barrier release for the same queue/active ownership boundary.
        const bool needs_safety_transition =
            request.operation == NativeGatewayOperation::Place ||
            request.safety_fence;
        AtomicFlagLease safety_lease(
            safety_transition_lock_,
            needs_safety_transition
        );
        const auto current_epoch = admission_epoch_.load(std::memory_order_acquire);
        const auto current_place_epoch = place_epoch_.load(std::memory_order_acquire);
        if (dequeue_time_ns == 0) {
            throw std::invalid_argument(
                "timestamps must satisfy enqueue<=dequeue with nonzero dequeue"
            );
        }
        if (dequeue_time_ns < request.enqueue_time_ns) {
            // A fixed dequeue timestamp may legitimately consume stale work
            // and then reach a future head published by the producer. Return
            // the accumulated invalidations instead of throwing them away.
            if (!result.invalidations.empty()) {
                return result;
            }
            throw std::invalid_argument(
                "timestamps must satisfy enqueue<=dequeue with nonzero dequeue"
            );
        }
        const bool stale_admission =
            request.operation == NativeGatewayOperation::Place &&
            request.admission_epoch != current_epoch;
        const bool stale_place =
            request.operation == NativeGatewayOperation::Place &&
            (request.place_epoch != current_place_epoch ||
             safety_barrier_latched_.load(std::memory_order_acquire));
        const bool stale_ownership =
            request.operation != NativeGatewayOperation::CancelAll &&
            request.expected_ownership_generation != 0 &&
            request.expected_ownership_generation != current_ownership_generation;
        const bool expired = request.deadline_time_ns != 0 &&
                             dequeue_time_ns > request.deadline_time_ns;
        if (stale_admission || stale_place || stale_ownership || expired) {
            // Allocation is exceptional rather than part of empty polling or
            // the successful steady path.  Reserve before consuming the first
            // invalid request so allocation failure cannot lose queue state.
            if (result.invalidations.capacity() == 0) {
                result.invalidations.reserve(kNativeGatewayQueueCapacity);
            }
            const char* reason = stale_admission
                ? "invalidated_by_reconciliation_epoch"
                : stale_place
                    ? "invalidated_by_safety_barrier"
                    : stale_ownership
                        ? "ownership_generation_mismatch"
                        : "deadline_expired_before_dequeue";
            // Construct every potentially allocating diagnostic before the
            // queue cursor moves. An allocation failure cannot make a caller
            // lose track of a request that the core already consumed.
            auto invalidation = receipt_for(
                request,
                TransportPhase::Enqueued,
                TransportUnknownState::ConfirmedNotDispatched,
                false,
                reason,
                false,
                dequeue_time_ns
            );
            if (request.operation != NativeGatewayOperation::Place) {
                invalidation.safety_escalation_required = true;
                safety_escalation_pending_.store(
                    true,
                    std::memory_order_release
                );
            }
            result.invalidations.push_back(std::move(invalidation));
            NativeGatewayWireRequest removed;
            if (!queue_.try_pop(removed)) {
                throw std::logic_error(
                    "native gateway SPSC queue changed without its consumer"
                );
            }
            consumer_counters_.invalidated.fetch_add(
                1,
                std::memory_order_relaxed
            );
            continue;
        }

        // Snapshot allocation also precedes the irreversible queue pop and
        // active-request transition.
        auto snapshot = request_snapshot(request, dequeue_time_ns, generation);
        NativeGatewayWireRequest removed;
        if (!queue_.try_pop(removed)) {
            throw std::logic_error(
                "native gateway SPSC queue changed without its consumer"
            );
        }
        active_ = removed;
        active_dequeue_time_ns_ = dequeue_time_ns;
        active_dispatch_time_ns_ = 0;
        active_wire_time_ns_ = 0;
        active_generation_ = generation;
        active_send_attempted_ = false;
        result.request = std::move(snapshot);
        has_active_request_.store(true, std::memory_order_release);
        consumer_counters_.dequeued.fetch_add(1, std::memory_order_relaxed);
        return result;
    }
}

NativeGatewayWireRequest& NativeUsdMOrderGatewayCore::require_active() {
    if (!active_.has_value()) {
        throw std::runtime_error("native order gateway has no active request");
    }
    return active_.value();
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::mark_wire_dispatched(
    std::uint64_t dispatch_time_ns,
    std::uint64_t wire_time_ns
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    auto& request = require_active();
    if (dispatch_time_ns == 0 || dispatch_time_ns < active_dequeue_time_ns_ ||
        wire_time_ns < dispatch_time_ns) {
        throw std::invalid_argument(
            "timestamps must satisfy dequeue<=dispatch<=wire"
        );
    }
    if (request.deadline_time_ns != 0 &&
        wire_time_ns > request.deadline_time_ns) {
        throw std::invalid_argument("request deadline expired before wire");
    }
    if (!active_send_attempted_) {
        throw std::runtime_error(
            "wire dispatch requires a prior pre-send fence"
        );
    }
    if (active_wire_time_ns_ != 0) {
        throw std::runtime_error("active request was already wire-dispatched");
    }
    if (active_send_attempted_ && dispatch_time_ns != active_dispatch_time_ns_) {
        throw std::invalid_argument(
            "wire dispatch must retain the original send-attempt timestamp"
        );
    }
    auto receipt = receipt_for(
        request,
        TransportPhase::WireDispatched,
        TransportUnknownState::None,
        true,
        "wire_dispatched",
        false,
        active_dequeue_time_ns_,
        dispatch_time_ns,
        wire_time_ns,
        true
    );
    receipt.generation = active_generation_;
    active_dispatch_time_ns_ = dispatch_time_ns;
    active_wire_time_ns_ = wire_time_ns;
    active_send_attempted_ = true;
    consumer_counters_.dispatched.fetch_add(1, std::memory_order_relaxed);
    return receipt;
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::mark_send_attempted(
    std::uint64_t dispatch_time_ns
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    auto& request = require_active();
    if (dispatch_time_ns == 0 || dispatch_time_ns < active_dequeue_time_ns_) {
        throw std::invalid_argument(
            "timestamps must satisfy dequeue<=dispatch with nonzero dispatch"
        );
    }
    if (!active_send_attempted_ &&
        request.operation == NativeGatewayOperation::Place &&
        request.place_epoch != place_epoch_.load(std::memory_order_acquire)) {
        auto receipt = receipt_for(
            request,
            TransportPhase::Enqueued,
            TransportUnknownState::ConfirmedNotDispatched,
            false,
            "invalidated_by_safety_barrier_before_send",
            false,
            active_dequeue_time_ns_,
            0,
            0,
            false,
            0,
            dispatch_time_ns,
            0
        );
        receipt.generation = active_generation_;
        consumer_counters_.invalidated.fetch_add(1, std::memory_order_relaxed);
        active_.reset();
        has_active_request_.store(false, std::memory_order_release);
        return receipt;
    }
    if (request.deadline_time_ns != 0 &&
        dispatch_time_ns > request.deadline_time_ns) {
        throw std::invalid_argument("request deadline expired before send attempt");
    }
    if (active_send_attempted_) {
        throw std::runtime_error("active request already attempted a send");
    }
    auto receipt = receipt_for(
        request,
        TransportPhase::Enqueued,
        TransportUnknownState::None,
        true,
        "send_attempted",
        false,
        active_dequeue_time_ns_,
        dispatch_time_ns,
        0,
        true
    );
    receipt.generation = active_generation_;
    active_dispatch_time_ns_ = dispatch_time_ns;
    active_send_attempted_ = true;
    return receipt;
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::mark_exchange_ack(
    bool accepted,
    std::uint64_t response_time_ns,
    std::uint64_t completion_time_ns,
    std::uint64_t exchange_time_ns,
    const std::string& reason
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    auto& request = require_active();
    if (active_dispatch_time_ns_ == 0) {
        throw std::runtime_error("exchange ACK cannot precede wire dispatch");
    }
    if (!active_send_attempted_ || active_wire_time_ns_ == 0) {
        throw std::runtime_error("exchange ACK cannot precede wire dispatch");
    }
    if (response_time_ns < active_wire_time_ns_ ||
        completion_time_ns < response_time_ns) {
        throw std::invalid_argument("ACK timestamps must be monotonic");
    }
    // exchange_time_ns belongs to the venue clock domain.  It is deliberately
    // not ordered against local dispatch/response/completion timestamps.
    auto receipt = receipt_for(
        request,
        accepted ? TransportPhase::ExchangeAckAccepted
                 : TransportPhase::ExchangeAckRejected,
        TransportUnknownState::None,
        true,
        reason,
        false,
        active_dequeue_time_ns_,
        active_dispatch_time_ns_,
        active_wire_time_ns_,
        true,
        response_time_ns,
        completion_time_ns,
        exchange_time_ns
    );
    receipt.generation = active_generation_;
    if (accepted) {
        consumer_counters_.accepted.fetch_add(1, std::memory_order_relaxed);
    } else {
        consumer_counters_.rejected.fetch_add(1, std::memory_order_relaxed);
        if (request.operation != NativeGatewayOperation::Place) {
            receipt.safety_escalation_required = true;
            safety_escalation_pending_.store(true, std::memory_order_release);
        }
    }
    active_.reset();
    has_active_request_.store(false, std::memory_order_release);
    return receipt;
}

NativeGatewayReceiptSnapshot
NativeUsdMOrderGatewayCore::mark_transport_unknown(
    std::uint64_t completion_time_ns,
    const std::string& reason
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    auto& request = require_active();
    if (!active_send_attempted_) {
        throw std::runtime_error(
            "transport unknown requires an explicit send attempt; use "
            "mark_confirmed_not_dispatched before any send attempt"
        );
    }
    if (completion_time_ns < active_dispatch_time_ns_) {
        throw std::invalid_argument("completion_time_ns precedes send attempt");
    }
    auto receipt = receipt_for(
        request,
        active_wire_time_ns_ != 0 ? TransportPhase::WireDispatched
                                  : TransportPhase::Enqueued,
        TransportUnknownState::AwaitingReconciliation,
        true,
        reason,
        false,
        active_dequeue_time_ns_,
        active_dispatch_time_ns_,
        active_wire_time_ns_,
        true,
        0,
        completion_time_ns,
        0
    );
    receipt.generation = active_generation_;
    receipt.reconciliation_required = true;
    if (request.operation != NativeGatewayOperation::Place) {
        receipt.safety_escalation_required = true;
        safety_escalation_pending_.store(true, std::memory_order_release);
    }
    // Publish the required epoch first. A producer either observes it and is
    // blocked or still carries the previous admission epoch and is rejected
    // by begin_next(). Clearing an older observed epoch cannot erase this one.
    reconciliation_epoch_.fetch_add(1, std::memory_order_acq_rel);
    // Every already-queued PLACE request was admitted before this ambiguous
    // write. Cancel work remains executable while reconciliation is pending.
    admission_epoch_.fetch_add(1, std::memory_order_acq_rel);
    consumer_counters_.unknown.fetch_add(1, std::memory_order_relaxed);
    active_.reset();
    has_active_request_.store(false, std::memory_order_release);
    return receipt;
}

NativeGatewayReceiptSnapshot
NativeUsdMOrderGatewayCore::mark_confirmed_not_dispatched(
    std::uint64_t completion_time_ns,
    const std::string& reason
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    auto& request = require_active();
    if (active_send_attempted_) {
        throw std::runtime_error(
            "cannot confirm not-dispatched after a send attempt"
        );
    }
    if (completion_time_ns < active_dequeue_time_ns_) {
        throw std::invalid_argument("completion_time_ns precedes dequeue_time_ns");
    }
    auto receipt = receipt_for(
        request,
        TransportPhase::Enqueued,
        TransportUnknownState::ConfirmedNotDispatched,
        true,
        reason,
        !reconciliation_required() && !safety_barrier_latched(),
        active_dequeue_time_ns_,
        0,
        0,
        false,
        0,
        completion_time_ns,
        0
    );
    receipt.generation = active_generation_;
    if (request.operation != NativeGatewayOperation::Place) {
        // A risk-reducing request proved not to have reached the wire still
        // has to be issued by the safety executor. The latched barrier blocks
        // exposure but is not itself a substitute for the cancel action.
        receipt.safety_escalation_required = true;
        safety_escalation_pending_.store(true, std::memory_order_release);
    }
    active_.reset();
    has_active_request_.store(false, std::memory_order_release);
    return receipt;
}

void NativeUsdMOrderGatewayCore::mark_reconciled(
    std::uint64_t generation,
    std::uint64_t expected_reconciliation_epoch
) {
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    if (has_active_request()) {
        throw std::runtime_error(
            "cannot mark gateway reconciled with an active request"
        );
    }
    if (expected_reconciliation_epoch == 0 ||
        reconciliation_epoch_.load(std::memory_order_acquire) !=
            expected_reconciliation_epoch) {
        throw std::invalid_argument(
            "reconciliation epoch changed or was not supplied"
        );
    }
    auto previous_generation = reconciled_generation_.load(
        std::memory_order_acquire
    );
    do {
        if (generation == 0 || generation <= previous_generation) {
            throw std::invalid_argument(
                "reconciliation generation must strictly increase and be nonzero"
            );
        }
    } while (!reconciled_generation_.compare_exchange_weak(
        previous_generation,
        generation,
        std::memory_order_acq_rel,
        std::memory_order_acquire
    ));
    // Only acknowledge the exact epoch covered by the caller's exchange
    // reconciliation. If a new unknown result races after the equality check,
    // its larger epoch remains required.
    auto previous_epoch = reconciled_epoch_.load(std::memory_order_acquire);
    while (previous_epoch < expected_reconciliation_epoch &&
           !reconciled_epoch_.compare_exchange_weak(
               previous_epoch,
               expected_reconciliation_epoch,
               std::memory_order_acq_rel,
               std::memory_order_acquire
           )) {
    }
}

void NativeUsdMOrderGatewayCore::release_safety_barrier(
    std::uint64_t generation,
    bool safety_action_resolved
) {
    // Match enqueue_impl's lock order.  Serializing with every producer only
    // at this cold release boundary makes the queue-empty observation and the
    // barrier clear one transaction.  A normal PLACE producer can therefore
    // only be rejected before the release or admitted after it; it cannot be
    // missed between the empty check and the clear.
    AtomicFlagLease producer_lease(producer_transition_lock_);
    SingleConsumerLease consumer_lease(consumer_transition_lock_);
    lock_safety_transition();
    try {
        if (has_active_request() || pending_count() != 0) {
            throw std::runtime_error(
                "cannot release safety barrier with active or queued requests"
            );
        }
        if (reconciliation_required()) {
            throw std::runtime_error(
                "cannot release safety barrier before gateway reconciliation"
            );
        }
        if (!safety_barrier_latched()) {
            throw std::runtime_error("native gateway safety barrier is not latched");
        }
        if (safety_escalation_pending() && !safety_action_resolved) {
            throw std::runtime_error(
                "cannot release safety barrier before resolving escalation"
            );
        }
        auto previous_generation = safety_release_generation_.load(
            std::memory_order_acquire
        );
        do {
            if (generation == 0 || generation <= previous_generation) {
                throw std::invalid_argument(
                    "safety release generation must strictly increase and be nonzero"
                );
            }
        } while (!safety_release_generation_.compare_exchange_weak(
            previous_generation,
            generation,
            std::memory_order_acq_rel,
            std::memory_order_acquire
        ));
        safety_barrier_latched_.store(false, std::memory_order_release);
        safety_escalation_pending_.store(false, std::memory_order_release);
    } catch (...) {
        unlock_safety_transition();
        throw;
    }
    unlock_safety_transition();
}

NativeGatewayReceiptSnapshot NativeUsdMOrderGatewayCore::receipt_for(
    const NativeGatewayWireRequest& request,
    TransportPhase phase,
    TransportUnknownState unknown_state,
    bool admitted,
    std::string_view reason,
    bool retry_permitted,
    std::uint64_t dequeue_time_ns,
    std::uint64_t dispatch_time_ns,
    std::uint64_t wire_time_ns,
    bool send_attempted,
    std::uint64_t response_time_ns,
    std::uint64_t completion_time_ns,
    std::uint64_t exchange_time_ns
) const noexcept {
    NativeGatewayReceiptSnapshot receipt;
    receipt.backend = backend_;
    receipt.operation = request.operation;
    receipt.phase = phase;
    receipt.unknown_state = unknown_state;
    receipt.admitted = admitted;
    receipt.reconciliation_required =
        unknown_state == TransportUnknownState::AwaitingReconciliation ||
        reconciliation_required();
    receipt.safety_barrier_latched = safety_barrier_latched();
    receipt.send_attempted = send_attempted;
    receipt.retry_permitted = retry_permitted;
    receipt.request_id = request.request_id;
    receipt.decision_id = request.decision_id;
    receipt.client_order_id = request.client_order_id;
    receipt.reason_truncated = receipt.reason.assign_truncated(reason);
    receipt.generation = request.expected_ownership_generation;
    receipt.decision_time_ns = request.decision_time_ns;
    receipt.enqueue_time_ns = request.enqueue_time_ns;
    receipt.dequeue_time_ns = dequeue_time_ns;
    receipt.dispatch_time_ns = dispatch_time_ns;
    receipt.wire_time_ns = wire_time_ns;
    receipt.response_time_ns = response_time_ns;
    receipt.completion_time_ns = completion_time_ns;
    receipt.exchange_time_ns = exchange_time_ns;
    return receipt;
}

std::uint64_t NativeUsdMOrderGatewayCore::enqueued_count() const noexcept {
    return producer_counters_.enqueued.load(std::memory_order_relaxed);
}

std::uint64_t NativeUsdMOrderGatewayCore::queue_full_count() const noexcept {
    return producer_counters_.queue_full.load(std::memory_order_relaxed);
}

#define NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(name)                    \
    std::uint64_t NativeUsdMOrderGatewayCore::name##_count() const noexcept { \
        return consumer_counters_.name.load(std::memory_order_relaxed);     \
    }

NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(dequeued)
NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(dispatched)
NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(accepted)
NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(rejected)
NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(unknown)
NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR(invalidated)

#undef NATIVE_GATEWAY_CONSUMER_COUNTER_ACCESSOR

#ifndef NARROWGATE_ORDER_GATEWAY_CORE_ONLY
void bind_order_gateway_core(py::module_& module) {
    py::enum_<NativeGatewayOperation>(module, "NativeGatewayOperation")
        .value("Place", NativeGatewayOperation::Place)
        .value("Cancel", NativeGatewayOperation::Cancel)
        .value("CancelAll", NativeGatewayOperation::CancelAll);

    py::class_<NativeGatewayRequestSnapshot>(
        module,
        "NativeGatewayRequestSnapshot"
    )
        .def_readonly("operation", &NativeGatewayRequestSnapshot::operation)
        .def_property_readonly("request_id", [](const NativeGatewayRequestSnapshot& value) {
            return value.request_id.str();
        })
        .def_property_readonly("decision_id", [](const NativeGatewayRequestSnapshot& value) {
            return value.decision_id.str();
        })
        .def_property_readonly(
            "client_order_id",
            [](const NativeGatewayRequestSnapshot& value) {
                return value.client_order_id.str();
            }
        )
        .def_property_readonly("symbol", [](const NativeGatewayRequestSnapshot& value) {
            return value.symbol.str();
        })
        .def_property_readonly("reason", [](const NativeGatewayRequestSnapshot& value) {
            return value.reason.str();
        })
        .def_readonly("side", &NativeGatewayRequestSnapshot::side)
        .def_readonly("order_type", &NativeGatewayRequestSnapshot::order_type)
        .def_readonly(
            "time_in_force",
            &NativeGatewayRequestSnapshot::time_in_force
        )
        .def_readonly("reduce_only", &NativeGatewayRequestSnapshot::reduce_only)
        .def_readonly("post_only", &NativeGatewayRequestSnapshot::post_only)
        .def_readonly(
            "safety_fence",
            &NativeGatewayRequestSnapshot::safety_fence
        )
        .def_readonly("price", &NativeGatewayRequestSnapshot::price)
        .def_readonly("quantity", &NativeGatewayRequestSnapshot::quantity)
        .def_readonly(
            "exchange_order_id",
            &NativeGatewayRequestSnapshot::exchange_order_id
        )
        .def_readonly("recv_window_ms", &NativeGatewayRequestSnapshot::recv_window_ms)
        .def_readonly(
            "deadline_time_ns",
            &NativeGatewayRequestSnapshot::deadline_time_ns
        )
        .def_readonly(
            "expected_ownership_generation",
            &NativeGatewayRequestSnapshot::expected_ownership_generation
        )
        .def_readonly(
            "decision_time_ns",
            &NativeGatewayRequestSnapshot::decision_time_ns
        )
        .def_readonly(
            "enqueue_time_ns",
            &NativeGatewayRequestSnapshot::enqueue_time_ns
        )
        .def_readonly(
            "dequeue_time_ns",
            &NativeGatewayRequestSnapshot::dequeue_time_ns
        )
        .def_readonly("generation", &NativeGatewayRequestSnapshot::generation);

    py::class_<NativeGatewayReceiptSnapshot>(
        module,
        "NativeGatewayReceiptSnapshot"
    )
        .def_readonly("backend", &NativeGatewayReceiptSnapshot::backend)
        .def_readonly("operation", &NativeGatewayReceiptSnapshot::operation)
        .def_readonly("phase", &NativeGatewayReceiptSnapshot::phase)
        .def_readonly("unknown_state", &NativeGatewayReceiptSnapshot::unknown_state)
        .def_readonly("admitted", &NativeGatewayReceiptSnapshot::admitted)
        .def_readonly(
            "reconciliation_required",
            &NativeGatewayReceiptSnapshot::reconciliation_required
        )
        .def_readonly(
            "safety_barrier_latched",
            &NativeGatewayReceiptSnapshot::safety_barrier_latched
        )
        .def_readonly("send_attempted", &NativeGatewayReceiptSnapshot::send_attempted)
        .def_readonly("retry_permitted", &NativeGatewayReceiptSnapshot::retry_permitted)
        .def_readonly(
            "safety_escalation_required",
            &NativeGatewayReceiptSnapshot::safety_escalation_required
        )
        .def_readonly(
            "reason_truncated",
            &NativeGatewayReceiptSnapshot::reason_truncated
        )
        .def_property_readonly("request_id", [](const NativeGatewayReceiptSnapshot& value) {
            return value.request_id.str();
        })
        .def_property_readonly("decision_id", [](const NativeGatewayReceiptSnapshot& value) {
            return value.decision_id.str();
        })
        .def_property_readonly(
            "client_order_id",
            [](const NativeGatewayReceiptSnapshot& value) {
                return value.client_order_id.str();
            }
        )
        .def_property_readonly("reason", [](const NativeGatewayReceiptSnapshot& value) {
            return value.reason.str();
        })
        .def_readonly("generation", &NativeGatewayReceiptSnapshot::generation)
        .def_readonly(
            "decision_time_ns",
            &NativeGatewayReceiptSnapshot::decision_time_ns
        )
        .def_readonly(
            "enqueue_time_ns",
            &NativeGatewayReceiptSnapshot::enqueue_time_ns
        )
        .def_readonly(
            "dequeue_time_ns",
            &NativeGatewayReceiptSnapshot::dequeue_time_ns
        )
        .def_readonly(
            "dispatch_time_ns",
            &NativeGatewayReceiptSnapshot::dispatch_time_ns
        )
        .def_readonly("wire_time_ns", &NativeGatewayReceiptSnapshot::wire_time_ns)
        .def_readonly(
            "response_time_ns",
            &NativeGatewayReceiptSnapshot::response_time_ns
        )
        .def_readonly(
            "completion_time_ns",
            &NativeGatewayReceiptSnapshot::completion_time_ns
        )
        .def_readonly(
            "exchange_time_ns",
            &NativeGatewayReceiptSnapshot::exchange_time_ns
        )
        .def(
            "allows_cross_backend_retry",
            &NativeGatewayReceiptSnapshot::allows_cross_backend_retry
        );

    py::class_<NativeGatewayDequeueResult>(module, "NativeGatewayDequeueResult")
        .def_readonly("request", &NativeGatewayDequeueResult::request)
        .def_readonly("invalidations", &NativeGatewayDequeueResult::invalidations);

    py::class_<NativeUsdMOrderGatewayCore>(
        module,
        "NativeUsdMOrderGatewayCore"
    )
        .def(py::init<TransportBackendKind>(), py::arg("backend"))
        .def(
            "enqueue_order",
            &NativeUsdMOrderGatewayCore::enqueue_order,
            py::arg("intent"),
            py::arg("decision_time_ns"),
            py::arg("enqueue_time_ns")
        )
        .def(
            "enqueue_cancel",
            &NativeUsdMOrderGatewayCore::enqueue_cancel,
            py::arg("intent"),
            py::arg("decision_time_ns"),
            py::arg("enqueue_time_ns"),
            py::arg("safety_fence") = false
        )
        .def(
            "enqueue_cancel_all",
            &NativeUsdMOrderGatewayCore::enqueue_cancel_all,
            py::arg("intent"),
            py::arg("decision_time_ns"),
            py::arg("enqueue_time_ns")
        )
        .def(
            "begin_next",
            &NativeUsdMOrderGatewayCore::begin_next,
            py::arg("dequeue_time_ns"),
            py::arg("generation"),
            py::arg("current_ownership_generation")
        )
        .def(
            "mark_send_attempted",
            &NativeUsdMOrderGatewayCore::mark_send_attempted,
            py::arg("dispatch_time_ns")
        )
        .def(
            "mark_wire_dispatched",
            &NativeUsdMOrderGatewayCore::mark_wire_dispatched,
            py::arg("dispatch_time_ns"),
            py::arg("wire_time_ns")
        )
        .def(
            "mark_exchange_ack",
            &NativeUsdMOrderGatewayCore::mark_exchange_ack,
            py::arg("accepted"),
            py::arg("response_time_ns"),
            py::arg("completion_time_ns"),
            py::arg("exchange_time_ns") = 0,
            py::arg("reason") = ""
        )
        .def(
            "mark_transport_unknown",
            &NativeUsdMOrderGatewayCore::mark_transport_unknown,
            py::arg("completion_time_ns"),
            py::arg("reason")
        )
        .def(
            "mark_confirmed_not_dispatched",
            &NativeUsdMOrderGatewayCore::mark_confirmed_not_dispatched,
            py::arg("completion_time_ns"),
            py::arg("reason")
        )
        .def(
            "mark_reconciled",
            &NativeUsdMOrderGatewayCore::mark_reconciled,
            py::arg("generation"),
            py::arg("expected_reconciliation_epoch")
        )
        .def(
            "release_safety_barrier",
            &NativeUsdMOrderGatewayCore::release_safety_barrier,
            py::arg("generation"),
            py::arg("safety_action_resolved") = false
        )
        .def_property_readonly("backend", &NativeUsdMOrderGatewayCore::backend)
        .def_property_readonly(
            "pending_count",
            &NativeUsdMOrderGatewayCore::pending_count
        )
        .def_property_readonly(
            "has_active_request",
            &NativeUsdMOrderGatewayCore::has_active_request
        )
        .def_property_readonly(
            "reconciliation_required",
            &NativeUsdMOrderGatewayCore::reconciliation_required
        )
        .def_property_readonly(
            "reconciled_generation",
            &NativeUsdMOrderGatewayCore::reconciled_generation
        )
        .def_property_readonly(
            "reconciliation_epoch",
            &NativeUsdMOrderGatewayCore::reconciliation_epoch
        )
        .def_property_readonly(
            "safety_barrier_latched",
            &NativeUsdMOrderGatewayCore::safety_barrier_latched
        )
        .def_property_readonly(
            "safety_escalation_pending",
            &NativeUsdMOrderGatewayCore::safety_escalation_pending
        )
        .def_property_readonly(
            "safety_release_generation",
            &NativeUsdMOrderGatewayCore::safety_release_generation
        )
        .def_property_readonly("enqueued_count", &NativeUsdMOrderGatewayCore::enqueued_count)
        .def_property_readonly("dequeued_count", &NativeUsdMOrderGatewayCore::dequeued_count)
        .def_property_readonly("queue_full_count", &NativeUsdMOrderGatewayCore::queue_full_count)
        .def_property_readonly("dispatched_count", &NativeUsdMOrderGatewayCore::dispatched_count)
        .def_property_readonly("accepted_count", &NativeUsdMOrderGatewayCore::accepted_count)
        .def_property_readonly("rejected_count", &NativeUsdMOrderGatewayCore::rejected_count)
        .def_property_readonly("unknown_count", &NativeUsdMOrderGatewayCore::unknown_count)
        .def_property_readonly(
            "invalidated_count",
            &NativeUsdMOrderGatewayCore::invalidated_count
        );

    module.attr("NATIVE_ORDER_GATEWAY_CORE_AVAILABLE") = py::bool_(true);
    module.attr("NATIVE_ORDER_GATEWAY_QUEUE_CAPACITY") =
        py::int_(kNativeGatewayQueueCapacity);
    module.attr("NATIVE_ORDER_GATEWAY_SAFETY_RESERVE") =
        py::int_(kNativeGatewaySafetyReserve);
    module.attr("NATIVE_ORDER_GATEWAY_CACHE_LINE_BYTES") =
        py::int_(kNativeGatewayCacheLineBytes);
    module.attr("NATIVE_ORDER_GATEWAY_WIRE_REQUEST_BYTES") =
        py::int_(sizeof(NativeGatewayWireRequest));
    module.attr("NATIVE_ORDER_GATEWAY_WIRE_ADAPTER_AVAILABLE") = py::bool_(false);
}
#endif

}  // namespace narrowgate_cpp
