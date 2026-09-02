#include "narrowgate_cpp/live_order_state.hpp"
#include "narrowgate_cpp/order_gateway_core.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::atomic<bool> fail_regular_allocations{false};

[[nodiscard]] void* allocate_regular(std::size_t size) {
    if (fail_regular_allocations.load(std::memory_order_relaxed)) {
        throw std::bad_alloc{};
    }
    if (void* const value = std::malloc(size == 0 ? 1 : size)) {
        return value;
    }
    throw std::bad_alloc{};
}

}  // namespace

void* operator new(std::size_t size) {
    return allocate_regular(size);
}

void* operator new[](std::size_t size) {
    return allocate_regular(size);
}

void operator delete(void* value) noexcept {
    std::free(value);
}

void operator delete[](void* value) noexcept {
    std::free(value);
}

void operator delete(void* value, std::size_t) noexcept {
    std::free(value);
}

void operator delete[](void* value, std::size_t) noexcept {
    std::free(value);
}

namespace narrowgate_cpp {

static_assert(sizeof(NativeLiveOrderInlineText<64>) == 65);
static_assert(sizeof(NativeLiveOrderInlineText<32>) == 33);
static_assert(sizeof(NativeLiveOrderSnapshot) == 200);
static_assert(alignof(NativeLiveOrderSnapshot) == 8);
static_assert(offsetof(NativeLiveOrderSnapshot, client_order_id) == 6);
static_assert(offsetof(NativeLiveOrderSnapshot, ownership_generation) == 104);
static_assert(sizeof(NativeLiveOrderTransition) == 216);
static_assert(alignof(NativeLiveOrderTransition) == 8);
static_assert(offsetof(NativeLiveOrderTransition, reason) == 8);
static_assert(offsetof(NativeLiveOrderTransition, order) == 16);

struct NativeLiveOrderStateCoreTestAccess {
    static std::unique_lock<std::mutex> lock_side(
        NativeLiveOrderStateCore& core,
        CanonicalSide side
    ) {
        return std::unique_lock<std::mutex>(
            side == CanonicalSide::Buy ? core.buy_.mutex : core.sell_.mutex
        );
    }

    static std::uint64_t active_leases(
        const NativeLiveOrderStateCore& core
    ) noexcept {
        return core.mutation_gate_.load(std::memory_order_acquire) &
            NativeLiveOrderStateCore::kMutationGateLeaseMask;
    }

    static bool gate_closed(const NativeLiveOrderStateCore& core) noexcept {
        return (
            core.mutation_gate_.load(std::memory_order_acquire) &
            NativeLiveOrderStateCore::kMutationGateClosedBit
        ) != 0;
    }
};

struct NativeUsdMOrderGatewayCoreTestAccess {
    static void lock_producer(NativeUsdMOrderGatewayCore& core) noexcept {
        while (core.producer_transition_lock_.test_and_set(
            std::memory_order_acquire
        )) {
            std::this_thread::yield();
        }
    }

    static void unlock_producer(NativeUsdMOrderGatewayCore& core) noexcept {
        core.producer_transition_lock_.clear(std::memory_order_release);
    }

    static void lock_consumer(NativeUsdMOrderGatewayCore& core) noexcept {
        while (core.consumer_transition_lock_.test_and_set(
            std::memory_order_acquire
        )) {
            std::this_thread::yield();
        }
    }

    static void unlock_consumer(NativeUsdMOrderGatewayCore& core) noexcept {
        core.consumer_transition_lock_.clear(std::memory_order_release);
    }

    static void pause_begin_next_after_place_peek(
        NativeUsdMOrderGatewayCore& core,
        std::atomic<bool>& observed,
        std::atomic<bool>& resume
    ) noexcept {
        core.begin_next_place_peek_observed_test_ = &observed;
        core.begin_next_place_continue_test_ = &resume;
    }

    static void clear_begin_next_place_pause(
        NativeUsdMOrderGatewayCore& core
    ) noexcept {
        core.begin_next_place_peek_observed_test_ = nullptr;
        core.begin_next_place_continue_test_ = nullptr;
    }
};

namespace {

class FailRegularAllocations {
public:
    FailRegularAllocations() noexcept {
        fail_regular_allocations.store(true, std::memory_order_relaxed);
    }

    FailRegularAllocations(const FailRegularAllocations&) = delete;
    FailRegularAllocations& operator=(const FailRegularAllocations&) = delete;

    ~FailRegularAllocations() {
        fail_regular_allocations.store(false, std::memory_order_relaxed);
    }
};

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void committed_transition_publication_does_not_allocate() {
    constexpr std::string_view kMaxClientOrderId =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    static_assert(
        kMaxClientOrderId.size() == kNativeLiveOrderClientIdBytes
    );

    NativeLiveOrderStateCore core;
    NativeLiveOrderTransition admitted;
    NativeLiveOrderTransition activated;
    NativeLiveOrderTransition cancel_requested;
    NativeLiveOrderTransition canceled;
    NativeLiveOrderSnapshot terminal_snapshot;

    // The old transition shape allocated both its reason and its 64-byte
    // client ID after committing the side cell.  Force every regular heap
    // allocation to fail across an entire lifecycle: all authoritative
    // transition and snapshot publications must still complete.
    {
        FailRegularAllocations fail;
        admitted = core.admit(
            CanonicalSide::Buy,
            kMaxClientOrderId,
            "BTCUSDC",
            1,
            1'000,
            10,
            1'000
        );
        activated = core.confirm_new(
            CanonicalSide::Buy,
            kMaxClientOrderId,
            1,
            42,
            1'100
        );
        cancel_requested = core.request_cancel(
            CanonicalSide::Buy,
            kMaxClientOrderId,
            1,
            1'200
        );
        canceled = core.apply_exchange_update(
            CanonicalSide::Buy,
            kMaxClientOrderId,
            1,
            42,
            NativeExchangeOrderStatus::Canceled,
            0,
            1'300
        );
        terminal_snapshot = core.snapshot(CanonicalSide::Buy);
    }

    require(
        admitted.abi_version == kNativeLiveOrderResultAbiVersion &&
            admitted.order.abi_version == kNativeLiveOrderResultAbiVersion,
        "transition result carried the wrong ABI version"
    );
    require(
        admitted.reason ==
            NativeLiveOrderTransitionReason::AdmittedPendingNew &&
            activated.reason == NativeLiveOrderTransitionReason::RestAck &&
            cancel_requested.reason ==
                NativeLiveOrderTransitionReason::CancelRequested &&
            canceled.reason == NativeLiveOrderTransitionReason::CancelAck,
        "allocation-free lifecycle changed transition reason identity"
    );
    require(
        admitted.order.client_order_id.equals(kMaxClientOrderId),
        "allocation-free transition lost the maximum-length client ID"
    );
    require(
        terminal_snapshot.state == NativeLiveOrderState::Canceled &&
            terminal_snapshot.terminal,
        "allocation-free snapshot did not observe the committed terminal state"
    );
    require(
        core.telemetry().transition_count == 4,
        "allocation-free lifecycle lost transition telemetry"
    );

    // String conversion is deliberately outside the commit/publication path.
    // It may fail, but the POD result and core snapshot remain recoverable.
    bool diagnostic_allocation_failed = false;
    try {
        FailRegularAllocations fail;
        (void)admitted.order.client_order_id.str();
    } catch (const std::bad_alloc&) {
        diagnostic_allocation_failed = true;
    }
    require(
        diagnostic_allocation_failed,
        "deterministic diagnostic allocation failure did not fire"
    );
    const auto recovered = core.snapshot(CanonicalSide::Buy);
    require(
        recovered.state == NativeLiveOrderState::Canceled &&
            recovered.client_order_id.equals(kMaxClientOrderId),
        "diagnostic allocation failure hid the committed transition"
    );
}

void foreign_exchange_status_is_rejected_before_commit() {
    NativeLiveOrderStateCore core;
    const auto admitted = core.admit(
        CanonicalSide::Sell,
        "foreign-status",
        "BTCUSDC",
        1,
        1'001,
        10,
        1'000
    );
    require(
        admitted.abi_version == NativeLiveOrderStateCore::result_abi_version(),
        "public result ABI accessor disagrees with the transition"
    );
    require(
        NativeLiveOrderStateCore::snapshot_result_size_bytes() == 200 &&
            NativeLiveOrderStateCore::transition_result_size_bytes() == 216,
        "public result ABI size accessors changed"
    );

    bool rejected = false;
    try {
        (void)core.apply_exchange_update(
            CanonicalSide::Sell,
            "foreign-status",
            1,
            77,
            static_cast<NativeExchangeOrderStatus>(255),
            0,
            1'100
        );
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "foreign exchange-status ABI value was accepted");
    const auto preserved = core.snapshot(CanonicalSide::Sell);
    require(
        preserved.state == NativeLiveOrderState::PendingNew &&
            preserved.exchange_order_id == 0,
        "foreign exchange-status ABI value mutated order state"
    );
    require(
        !core.reconciliation_required(),
        "foreign exchange-status ABI validation latched reconciliation"
    );
}

template <typename Predicate>
void wait_until(Predicate&& predicate, const char* message) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error(message);
        }
        std::this_thread::yield();
    }
}

void exact_cross_side_latch_interleaving() {
    NativeLiveOrderStateCore core;
    (void)core.admit(
        CanonicalSide::Buy,
        "buy-linearization",
        "BTCUSDC",
        1,
        1'000,
        10,
        1'000
    );
    (void)core.admit(
        CanonicalSide::Sell,
        "sell-invalid",
        "BTCUSDC",
        1,
        1'001,
        10,
        1'000
    );

    // Hold BUY after it obtains the shared mutation lease but before it can
    // enter the side cell.  SELL then discovers an invariant violation.  The
    // global latch must close admission, wait for BUY's pre-existing lease,
    // and publish only after BUY has linearized.
    auto held_buy = NativeLiveOrderStateCoreTestAccess::lock_side(
        core,
        CanonicalSide::Buy
    );
    std::exception_ptr buy_error;
    std::exception_ptr latch_error;
    std::exception_ptr contender_error;
    std::atomic<bool> contender_done{false};

    std::thread buy([&]() {
        try {
            (void)core.confirm_new(
                CanonicalSide::Buy,
                "buy-linearization",
                1,
                41,
                1'100
            );
        } catch (...) {
            buy_error = std::current_exception();
        }
    });
    wait_until(
        [&]() {
            return NativeLiveOrderStateCoreTestAccess::active_leases(core) == 1;
        },
        "BUY did not acquire its mutation lease"
    );

    std::thread latch([&]() {
        try {
            // PENDING_NEW cannot be canceled.  This is the SELL-side fault.
            (void)core.request_cancel(
                CanonicalSide::Sell,
                "sell-invalid",
                1,
                1'100
            );
        } catch (...) {
            latch_error = std::current_exception();
        }
    });
    wait_until(
        [&]() {
            return NativeLiveOrderStateCoreTestAccess::gate_closed(core);
        },
        "SELL fault did not close the shared mutation gate"
    );
    require(
        !core.reconciliation_required(),
        "reconciliation published while a pre-existing mutation could commit"
    );

    std::thread contender([&]() {
        try {
            (void)core.confirm_new(
                CanonicalSide::Sell,
                "sell-invalid",
                1,
                42,
                1'200
            );
        } catch (...) {
            contender_error = std::current_exception();
        }
        contender_done.store(true, std::memory_order_release);
    });
    for (int attempt = 0; attempt < 1'000; ++attempt) {
        require(
            !contender_done.load(std::memory_order_acquire),
            "new mutation crossed a closed reconciliation gate"
        );
        std::this_thread::yield();
    }

    held_buy.unlock();
    buy.join();
    latch.join();
    contender.join();

    require(buy_error == nullptr, "pre-existing BUY mutation failed");
    require(latch_error != nullptr, "SELL invariant violation did not throw");
    require(contender_error != nullptr, "post-close mutation did not fail");
    require(core.reconciliation_required(), "reconciliation was not published");
    require(
        core.snapshot(CanonicalSide::Buy).state == NativeLiveOrderState::Open,
        "BUY state did not commit before reconciliation"
    );
    const auto sell = core.snapshot(CanonicalSide::Sell);
    require(
        sell.state == NativeLiveOrderState::PendingNew &&
            sell.exchange_order_id == 0,
        "a mutation committed after the reconciliation gate closed"
    );
    require(
        core.telemetry().reconciliation_latch_count == 1,
        "reconciliation latch was published more than once"
    );
}

void snapshot_waits_for_partial_fatal_transition_publication() {
    NativeLiveOrderStateCore core;
    (void)core.admit(
        CanonicalSide::Buy,
        "buy-partial-fatal",
        "BTCUSDC",
        1,
        1'000,
        10,
        1'000
    );
    (void)core.confirm_new(
        CanonicalSide::Buy,
        "buy-partial-fatal",
        1,
        41,
        1'100
    );
    (void)core.admit(
        CanonicalSide::Sell,
        "sell-drain",
        "BTCUSDC",
        1,
        1'001,
        10,
        1'000
    );

    // Keep one pre-existing SELL mutation lease alive so the BUY publisher
    // has a deterministic closed-gate/pre-publication interval.
    auto held_sell = NativeLiveOrderStateCoreTestAccess::lock_side(
        core,
        CanonicalSide::Sell
    );
    std::exception_ptr sell_error;
    std::exception_ptr fatal_error;
    std::thread sell([&]() {
        try {
            (void)core.confirm_new(
                CanonicalSide::Sell,
                "sell-drain",
                1,
                42,
                1'100
            );
        } catch (...) {
            sell_error = std::current_exception();
        }
    });
    wait_until(
        [&]() {
            return NativeLiveOrderStateCoreTestAccess::active_leases(core) == 1;
        },
        "SELL did not acquire its mutation lease"
    );

    std::thread fatal([&]() {
        try {
            // The cumulative quantity is committed for diagnosis, but FILLED
            // with only 4/10 lots is fatal and requires reconciliation.
            (void)core.apply_exchange_update(
                CanonicalSide::Buy,
                "buy-partial-fatal",
                1,
                41,
                NativeExchangeOrderStatus::Filled,
                4,
                1'200
            );
        } catch (...) {
            fatal_error = std::current_exception();
        }
    });
    wait_until(
        [&]() {
            return NativeLiveOrderStateCoreTestAccess::gate_closed(core);
        },
        "fatal BUY update did not close the mutation gate"
    );
    require(
        !core.reconciliation_required(),
        "fatal latch published before the pre-existing SELL lease drained"
    );

    NativeLiveOrderSnapshot observed;
    std::exception_ptr snapshot_error;
    std::atomic<bool> snapshot_done{false};
    std::thread reader([&]() {
        try {
            observed = core.snapshot(CanonicalSide::Buy);
        } catch (...) {
            snapshot_error = std::current_exception();
        }
        snapshot_done.store(true, std::memory_order_release);
    });
    for (int attempt = 0; attempt < 10'000; ++attempt) {
        require(
            !snapshot_done.load(std::memory_order_acquire),
            "snapshot exposed a partial fatal mutation before latch publication"
        );
        std::this_thread::yield();
    }

    held_sell.unlock();
    sell.join();
    fatal.join();
    reader.join();
    require(sell_error == nullptr, "pre-existing SELL mutation failed");
    require(fatal_error != nullptr, "incomplete FILLED update did not fail");
    require(snapshot_error == nullptr, "post-publication snapshot failed");
    require(core.reconciliation_required(), "fatal latch was not published");
    require(
        observed.state == NativeLiveOrderState::PartiallyFilled &&
            observed.filled_lots == 4,
        "post-publication snapshot lost the diagnostic partial fill"
    );
}

CanonicalCancelAllIntent safety_cancel_all() {
    CanonicalCancelAllIntent intent;
    intent.request_id = "safety-cancel-all";
    intent.decision_id = "safety-decision";
    intent.symbol = "BTCUSDC";
    intent.reason = "risk_stop";
    intent.expected_ownership_generation = 7;
    return intent;
}

CanonicalOrderIntent normal_place() {
    CanonicalOrderIntent intent;
    intent.request_id = "normal-place";
    intent.decision_id = "normal-decision";
    intent.client_order_id = "normal-cid";
    intent.symbol = "BTCUSDC";
    intent.side = CanonicalSide::Buy;
    intent.order_type = CanonicalOrderType::Limit;
    intent.time_in_force = CanonicalTimeInForce::Gtx;
    intent.price = 100'000.1;
    intent.quantity = 0.001;
    intent.post_only = true;
    intent.deadline_time_ns = 10'000;
    intent.expected_ownership_generation = 7;
    return intent;
}

void transport_intents_reject_foreign_identity_and_enum_values() {
    {
        auto intent = normal_place();
        intent.abi_version = kTransportContractAbiVersion + 1;
        require(
            !intent.is_structurally_valid(),
            "order intent accepted a foreign ABI version"
        );
    }
    {
        auto intent = normal_place();
        intent.product = static_cast<TransportProduct>(255);
        require(
            !intent.is_structurally_valid(),
            "order intent accepted a foreign product"
        );
    }
    {
        auto intent = normal_place();
        intent.side = static_cast<CanonicalSide>(255);
        require(
            !intent.is_structurally_valid(),
            "order intent accepted an out-of-range side"
        );
    }
    {
        auto intent = normal_place();
        intent.order_type = static_cast<CanonicalOrderType>(255);
        require(
            !intent.is_structurally_valid(),
            "order intent accepted an out-of-range order type"
        );
    }
    {
        auto intent = normal_place();
        intent.time_in_force = static_cast<CanonicalTimeInForce>(255);
        require(
            !intent.is_structurally_valid(),
            "order intent accepted an out-of-range time in force"
        );
    }

    CanonicalCancelIntent cancel;
    cancel.request_id = "cancel-invalid-identity";
    cancel.client_order_id = "cancel-invalid-cid";
    cancel.symbol = "BTCUSDC";
    cancel.abi_version = kTransportContractAbiVersion + 1;
    require(
        !cancel.is_structurally_valid(),
        "cancel intent accepted a foreign ABI version"
    );
    cancel.abi_version = kTransportContractAbiVersion;
    cancel.product = static_cast<TransportProduct>(255);
    require(
        !cancel.is_structurally_valid(),
        "cancel intent accepted a foreign product"
    );

    auto cancel_all = safety_cancel_all();
    cancel_all.abi_version = kTransportContractAbiVersion + 1;
    require(
        !cancel_all.is_structurally_valid(),
        "cancel-all intent accepted a foreign ABI version"
    );
    cancel_all.abi_version = kTransportContractAbiVersion;
    cancel_all.product = static_cast<TransportProduct>(255);
    require(
        !cancel_all.is_structurally_valid(),
        "cancel-all intent accepted a foreign product"
    );
}

void gateway_empty_poll_and_success_path_do_not_allocate() {
    NativeUsdMOrderGatewayCore core(TransportBackendKind::CppUsdmRest);
    for (std::uint64_t now = 1; now <= 1'000; ++now) {
        const auto empty = core.begin_next(now, 1, 7);
        require(!empty.request.has_value(), "empty gateway poll returned work");
        require(
            empty.invalidations.capacity() == 0,
            "empty gateway poll allocated an invalidation buffer"
        );
    }
    const auto enqueued = core.enqueue_order(normal_place(), 1'000, 1'100);
    require(enqueued.admitted, "normal place was not admitted");
    const auto ready = core.begin_next(1'200, 1, 7);
    require(ready.request.has_value(), "normal place was not dequeued");
    require(
        ready.invalidations.capacity() == 0,
        "successful gateway dequeue allocated an invalidation buffer"
    );
}

void gateway_rejects_overlapping_consumers() {
    NativeUsdMOrderGatewayCore core(TransportBackendKind::CppUsdmRest);
    NativeUsdMOrderGatewayCoreTestAccess::lock_consumer(core);
    bool rejected = false;
    try {
        (void)core.begin_next(1, 1, 7);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    NativeUsdMOrderGatewayCoreTestAccess::unlock_consumer(core);
    require(rejected, "gateway did not reject an overlapping consumer");
    require(
        !core.begin_next(2, 1, 7).request.has_value(),
        "gateway consumer lock did not recover after overlap rejection"
    );
}

void gateway_release_serializes_with_producer_admission() {
    NativeUsdMOrderGatewayCore core(TransportBackendKind::CppUsdmRest);
    const auto cancel = core.enqueue_cancel_all(
        safety_cancel_all(),
        1'000,
        1'100
    );
    require(cancel.admitted, "safety cancel-all was not admitted");
    const auto dequeued = core.begin_next(1'200, 1, 7);
    require(dequeued.request.has_value(), "safety cancel-all was not dequeued");
    (void)core.mark_confirmed_not_dispatched(1'300, "test_terminal");
    require(core.safety_barrier_latched(), "safety barrier was not latched");

    // This represents a producer that has entered enqueue_impl but has not
    // yet completed its final barrier/queue publication checks.  A release
    // must not observe an empty queue and clear around that producer.
    NativeUsdMOrderGatewayCoreTestAccess::lock_producer(core);
    std::atomic<bool> release_entered{false};
    std::atomic<bool> release_done{false};
    std::exception_ptr release_error;
    std::thread releaser([&]() {
        release_entered.store(true, std::memory_order_release);
        try {
            core.release_safety_barrier(1, true);
        } catch (...) {
            release_error = std::current_exception();
        }
        release_done.store(true, std::memory_order_release);
    });
    wait_until(
        [&]() { return release_entered.load(std::memory_order_acquire); },
        "safety release thread did not start"
    );
    for (int attempt = 0; attempt < 10'000; ++attempt) {
        require(
            !release_done.load(std::memory_order_acquire),
            "safety release did not serialize with producer admission"
        );
        require(
            core.safety_barrier_latched(),
            "safety barrier cleared around an in-flight producer"
        );
        std::this_thread::yield();
    }
    NativeUsdMOrderGatewayCoreTestAccess::unlock_producer(core);
    releaser.join();
    require(release_error == nullptr, "serialized safety release failed");
    require(release_done.load(std::memory_order_acquire), "release did not finish");
    require(!core.safety_barrier_latched(), "safety barrier did not clear");
}

void gateway_safety_latch_invalidates_place_observed_before_latch() {
    NativeUsdMOrderGatewayCore core(TransportBackendKind::CppUsdmRest);
    const auto place = core.enqueue_order(normal_place(), 1'000, 1'100);
    require(place.admitted, "normal place was not admitted");

    std::atomic<bool> place_peeked{false};
    std::atomic<bool> resume_consumer{false};
    NativeUsdMOrderGatewayCoreTestAccess::pause_begin_next_after_place_peek(
        core,
        place_peeked,
        resume_consumer
    );
    NativeGatewayDequeueResult place_result;
    std::exception_ptr consumer_error;
    std::thread consumer([&]() {
        try {
            place_result = core.begin_next(1'300, 1, 7);
        } catch (...) {
            consumer_error = std::current_exception();
        }
    });
    wait_until(
        [&]() { return place_peeked.load(std::memory_order_acquire); },
        "consumer did not pause after observing PLACE"
    );

    // The producer latches and publishes a safety CANCEL after begin_next has
    // observed the old PLACE but before its epoch check/pop. The PLACE must be
    // invalidated rather than becoming the active wire request.
    const auto cancel = core.enqueue_cancel_all(
        safety_cancel_all(),
        1'150,
        1'200
    );
    require(cancel.admitted, "concurrent safety cancel-all was not admitted");
    require(core.safety_barrier_latched(), "concurrent safety latch was not visible");
    resume_consumer.store(true, std::memory_order_release);
    consumer.join();
    NativeUsdMOrderGatewayCoreTestAccess::clear_begin_next_place_pause(core);

    require(consumer_error == nullptr, "consumer failed during safety race");
    require(
        place_result.request.has_value() &&
            place_result.request->operation == NativeGatewayOperation::CancelAll,
        "PLACE became active instead of the safety cancel-all after a latch"
    );
    require(
        place_result.invalidations.size() == 1 &&
            place_result.invalidations.front().reason.str() ==
                "invalidated_by_safety_barrier",
        "PLACE was not diagnosed as safety-barrier invalidated"
    );
    require(core.has_active_request(), "safety cancel-all did not become active");
}

void concurrent_valid_side_stress() {
    NativeLiveOrderStateCore core;
    constexpr std::uint64_t kIterations = 4'000;
    std::exception_ptr buy_error;
    std::exception_ptr sell_error;

    const auto run_side = [&](CanonicalSide side, std::exception_ptr& error) {
        try {
            const std::string prefix =
                side == CanonicalSide::Buy ? "buy-" : "sell-";
            const auto price = side == CanonicalSide::Buy ? 1'000 : 1'001;
            const auto exchange_base =
                side == CanonicalSide::Buy ? 1'000'000ULL : 2'000'000ULL;
            for (std::uint64_t generation = 1;
                 generation <= kIterations;
                 ++generation) {
                const auto client_id = prefix + std::to_string(generation);
                const auto timestamp = generation * 10 + 1;
                (void)core.admit(
                    side,
                    client_id,
                    "BTCUSDC",
                    generation,
                    price,
                    10,
                    timestamp
                );
                (void)core.confirm_new(
                    side,
                    client_id,
                    generation,
                    exchange_base + generation,
                    timestamp + 1
                );
                (void)core.request_cancel(
                    side,
                    client_id,
                    generation,
                    timestamp + 2
                );
                (void)core.apply_exchange_update(
                    side,
                    client_id,
                    generation,
                    exchange_base + generation,
                    NativeExchangeOrderStatus::Canceled,
                    0,
                    timestamp + 3
                );
            }
        } catch (...) {
            error = std::current_exception();
        }
    };

    std::thread buy(run_side, CanonicalSide::Buy, std::ref(buy_error));
    std::thread sell(run_side, CanonicalSide::Sell, std::ref(sell_error));
    buy.join();
    sell.join();

    require(buy_error == nullptr, "concurrent BUY lifecycle failed");
    require(sell_error == nullptr, "concurrent SELL lifecycle failed");
    require(!core.reconciliation_required(), "valid concurrency latched fatal state");
    require(
        core.snapshot(CanonicalSide::Buy).state == NativeLiveOrderState::Canceled,
        "BUY stress lifecycle did not finish terminal"
    );
    require(
        core.snapshot(CanonicalSide::Sell).state == NativeLiveOrderState::Canceled,
        "SELL stress lifecycle did not finish terminal"
    );
    const auto telemetry = core.telemetry();
    require(
        telemetry.admitted_count == kIterations * 2,
        "concurrent admissions were lost"
    );
    require(
        telemetry.transition_count == kIterations * 8,
        "concurrent transitions were lost"
    );
}

}  // namespace
}  // namespace narrowgate_cpp

int main() {
    try {
        narrowgate_cpp::committed_transition_publication_does_not_allocate();
        narrowgate_cpp::foreign_exchange_status_is_rejected_before_commit();
        narrowgate_cpp::exact_cross_side_latch_interleaving();
        narrowgate_cpp::snapshot_waits_for_partial_fatal_transition_publication();
        narrowgate_cpp::concurrent_valid_side_stress();
        narrowgate_cpp::transport_intents_reject_foreign_identity_and_enum_values();
        narrowgate_cpp::gateway_empty_poll_and_success_path_do_not_allocate();
        narrowgate_cpp::gateway_rejects_overlapping_consumers();
        narrowgate_cpp::gateway_release_serializes_with_producer_admission();
        narrowgate_cpp::gateway_safety_latch_invalidates_place_observed_before_latch();
    } catch (const std::exception& error) {
        std::cerr << "live_order_state concurrency selftest failed: "
                  << error.what() << '\n';
        return 1;
    }
    std::cout << "live_order_state concurrency selftest passed\n";
    return 0;
}
