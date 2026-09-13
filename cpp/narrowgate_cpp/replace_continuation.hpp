#pragma once

#include "common.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace narrowgate_cpp {

// Keep independently-mutated BUY/SELL state on different cache lines. The
// exact isolation span comes from common.hpp so arm64 macOS and amd64 Linux
// artifacts use their own frozen, testable layout.
inline constexpr std::size_t kNativeCacheLineBytes =
    kDestructiveInterferenceBytes;
inline constexpr std::size_t kReplaceContinuationMaxClientOrderIdBytes = 64;

enum class ReplaceContinuationPhase : std::uint8_t {
    Empty = 0,
    Armed = 1,
    Ready = 2,
    InFlight = 3,
};

enum class ReplaceContinuationEventKind : std::uint8_t {
    Arm = 0,
    Publish = 1,
    Decision = 2,
    Drop = 3,
};

struct ReplaceContinuationIntent {
    Side side = Side::Buy;
    std::string client_order_id;
    std::uint64_t generation = 0;
    std::int64_t armed_ts_ns = 0;
    std::int64_t terminal_visible_ts_ns = 0;
};

struct ReplaceContinuationEvent {
    ReplaceContinuationEventKind kind = ReplaceContinuationEventKind::Arm;
    std::uint64_t sequence = 0;
    Side side = Side::Buy;
    std::uint64_t generation = 0;
    std::string client_order_id;
    std::int64_t armed_ts_ns = 0;
    std::int64_t terminal_visible_ts_ns = 0;
    std::int64_t decision_start_ts_ns = 0;
    std::int64_t decision_latency_ns = 0;
    std::string reason;
};

struct ReplaceContinuationTransition {
    bool accepted = false;
    std::uint64_t generation = 0;
    std::vector<ReplaceContinuationEvent> events;
};

struct ReplaceContinuationSideSnapshot {
    Side side = Side::Buy;
    std::uint64_t generation_counter = 0;
    ReplaceContinuationPhase pending_phase = ReplaceContinuationPhase::Empty;
    std::optional<ReplaceContinuationIntent> pending;
    std::optional<ReplaceContinuationIntent> in_flight;
};

struct ReplaceContinuationTelemetry {
    std::uint64_t arm_count = 0;
    std::uint64_t publish_count = 0;
    std::uint64_t decision_count = 0;
    std::uint64_t drop_count = 0;
    std::uint64_t buy_decision_count = 0;
    std::uint64_t sell_decision_count = 0;
    std::uint64_t decision_latency_sum_ns = 0;
    std::uint64_t decision_latency_max_ns = 0;
    std::uint64_t pending_count = 0;
    std::uint64_t in_flight_count = 0;
    std::uint64_t event_sequence = 0;
};

// Native state for the exact replacement-terminal continuation lifecycle:
//
//   arm -> authoritative cancel terminal -> take -> decision/drop
//
// The class deliberately does not submit orders or invoke a callback.  It is
// a bounded fixed-storage state owner; public snapshots/events may allocate
// after the transition has been prepared, but no fallible output construction
// is allowed after state commit. The caller remains responsible for waking the
// one decision executor after an accepted publish transition.
class NativeReplaceContinuationState {
public:
    explicit NativeReplaceContinuationState(bool enabled = true) noexcept
        : enabled_(enabled) {}

    NativeReplaceContinuationState(const NativeReplaceContinuationState&) = delete;
    NativeReplaceContinuationState& operator=(
        const NativeReplaceContinuationState&
    ) = delete;

    [[nodiscard]] bool enabled() const noexcept { return enabled_; }

    [[nodiscard]] ReplaceContinuationTransition arm(
        Side side,
        std::string_view client_order_id,
        std::int64_t armed_ts_ns,
        bool can_post = true
    );

    [[nodiscard]] ReplaceContinuationTransition publish(
        Side side,
        std::string_view client_order_id,
        std::uint64_t generation,
        std::int64_t terminal_visible_ts_ns
    );

    [[nodiscard]] ReplaceContinuationTransition clear_exact(
        Side side,
        std::string_view client_order_id,
        std::uint64_t generation = 0,
        std::int64_t event_ts_ns = 0,
        std::string_view reason = "cleared"
    );

    [[nodiscard]] ReplaceContinuationTransition clear_side(
        Side side,
        std::string_view reason = "side_superseded"
    );

    [[nodiscard]] ReplaceContinuationTransition clear_unready(
        Side side,
        std::string_view client_order_id,
        std::uint64_t generation,
        std::string_view reason = "terminal_before_callback"
    );

    [[nodiscard]] std::vector<ReplaceContinuationIntent> take_ready();

    [[nodiscard]] ReplaceContinuationTransition finalize_decision(
        Side side,
        std::uint64_t generation,
        std::int64_t decision_start_ts_ns
    );

    [[nodiscard]] ReplaceContinuationTransition drop_in_flight(
        Side side,
        std::uint64_t generation,
        std::string_view reason
    );

    [[nodiscard]] std::vector<ReplaceContinuationEvent> clear_all(
        std::string_view reason = "clear_all"
    );

    [[nodiscard]] ReplaceContinuationSideSnapshot side_snapshot(Side side) const;
    [[nodiscard]] ReplaceContinuationTelemetry telemetry() const;

    static constexpr std::size_t cache_line_bytes() noexcept {
        return kNativeCacheLineBytes;
    }
    static constexpr std::size_t max_client_order_id_bytes() noexcept {
        return kReplaceContinuationMaxClientOrderIdBytes;
    }

private:
    struct FixedClientOrderId {
        std::array<char, kReplaceContinuationMaxClientOrderIdBytes> bytes{};
        std::uint8_t size = 0;

        void assign(std::string_view value);
        [[nodiscard]] bool equals(std::string_view value) const noexcept;
        [[nodiscard]] std::string str() const;
        void clear() noexcept;
    };

    struct IntentState {
        FixedClientOrderId client_order_id;
        std::uint64_t generation = 0;
        std::int64_t armed_ts_ns = 0;
        std::int64_t terminal_visible_ts_ns = 0;

        void clear() noexcept;
    };

    struct alignas(kNativeCacheLineBytes) SideCell {
        mutable std::mutex mutex;
        std::uint64_t generation_counter = 0;
        ReplaceContinuationPhase pending_phase = ReplaceContinuationPhase::Empty;
        IntentState pending;
        bool in_flight_active = false;
        IntentState in_flight;
    };

    static_assert(alignof(SideCell) >= kNativeCacheLineBytes);
    static_assert(sizeof(SideCell) % kNativeCacheLineBytes == 0);

    struct alignas(kNativeCacheLineBytes) AtomicTelemetry {
        std::atomic<std::uint64_t> arm_count{0};
        std::atomic<std::uint64_t> publish_count{0};
        std::atomic<std::uint64_t> decision_count{0};
        std::atomic<std::uint64_t> drop_count{0};
        std::atomic<std::uint64_t> buy_decision_count{0};
        std::atomic<std::uint64_t> sell_decision_count{0};
        std::atomic<std::uint64_t> decision_latency_sum_ns{0};
        std::atomic<std::uint64_t> decision_latency_max_ns{0};
    };

    template <Side S>
    [[nodiscard]] SideCell& cell() noexcept {
        if constexpr (S == Side::Buy) {
            return buy_;
        } else {
            return sell_;
        }
    }

    template <Side S>
    [[nodiscard]] const SideCell& cell() const noexcept {
        if constexpr (S == Side::Buy) {
            return buy_;
        } else {
            return sell_;
        }
    }

    template <Side S>
    [[nodiscard]] ReplaceContinuationTransition arm_side(
        std::string_view client_order_id,
        std::int64_t armed_ts_ns,
        bool can_post
    );

    template <Side S>
    [[nodiscard]] ReplaceContinuationTransition publish_side(
        std::string_view client_order_id,
        std::uint64_t generation,
        std::int64_t terminal_visible_ts_ns
    );

    template <Side S>
    [[nodiscard]] ReplaceContinuationTransition clear_exact_side(
        std::string_view client_order_id,
        std::uint64_t generation,
        std::int64_t event_ts_ns,
        std::string_view reason,
        bool require_unready
    );

    template <Side S>
    [[nodiscard]] ReplaceContinuationTransition clear_side_impl(
        std::string_view reason
    );

    template <Side S>
    void take_ready_side(std::vector<ReplaceContinuationIntent>& out);

    template <Side S>
    [[nodiscard]] ReplaceContinuationTransition finalize_side(
        std::uint64_t generation,
        std::int64_t decision_start_ts_ns,
        bool decision,
        std::string_view reason
    );

    template <Side S>
    [[nodiscard]] ReplaceContinuationSideSnapshot side_snapshot_impl() const;

    template <Side S>
    [[nodiscard]] static ReplaceContinuationIntent snapshot_intent(
        const IntentState& intent
    );

    template <Side S>
    [[nodiscard]] static ReplaceContinuationEvent event_from_intent(
        ReplaceContinuationEventKind kind,
        const IntentState& intent,
        std::int64_t decision_start_ts_ns = 0,
        std::string_view reason = {}
    );

    void commit_events(std::vector<ReplaceContinuationEvent>& events) noexcept;
    void record_decision_latency(Side side, std::uint64_t latency_ns) noexcept;

    bool enabled_ = true;
    SideCell buy_;
    SideCell sell_;
    alignas(kNativeCacheLineBytes) std::atomic<std::uint64_t> event_sequence_{0};
    AtomicTelemetry telemetry_;
};

}  // namespace narrowgate_cpp
