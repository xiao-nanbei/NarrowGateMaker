#include "replace_continuation.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace narrowgate_cpp {

void NativeReplaceContinuationState::FixedClientOrderId::assign(
    std::string_view value
) {
    if (value.empty()) {
        throw std::invalid_argument("replacement continuation CID cannot be empty");
    }
    if (value.size() > bytes.size()) {
        throw std::invalid_argument(
            "replacement continuation CID exceeds fixed native capacity"
        );
    }
    std::copy(value.begin(), value.end(), bytes.begin());
    size = static_cast<std::uint8_t>(value.size());
}

bool NativeReplaceContinuationState::FixedClientOrderId::equals(
    std::string_view value
) const noexcept {
    return value.size() == size &&
        std::equal(value.begin(), value.end(), bytes.begin());
}

std::string NativeReplaceContinuationState::FixedClientOrderId::str() const {
    return std::string(bytes.data(), size);
}

void NativeReplaceContinuationState::FixedClientOrderId::clear() noexcept {
    size = 0;
}

void NativeReplaceContinuationState::IntentState::clear() noexcept {
    client_order_id.clear();
    generation = 0;
    armed_ts_ns = 0;
    terminal_visible_ts_ns = 0;
}

template <Side S>
ReplaceContinuationIntent NativeReplaceContinuationState::snapshot_intent(
    const IntentState& intent
) {
    return ReplaceContinuationIntent{
        .side = S,
        .client_order_id = intent.client_order_id.str(),
        .generation = intent.generation,
        .armed_ts_ns = intent.armed_ts_ns,
        .terminal_visible_ts_ns = intent.terminal_visible_ts_ns,
    };
}

template <Side S>
ReplaceContinuationEvent NativeReplaceContinuationState::event_from_intent(
    ReplaceContinuationEventKind kind,
    const IntentState& intent,
    std::int64_t decision_start_ts_ns,
    std::string_view reason
) {
    const auto latency = kind == ReplaceContinuationEventKind::Decision
        ? std::max<std::int64_t>(
            0,
            decision_start_ts_ns - intent.terminal_visible_ts_ns
        )
        : 0;
    return ReplaceContinuationEvent{
        .kind = kind,
        .sequence = 0,
        .side = S,
        .generation = intent.generation,
        .client_order_id = intent.client_order_id.str(),
        .armed_ts_ns = intent.armed_ts_ns,
        .terminal_visible_ts_ns = intent.terminal_visible_ts_ns,
        .decision_start_ts_ns = decision_start_ts_ns,
        .decision_latency_ns = latency,
        .reason = std::string(reason.empty() ? "none" : reason),
    };
}

void NativeReplaceContinuationState::record_decision_latency(
    Side side,
    std::uint64_t latency_ns
) noexcept {
    telemetry_.decision_latency_sum_ns.fetch_add(
        latency_ns,
        std::memory_order_relaxed
    );
    auto observed = telemetry_.decision_latency_max_ns.load(
        std::memory_order_relaxed
    );
    while (
        observed < latency_ns &&
        !telemetry_.decision_latency_max_ns.compare_exchange_weak(
            observed,
            latency_ns,
            std::memory_order_relaxed,
            std::memory_order_relaxed
        )
    ) {}
    auto& counter = side == Side::Buy
        ? telemetry_.buy_decision_count
        : telemetry_.sell_decision_count;
    counter.fetch_add(1, std::memory_order_relaxed);
}

void NativeReplaceContinuationState::commit_events(
    std::vector<ReplaceContinuationEvent>& events
) noexcept {
    if (events.empty()) {
        return;
    }
    const auto first_sequence = event_sequence_.fetch_add(
        events.size(),
        std::memory_order_relaxed
    ) + 1;
    for (std::size_t index = 0; index < events.size(); ++index) {
        auto& event = events[index];
        event.sequence = first_sequence + index;
        switch (event.kind) {
            case ReplaceContinuationEventKind::Arm:
                telemetry_.arm_count.fetch_add(1, std::memory_order_relaxed);
                break;
            case ReplaceContinuationEventKind::Publish:
                telemetry_.publish_count.fetch_add(1, std::memory_order_relaxed);
                break;
            case ReplaceContinuationEventKind::Decision:
                telemetry_.decision_count.fetch_add(1, std::memory_order_relaxed);
                record_decision_latency(
                    event.side,
                    static_cast<std::uint64_t>(event.decision_latency_ns)
                );
                break;
            case ReplaceContinuationEventKind::Drop:
                telemetry_.drop_count.fetch_add(1, std::memory_order_relaxed);
                break;
        }
    }
}

template <Side S>
ReplaceContinuationTransition NativeReplaceContinuationState::arm_side(
    std::string_view client_order_id,
    std::int64_t armed_ts_ns,
    bool can_post
) {
    ReplaceContinuationTransition out;
    if (!enabled_ || !can_post) {
        return out;
    }
    if (armed_ts_ns <= 0) {
        throw std::invalid_argument(
            "replacement continuation arm timestamp must be positive"
        );
    }
    auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    if (side_cell.generation_counter ==
        std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error(
            "replacement continuation generation exhausted"
        );
    }
    IntentState next_pending;
    next_pending.client_order_id.assign(client_order_id);
    next_pending.generation = side_cell.generation_counter + 1;
    next_pending.armed_ts_ns = armed_ts_ns;
    next_pending.terminal_visible_ts_ns = 0;
    out.events.reserve(2);
    if (side_cell.pending_phase != ReplaceContinuationPhase::Empty) {
        out.events.push_back(event_from_intent<S>(
            ReplaceContinuationEventKind::Drop,
            side_cell.pending,
            0,
            "superseded_by_new_arm"
        ));
    }
    out.events.push_back(event_from_intent<S>(
        ReplaceContinuationEventKind::Arm,
        next_pending
    ));
    const auto generation = next_pending.generation;
    side_cell.generation_counter = generation;
    side_cell.pending = next_pending;
    side_cell.pending_phase = ReplaceContinuationPhase::Armed;
    out.accepted = true;
    out.generation = generation;
    commit_events(out.events);
    return out;
}

ReplaceContinuationTransition NativeReplaceContinuationState::arm(
    Side side,
    std::string_view client_order_id,
    std::int64_t armed_ts_ns,
    bool can_post
) {
    return side == Side::Buy
        ? arm_side<Side::Buy>(client_order_id, armed_ts_ns, can_post)
        : arm_side<Side::Sell>(client_order_id, armed_ts_ns, can_post);
}

template <Side S>
ReplaceContinuationTransition NativeReplaceContinuationState::publish_side(
    std::string_view client_order_id,
    std::uint64_t generation,
    std::int64_t terminal_visible_ts_ns
) {
    ReplaceContinuationTransition out;
    if (!enabled_) {
        return out;
    }
    auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    if (
        side_cell.pending_phase != ReplaceContinuationPhase::Armed ||
        !side_cell.pending.client_order_id.equals(client_order_id) ||
        (generation > 0 && side_cell.pending.generation != generation) ||
        terminal_visible_ts_ns < side_cell.pending.armed_ts_ns
    ) {
        return out;
    }
    IntentState ready = side_cell.pending;
    ready.terminal_visible_ts_ns = terminal_visible_ts_ns;
    out.events.reserve(1);
    out.events.push_back(event_from_intent<S>(
        ReplaceContinuationEventKind::Publish,
        ready
    ));
    side_cell.pending = ready;
    side_cell.pending_phase = ReplaceContinuationPhase::Ready;
    out.accepted = true;
    out.generation = side_cell.pending.generation;
    commit_events(out.events);
    return out;
}

ReplaceContinuationTransition NativeReplaceContinuationState::publish(
    Side side,
    std::string_view client_order_id,
    std::uint64_t generation,
    std::int64_t terminal_visible_ts_ns
) {
    return side == Side::Buy
        ? publish_side<Side::Buy>(
            client_order_id,
            generation,
            terminal_visible_ts_ns
        )
        : publish_side<Side::Sell>(
            client_order_id,
            generation,
            terminal_visible_ts_ns
        );
}

template <Side S>
ReplaceContinuationTransition NativeReplaceContinuationState::clear_exact_side(
    std::string_view client_order_id,
    std::uint64_t generation,
    std::int64_t event_ts_ns,
    std::string_view reason,
    bool require_unready
) {
    ReplaceContinuationTransition out;
    auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    if (
        side_cell.pending_phase == ReplaceContinuationPhase::Empty ||
        !side_cell.pending.client_order_id.equals(client_order_id) ||
        (require_unready && generation == 0) ||
        (generation > 0 && side_cell.pending.generation != generation) ||
        (event_ts_ns > 0 && event_ts_ns < side_cell.pending.armed_ts_ns) ||
        (require_unready &&
         side_cell.pending_phase != ReplaceContinuationPhase::Armed)
    ) {
        return out;
    }
    out.generation = side_cell.pending.generation;
    out.events.push_back(event_from_intent<S>(
        ReplaceContinuationEventKind::Drop,
        side_cell.pending,
        0,
        reason
    ));
    side_cell.pending.clear();
    side_cell.pending_phase = ReplaceContinuationPhase::Empty;
    out.accepted = true;
    commit_events(out.events);
    return out;
}

ReplaceContinuationTransition NativeReplaceContinuationState::clear_exact(
    Side side,
    std::string_view client_order_id,
    std::uint64_t generation,
    std::int64_t event_ts_ns,
    std::string_view reason
) {
    return side == Side::Buy
        ? clear_exact_side<Side::Buy>(
            client_order_id,
            generation,
            event_ts_ns,
            reason,
            false
        )
        : clear_exact_side<Side::Sell>(
            client_order_id,
            generation,
            event_ts_ns,
            reason,
            false
        );
}

template <Side S>
ReplaceContinuationTransition NativeReplaceContinuationState::clear_side_impl(
    std::string_view reason
) {
    ReplaceContinuationTransition out;
    auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    if (side_cell.pending_phase == ReplaceContinuationPhase::Empty) {
        return out;
    }
    out.generation = side_cell.pending.generation;
    out.events.push_back(event_from_intent<S>(
        ReplaceContinuationEventKind::Drop,
        side_cell.pending,
        0,
        reason
    ));
    side_cell.pending.clear();
    side_cell.pending_phase = ReplaceContinuationPhase::Empty;
    out.accepted = true;
    commit_events(out.events);
    return out;
}

ReplaceContinuationTransition NativeReplaceContinuationState::clear_side(
    Side side,
    std::string_view reason
) {
    return side == Side::Buy
        ? clear_side_impl<Side::Buy>(reason)
        : clear_side_impl<Side::Sell>(reason);
}

ReplaceContinuationTransition NativeReplaceContinuationState::clear_unready(
    Side side,
    std::string_view client_order_id,
    std::uint64_t generation,
    std::string_view reason
) {
    return side == Side::Buy
        ? clear_exact_side<Side::Buy>(
            client_order_id,
            generation,
            0,
            reason,
            true
        )
        : clear_exact_side<Side::Sell>(
            client_order_id,
            generation,
            0,
            reason,
            true
        );
}

template <Side S>
void NativeReplaceContinuationState::take_ready_side(
    std::vector<ReplaceContinuationIntent>& out
) {
    auto& side_cell = cell<S>();
    if (side_cell.pending_phase != ReplaceContinuationPhase::Ready) {
        return;
    }
    if (side_cell.in_flight_active) {
        // Preserve the ready intent rather than overwrite unresolved state.
        return;
    }
    auto snapshot = snapshot_intent<S>(side_cell.pending);
    // The caller reserves two entries before locking both sides. Push the
    // potentially allocating public snapshot before changing ownership.
    out.push_back(std::move(snapshot));
    side_cell.in_flight = side_cell.pending;
    side_cell.in_flight_active = true;
    side_cell.pending.clear();
    side_cell.pending_phase = ReplaceContinuationPhase::Empty;
}

std::vector<ReplaceContinuationIntent>
NativeReplaceContinuationState::take_ready() {
    if (!enabled_) {
        return {};
    }
    std::vector<ReplaceContinuationIntent> out;
    out.reserve(2);
    const std::scoped_lock lock(buy_.mutex, sell_.mutex);
    take_ready_side<Side::Buy>(out);
    take_ready_side<Side::Sell>(out);
    return out;
}

template <Side S>
ReplaceContinuationTransition NativeReplaceContinuationState::finalize_side(
    std::uint64_t generation,
    std::int64_t decision_start_ts_ns,
    bool decision,
    std::string_view reason
) {
    ReplaceContinuationTransition out;
    auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    if (
        !side_cell.in_flight_active ||
        side_cell.in_flight.generation != generation
    ) {
        return out;
    }
    out.generation = generation;
    out.events.push_back(event_from_intent<S>(
        decision
            ? ReplaceContinuationEventKind::Decision
            : ReplaceContinuationEventKind::Drop,
        side_cell.in_flight,
        decision ? decision_start_ts_ns : 0,
        decision ? "none" : reason
    ));
    side_cell.in_flight.clear();
    side_cell.in_flight_active = false;
    out.accepted = true;
    commit_events(out.events);
    return out;
}

ReplaceContinuationTransition NativeReplaceContinuationState::finalize_decision(
    Side side,
    std::uint64_t generation,
    std::int64_t decision_start_ts_ns
) {
    return side == Side::Buy
        ? finalize_side<Side::Buy>(
            generation,
            decision_start_ts_ns,
            true,
            "none"
        )
        : finalize_side<Side::Sell>(
            generation,
            decision_start_ts_ns,
            true,
            "none"
        );
}

ReplaceContinuationTransition NativeReplaceContinuationState::drop_in_flight(
    Side side,
    std::uint64_t generation,
    std::string_view reason
) {
    return side == Side::Buy
        ? finalize_side<Side::Buy>(generation, 0, false, reason)
        : finalize_side<Side::Sell>(generation, 0, false, reason);
}

std::vector<ReplaceContinuationEvent>
NativeReplaceContinuationState::clear_all(std::string_view reason) {
    std::vector<ReplaceContinuationEvent> events;
    events.reserve(2);
    const std::scoped_lock lock(buy_.mutex, sell_.mutex);
    if (buy_.pending_phase != ReplaceContinuationPhase::Empty) {
        events.push_back(event_from_intent<Side::Buy>(
            ReplaceContinuationEventKind::Drop,
            buy_.pending,
            0,
            reason
        ));
        buy_.pending.clear();
        buy_.pending_phase = ReplaceContinuationPhase::Empty;
    }
    if (sell_.pending_phase != ReplaceContinuationPhase::Empty) {
        events.push_back(event_from_intent<Side::Sell>(
            ReplaceContinuationEventKind::Drop,
            sell_.pending,
            0,
            reason
        ));
        sell_.pending.clear();
        sell_.pending_phase = ReplaceContinuationPhase::Empty;
    }
    commit_events(events);
    return events;
}

template <Side S>
ReplaceContinuationSideSnapshot
NativeReplaceContinuationState::side_snapshot_impl() const {
    const auto& side_cell = cell<S>();
    const std::scoped_lock lock(side_cell.mutex);
    ReplaceContinuationSideSnapshot out{
        .side = S,
        .generation_counter = side_cell.generation_counter,
        .pending_phase = side_cell.pending_phase,
    };
    if (side_cell.pending_phase != ReplaceContinuationPhase::Empty) {
        out.pending = snapshot_intent<S>(side_cell.pending);
    }
    if (side_cell.in_flight_active) {
        out.in_flight = snapshot_intent<S>(side_cell.in_flight);
    }
    return out;
}

ReplaceContinuationSideSnapshot
NativeReplaceContinuationState::side_snapshot(Side side) const {
    return side == Side::Buy
        ? side_snapshot_impl<Side::Buy>()
        : side_snapshot_impl<Side::Sell>();
}

ReplaceContinuationTelemetry NativeReplaceContinuationState::telemetry() const {
    const std::scoped_lock lock(buy_.mutex, sell_.mutex);
    return ReplaceContinuationTelemetry{
        .arm_count = telemetry_.arm_count.load(std::memory_order_relaxed),
        .publish_count = telemetry_.publish_count.load(std::memory_order_relaxed),
        .decision_count = telemetry_.decision_count.load(std::memory_order_relaxed),
        .drop_count = telemetry_.drop_count.load(std::memory_order_relaxed),
        .buy_decision_count = telemetry_.buy_decision_count.load(
            std::memory_order_relaxed
        ),
        .sell_decision_count = telemetry_.sell_decision_count.load(
            std::memory_order_relaxed
        ),
        .decision_latency_sum_ns = telemetry_.decision_latency_sum_ns.load(
            std::memory_order_relaxed
        ),
        .decision_latency_max_ns = telemetry_.decision_latency_max_ns.load(
            std::memory_order_relaxed
        ),
        .pending_count = static_cast<std::uint64_t>(
            buy_.pending_phase != ReplaceContinuationPhase::Empty
        ) + static_cast<std::uint64_t>(
            sell_.pending_phase != ReplaceContinuationPhase::Empty
        ),
        .in_flight_count = static_cast<std::uint64_t>(buy_.in_flight_active) +
            static_cast<std::uint64_t>(sell_.in_flight_active),
        .event_sequence = event_sequence_.load(std::memory_order_relaxed),
    };
}

// The dispatcher is the only runtime branch.  All side-dependent state access
// and mutation inside the transition is compiled from the two templates.
template ReplaceContinuationTransition
NativeReplaceContinuationState::arm_side<Side::Buy>(
    std::string_view,
    std::int64_t,
    bool
);
template ReplaceContinuationTransition
NativeReplaceContinuationState::arm_side<Side::Sell>(
    std::string_view,
    std::int64_t,
    bool
);

}  // namespace narrowgate_cpp
