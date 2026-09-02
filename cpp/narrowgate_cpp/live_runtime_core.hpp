#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "live_market_state.hpp"
#include "live_policy.hpp"
#include "quote_core.hpp"

namespace narrowgate_cpp {

enum class NativeLiveDecisionStatus : std::uint8_t {
    Applied = 0,
    Busy = 1,
    NoBook = 2,
    InvalidInput = 3,
    DecisionClockRegressed = 4,
    StaleBook = 5,
    MarketIdentityMismatch = 6,
    FeedFault = 7,
    InvalidOutput = 8,
};

// One flat input crosses the Python/native boundary once.  The long-term
// native executable will populate this object directly from native feature,
// inventory and lifecycle state; keeping the same POD now allows exact parity
// testing without reintroducing dict/dataclass materialization.
struct NativeLiveDecisionInput {
    QuoteState quote_state{};
    QuotePrediction prediction{};
    CommonSidePolicyInputPod buy_policy{};
    CommonSidePolicyInputPod sell_policy{};

    std::uint64_t decision_ts_ns = 0;
    std::uint64_t max_book_age_ns = 0;
    std::uint64_t expected_market_publication_sequence = 0;
    std::uint64_t expected_bid_generation = 0;
    std::uint64_t expected_ask_generation = 0;

    double min_qty = 0.0;
    double min_notional = 0.0;
    double size_eta = 0.0;
    double requote_threshold_bps = 0.0;
    // Zero means "use the quote-core cap".  The old NaN sentinel crossed the
    // Python/native ABI and could hide an accidentally non-finite runtime
    // value; keep the hot-path ABI entirely finite instead.
    double routing_max_spread = 0.0;

    double bid_active_price = 0.0;
    double bid_age_ms = 0.0;
    double ask_active_price = 0.0;
    double ask_age_ms = 0.0;
    double bid_order_ttl_ms = 0.0;
    double ask_order_ttl_ms = 0.0;

    bool symmetric_size = false;
    bool bid_active = false;
    bool ask_active = false;
};

struct NativeLiveDecisionResult {
    NativeLiveDecisionStatus status = NativeLiveDecisionStatus::InvalidInput;
    QuoteCoreResult quote{};
    CommonSidePolicyResultPod buy_policy{};
    CommonSidePolicyResultPod sell_policy{};
    LiveRoutingResult routing{};
    std::uint64_t market_publication_sequence = 0;
    std::uint64_t decision_sequence = 0;
    std::uint64_t book_age_ns = 0;
};

static_assert(std::is_trivially_copyable_v<NativeLiveDecisionInput>);
static_assert(std::is_trivially_copyable_v<NativeLiveDecisionResult>);

// Bounded live stage: quote calculation plus the stateless common side policy.
// Stateful owner policy and order lifecycle deliberately remain outside this
// object so enabling it cannot reorder BUY E3, TTL, campaign, hazard or P3.
struct NativeQuotePolicyStageResult {
    QuoteCoreResult quote{};
    CommonSidePolicyResultPod buy_policy{};
    CommonSidePolicyResultPod sell_policy{};
};

static_assert(std::is_trivially_copyable_v<NativeQuotePolicyStageResult>);

class NativeQuotePolicyStage final {
public:
    explicit NativeQuotePolicyStage(QuoteCoreConfig config);

    [[nodiscard]] NativeQuotePolicyStageResult compute(
        const QuoteState& state,
        const QuotePrediction& prediction,
        const DepthView& depth,
        CommonSidePolicyInputPod buy_policy,
        CommonSidePolicyInputPod sell_policy
    ) const;

private:
    template <LivePolicySide Side>
    [[nodiscard]] CommonSidePolicyResultPod finish_policy(
        CommonSidePolicyInputPod policy,
        const QuoteState& state,
        const QuotePrediction& prediction,
        const QuoteCoreResult& quote
    ) const noexcept;

    QuoteCoreConfig config_;
    const QuoteHotPlan quote_hot_plan_;
};

// Fused market-snapshot -> quote -> side-policy -> routing hot path.
//
// Template specialization is intentionally limited to finite semantic axes
// (BUY/SELL). Runtime strategy switches remain data: compiling every switch
// combination would bloat the instruction cache and make live configuration a
// build artifact. Independently mutated synchronization cells are isolated at
// the x86 cache-line boundary; sequential depth data remain packed.
class NativeLiveRuntimeCore final {
public:
    explicit NativeLiveRuntimeCore(QuoteCoreConfig config);

    NativeLiveRuntimeCore(const NativeLiveRuntimeCore&) = delete;
    NativeLiveRuntimeCore& operator=(const NativeLiveRuntimeCore&) = delete;
    NativeLiveRuntimeCore(NativeLiveRuntimeCore&&) = delete;
    NativeLiveRuntimeCore& operator=(NativeLiveRuntimeCore&&) = delete;

    [[nodiscard]] MarketStateUpdateStatus publish_book(
        const Depth20SideUpdate& bids,
        const Depth20SideUpdate& asks
    ) noexcept;

    [[nodiscard]] NativeLiveDecisionResult decide(
        const NativeLiveDecisionInput& input
    );

    [[nodiscard]] LiveDepth20BookSnapshot book_snapshot() const noexcept {
        return market_.read_book();
    }

    [[nodiscard]] std::uint64_t decision_count() const noexcept {
        return decision_sequence_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t market_publication_sequence() const noexcept {
        return market_.publication_sequence();
    }

    [[nodiscard]] std::uint64_t feed_fault_epoch() const noexcept {
        return feed_fault_epoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t feed_resync_epoch() const noexcept {
        return feed_resync_epoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool feed_fault_latched() const noexcept;

    [[nodiscard]] const QuoteCoreConfig& config() const noexcept {
        return config_;
    }

    static constexpr std::size_t cache_line_bytes() noexcept {
        return kDestructiveInterferenceBytes;
    }
    static std::size_t core_size_bytes() noexcept;
    static std::size_t core_alignment_bytes() noexcept;

private:
    class DecisionLease final {
    public:
        explicit DecisionLease(std::atomic_flag& flag) noexcept
            : flag_(&flag), acquired_(!flag.test_and_set(std::memory_order_acquire)) {}
        DecisionLease(const DecisionLease&) = delete;
        DecisionLease& operator=(const DecisionLease&) = delete;
        ~DecisionLease() {
            if (acquired_) {
                flag_->clear(std::memory_order_release);
            }
        }
        [[nodiscard]] bool acquired() const noexcept { return acquired_; }

    private:
        std::atomic_flag* flag_;
        bool acquired_;
    };

    template <LivePolicySide Side>
    [[nodiscard]] CommonSidePolicyResultPod evaluate_policy(
        const NativeLiveDecisionInput& input,
        const QuoteCoreResult& quote,
        double depth_age_s
    ) const noexcept;

    [[nodiscard]] static bool finite_non_negative(double value) noexcept;
    [[nodiscard]] static bool finite_positive(double value) noexcept;
    [[nodiscard]] static bool valid_quote_state(const QuoteState& value) noexcept;
    [[nodiscard]] static bool valid_prediction(
        const QuotePrediction& value
    ) noexcept;
    [[nodiscard]] static bool valid_policy_input(
        const CommonSidePolicyInputPod& value
    ) noexcept;
    [[nodiscard]] bool valid_quote_output(
        const QuoteCoreResult& value
    ) const noexcept;
    [[nodiscard]] static bool valid_policy_output(
        const CommonSidePolicyResultPod& value
    ) noexcept;
    [[nodiscard]] bool valid_routing_output(
        const LiveRoutingResult& value,
        const LiveDepth20BookSnapshot& book
    ) const noexcept;

    LiveMarketState market_{};
    LiveMarketState::Writer market_writer_;
    QuoteCoreConfig config_;
    const QuoteHotPlan quote_hot_plan_;
    // Largest depth cutoff reachable under the immutable quote config.  The
    // decision path builds only this many strict-forward prefix entries.
    const std::uint8_t depth_prefix_levels_;

    alignas(kDestructiveInterferenceBytes) std::atomic_flag decision_busy_ =
        ATOMIC_FLAG_INIT;
    alignas(kDestructiveInterferenceBytes) std::atomic<std::uint64_t>
        last_decision_ts_ns_{0};
    alignas(kDestructiveInterferenceBytes) std::atomic<std::uint64_t>
        decision_sequence_{0};
    // Every rejected market publication advances the fault epoch.  A complete
    // two-sided publication may admit exactly the fault epoch observed before
    // its write, but never a failure racing with that write.  Decisions are
    // blocked while the epochs differ, even though LiveMarketState still
    // contains the last valid book.
    alignas(kDestructiveInterferenceBytes) std::atomic<std::uint64_t>
        feed_fault_epoch_{0};
    alignas(kDestructiveInterferenceBytes) std::atomic<std::uint64_t>
        feed_resync_epoch_{0};
};

}  // namespace narrowgate_cpp
