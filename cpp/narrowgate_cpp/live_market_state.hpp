#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <type_traits>

#include "common.hpp"

namespace narrowgate_cpp {

inline constexpr std::size_t kLiveDepthLevels = 20;

// These clocks intentionally remain separate. source/exchange timestamps are
// external clock domains; receive/visible timestamps are local clock domains.
// Only receive <= visible is an intra-event ordering requirement. Every clock
// must advance monotonically for a given side, but no clock-offset assumption
// is made between an exchange and the live host.
struct MarketClockIdentity {
    std::uint64_t source_ts_ns = 0;
    std::uint64_t exchange_ts_ns = 0;
    std::uint64_t receive_ts_ns = 0;
    std::uint64_t visible_ts_ns = 0;
    std::uint64_t generation = 0;
};

static_assert(std::is_trivially_copyable_v<MarketClockIdentity>);

// Full top-20 replacement for one side of Binance's partial-depth stream.
// Prices and quantities have already crossed the network boundary and are
// represented only as exchange ticks/lots in the native hot path.
struct Depth20SideUpdate {
    std::array<std::int64_t, kLiveDepthLevels> price_ticks{};
    std::array<std::int64_t, kLiveDepthLevels> quantity_lots{};
    MarketClockIdentity clock{};
    std::uint8_t size = 0;
};

struct alignas(kDestructiveInterferenceBytes) Depth20SideSnapshot {
    alignas(kDestructiveInterferenceBytes)
        std::array<std::int64_t, kLiveDepthLevels> price_ticks{};
    alignas(kDestructiveInterferenceBytes)
        std::array<std::int64_t, kLiveDepthLevels> quantity_lots{};
    MarketClockIdentity clock{};
    std::uint8_t size = 0;

    [[nodiscard]] bool empty() const noexcept { return size == 0; }

    [[nodiscard]] std::int64_t best_price_ticks() const noexcept {
        return empty() ? 0 : price_ticks[0];
    }

    [[nodiscard]] std::int64_t best_quantity_lots() const noexcept {
        return empty() ? 0 : quantity_lots[0];
    }
};

static_assert(
    alignof(Depth20SideSnapshot) == kDestructiveInterferenceBytes
);
static_assert(
    sizeof(Depth20SideSnapshot) % kDestructiveInterferenceBytes == 0
);

struct BboTicksSnapshot {
    std::int64_t bid_price_ticks = 0;
    std::int64_t bid_quantity_lots = 0;
    std::int64_t ask_price_ticks = 0;
    std::int64_t ask_quantity_lots = 0;
    std::uint64_t bid_generation = 0;
    std::uint64_t ask_generation = 0;
    std::uint64_t bid_visible_ts_ns = 0;
    std::uint64_t ask_visible_ts_ns = 0;

    [[nodiscard]] bool valid() const noexcept {
        return bid_price_ticks > 0 && ask_price_ticks > bid_price_ticks &&
               bid_quantity_lots > 0 && ask_quantity_lots > 0;
    }
};

static_assert(std::is_trivially_copyable_v<BboTicksSnapshot>);

struct LiveDepth20BookSnapshot {
    Depth20SideSnapshot bids{};
    Depth20SideSnapshot asks{};
    BboTicksSnapshot bbo{};
    std::uint64_t publication_sequence = 0;

    [[nodiscard]] bool valid() const noexcept {
        return bbo.valid() && !bids.empty() && !asks.empty() &&
               bids.best_price_ticks() == bbo.bid_price_ticks &&
               asks.best_price_ticks() == bbo.ask_price_ticks;
    }
};

enum class MarketStateUpdateStatus : std::uint8_t {
    Applied = 0,
    InvalidDepthSize = 1,
    InvalidLevel = 2,
    InvalidPriceOrder = 3,
    InvalidClock = 4,
    ClockRegressed = 5,
    GenerationRegressed = 6,
    CrossedBook = 7,
    WriterBusy = 8,
};

// Single-writer, concurrent-reader market state.
//
// A caller must first claim the sole Writer lease. The lease is deliberately
// long-lived so the hot update path does not pay a mutex/thread-id check on
// every depth event. Readers use an atomic seqlock and can run concurrently.
// Atomic payload fields avoid the data races that a seqlock over ordinary
// C++ objects would otherwise create.
class LiveMarketState final {
public:
    class Writer final {
    public:
        Writer(const Writer&) = delete;
        Writer& operator=(const Writer&) = delete;
        Writer(Writer&&) = delete;
        Writer& operator=(Writer&&) = delete;

        ~Writer() { release(); }

        template <Side S>
        [[nodiscard]] MarketStateUpdateStatus replace(
            const Depth20SideUpdate& update
        ) noexcept {
            return with_write_lease([&]() noexcept {
                return owner_->replace_side<S>(update);
            });
        }

        // Publish a two-sided partial-depth message as one generation. This
        // avoids exposing an intermediate crossed book while the BBO moves
        // through the previous opposite-side price.
        [[nodiscard]] MarketStateUpdateStatus replace_book(
            const Depth20SideUpdate& bids,
            const Depth20SideUpdate& asks
        ) noexcept {
            return with_write_lease([&]() noexcept {
                return owner_->replace_book(bids, asks);
            });
        }

    private:
        friend class LiveMarketState;

        explicit Writer(LiveMarketState* owner) noexcept : owner_(owner) {}

        template <typename Write>
        [[nodiscard]] MarketStateUpdateStatus with_write_lease(
            Write&& write
        ) noexcept {
            if (owner_ == nullptr ||
                owner_->writer_in_progress_.test_and_set(
                    std::memory_order_acquire
                )) {
                return MarketStateUpdateStatus::WriterBusy;
            }
            const MarketStateUpdateStatus status = write();
            owner_->writer_in_progress_.clear(std::memory_order_release);
            return status;
        }

        void release() noexcept {
            if (owner_ != nullptr) {
                owner_->writer_claimed_.store(false, std::memory_order_release);
                owner_ = nullptr;
            }
        }

        LiveMarketState* owner_ = nullptr;
    };

    LiveMarketState() = default;
    LiveMarketState(const LiveMarketState&) = delete;
    LiveMarketState& operator=(const LiveMarketState&) = delete;
    LiveMarketState(LiveMarketState&&) = delete;
    LiveMarketState& operator=(LiveMarketState&&) = delete;

    [[nodiscard]] Writer claim_writer() {
        bool expected = false;
        if (!writer_claimed_.compare_exchange_strong(
                expected,
                true,
                std::memory_order_acq_rel,
                std::memory_order_acquire
            )) {
            throw std::logic_error("LiveMarketState already has a writer");
        }
        return Writer{this};
    }

    template <Side S>
    [[nodiscard]] Depth20SideSnapshot read() const noexcept {
        Depth20SideSnapshot snapshot;
        read_consistent([&]() noexcept {
            const SideStorage& storage = side_storage<S>();
            const std::size_t size = static_cast<std::size_t>(
                storage.size.load(std::memory_order_relaxed)
            );
            snapshot.size = static_cast<std::uint8_t>(size);
            for (std::size_t index = 0; index < kLiveDepthLevels; ++index) {
                snapshot.price_ticks[index] =
                    storage.price_ticks[index].load(std::memory_order_relaxed);
                snapshot.quantity_lots[index] =
                    storage.quantity_lots[index].load(std::memory_order_relaxed);
            }
            snapshot.clock = load_clock(storage);
        });
        return snapshot;
    }

    [[nodiscard]] BboTicksSnapshot read_bbo() const noexcept {
        BboTicksSnapshot snapshot;
        read_consistent([&]() noexcept {
            snapshot.bid_price_ticks =
                bbo_.bid_price_ticks.load(std::memory_order_relaxed);
            snapshot.bid_quantity_lots =
                bbo_.bid_quantity_lots.load(std::memory_order_relaxed);
            snapshot.ask_price_ticks =
                bbo_.ask_price_ticks.load(std::memory_order_relaxed);
            snapshot.ask_quantity_lots =
                bbo_.ask_quantity_lots.load(std::memory_order_relaxed);
            snapshot.bid_generation =
                bbo_.bid_generation.load(std::memory_order_relaxed);
            snapshot.ask_generation =
                bbo_.ask_generation.load(std::memory_order_relaxed);
            snapshot.bid_visible_ts_ns =
                bbo_.bid_visible_ts_ns.load(std::memory_order_relaxed);
            snapshot.ask_visible_ts_ns =
                bbo_.ask_visible_ts_ns.load(std::memory_order_relaxed);
        });
        return snapshot;
    }

    // Read both depth sides under one seqlock generation.  The BBO fields are
    // derived from those already-loaded side snapshots: the writer publishes
    // the compact BBO cache from the same first level/clock, so reloading its
    // eight atomics here is redundant for a full-book decision.  read_bbo()
    // remains the low-touch path for consumers that do not need depth.
    [[nodiscard]] LiveDepth20BookSnapshot read_book() const noexcept {
        return read_book_prefix_unchecked(kLiveDepthLevels);
    }

    // Read only the depth prefix required by a decision while preserving the
    // same two-sided seqlock, clocks, BBO and publication identity as a full
    // read.  The returned side sizes describe the initialized prefix, never
    // the larger stored depth, so downstream spans cannot observe an unloaded
    // tail.  Invalid bounds are rejected before entering the retry loop.
    [[nodiscard]] LiveDepth20BookSnapshot read_book_prefix(
        std::size_t levels
    ) const {
        if (levels == 0 || levels > kLiveDepthLevels) {
            throw std::out_of_range(
                "LiveMarketState depth prefix must be in [1, 20]"
            );
        }
        return read_book_prefix_unchecked(levels);
    }

    [[nodiscard]] std::uint64_t publication_sequence() const noexcept {
        return publication_sequence_.load(std::memory_order_acquire);
    }

    static constexpr std::size_t depth_levels() noexcept {
        return kLiveDepthLevels;
    }

    static constexpr std::size_t cache_line_bytes() noexcept {
        return kDestructiveInterferenceBytes;
    }

private:
    [[nodiscard]] LiveDepth20BookSnapshot read_book_prefix_unchecked(
        std::size_t levels
    ) const noexcept {
        LiveDepth20BookSnapshot snapshot;
        read_consistent([&]() noexcept {
            load_side_snapshot_prefix<Side::Buy>(snapshot.bids, levels);
            load_side_snapshot_prefix<Side::Sell>(snapshot.asks, levels);
            snapshot.bbo.bid_price_ticks = snapshot.bids.best_price_ticks();
            snapshot.bbo.bid_quantity_lots = snapshot.bids.best_quantity_lots();
            snapshot.bbo.ask_price_ticks = snapshot.asks.best_price_ticks();
            snapshot.bbo.ask_quantity_lots = snapshot.asks.best_quantity_lots();
            snapshot.bbo.bid_generation = snapshot.bids.clock.generation;
            snapshot.bbo.ask_generation = snapshot.asks.clock.generation;
            snapshot.bbo.bid_visible_ts_ns = snapshot.bids.clock.visible_ts_ns;
            snapshot.bbo.ask_visible_ts_ns = snapshot.asks.clock.visible_ts_ns;
            snapshot.publication_sequence =
                publication_sequence_.load(std::memory_order_relaxed);
        });
        return snapshot;
    }
    static_assert(std::atomic<std::int64_t>::is_always_lock_free);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

    struct alignas(kDestructiveInterferenceBytes) SideStorage {
        alignas(kDestructiveInterferenceBytes)
            std::array<std::atomic<std::int64_t>, kLiveDepthLevels>
                price_ticks{};
        alignas(kDestructiveInterferenceBytes)
            std::array<std::atomic<std::int64_t>, kLiveDepthLevels>
                quantity_lots{};
        alignas(kDestructiveInterferenceBytes) std::atomic<std::uint64_t> size{0};
        std::atomic<std::uint64_t> source_ts_ns{0};
        std::atomic<std::uint64_t> exchange_ts_ns{0};
        std::atomic<std::uint64_t> receive_ts_ns{0};
        std::atomic<std::uint64_t> visible_ts_ns{0};
        std::atomic<std::uint64_t> generation{0};
    };

    static_assert(alignof(SideStorage) == kDestructiveInterferenceBytes);
    static_assert(sizeof(SideStorage) % kDestructiveInterferenceBytes == 0);

    // Decision code reads this compact cache instead of touching both 20-level
    // arrays. On the current amd64 target it is exactly one 64-byte line; on
    // the M4 it occupies half of one 128-byte line and remains isolated.
    struct alignas(kDestructiveInterferenceBytes) BboHotCache {
        std::atomic<std::int64_t> bid_price_ticks{0};
        std::atomic<std::int64_t> bid_quantity_lots{0};
        std::atomic<std::int64_t> ask_price_ticks{0};
        std::atomic<std::int64_t> ask_quantity_lots{0};
        std::atomic<std::uint64_t> bid_generation{0};
        std::atomic<std::uint64_t> ask_generation{0};
        std::atomic<std::uint64_t> bid_visible_ts_ns{0};
        std::atomic<std::uint64_t> ask_visible_ts_ns{0};
    };

    static_assert(alignof(BboHotCache) == kDestructiveInterferenceBytes);
    static_assert(sizeof(BboHotCache) % kDestructiveInterferenceBytes == 0);

    template <Side S>
    [[nodiscard]] SideStorage& side_storage() noexcept {
        if constexpr (S == Side::Buy) {
            return bids_;
        } else {
            return asks_;
        }
    }

    template <Side S>
    [[nodiscard]] const SideStorage& side_storage() const noexcept {
        if constexpr (S == Side::Buy) {
            return bids_;
        } else {
            return asks_;
        }
    }

    static MarketClockIdentity load_clock(const SideStorage& storage) noexcept {
        return MarketClockIdentity{
            storage.source_ts_ns.load(std::memory_order_relaxed),
            storage.exchange_ts_ns.load(std::memory_order_relaxed),
            storage.receive_ts_ns.load(std::memory_order_relaxed),
            storage.visible_ts_ns.load(std::memory_order_relaxed),
            storage.generation.load(std::memory_order_relaxed),
        };
    }

    template <Side S>
    void load_side_snapshot(Depth20SideSnapshot& snapshot) const noexcept {
        load_side_snapshot_prefix<S>(snapshot, kLiveDepthLevels);
    }

    template <Side S>
    void load_side_snapshot_prefix(
        Depth20SideSnapshot& snapshot,
        std::size_t levels
    ) const noexcept {
        const SideStorage& storage = side_storage<S>();
        const std::size_t size = std::min(
            static_cast<std::size_t>(
                storage.size.load(std::memory_order_relaxed)
            ),
            levels
        );
        snapshot.size = static_cast<std::uint8_t>(size);
        for (std::size_t index = 0; index < size; ++index) {
            snapshot.price_ticks[index] =
                storage.price_ticks[index].load(std::memory_order_relaxed);
            snapshot.quantity_lots[index] =
                storage.quantity_lots[index].load(std::memory_order_relaxed);
        }
        snapshot.clock = load_clock(storage);
    }

    static bool clock_is_present(const MarketClockIdentity& clock) noexcept {
        return clock.source_ts_ns != 0 && clock.exchange_ts_ns != 0 &&
               clock.receive_ts_ns != 0 && clock.visible_ts_ns != 0 &&
               clock.generation != 0 &&
               clock.receive_ts_ns <= clock.visible_ts_ns;
    }

    static bool clock_regressed(
        const MarketClockIdentity& next,
        const MarketClockIdentity& previous
    ) noexcept {
        return next.source_ts_ns < previous.source_ts_ns ||
               next.exchange_ts_ns < previous.exchange_ts_ns ||
               next.receive_ts_ns < previous.receive_ts_ns ||
               next.visible_ts_ns < previous.visible_ts_ns;
    }

    template <Side S>
    static MarketStateUpdateStatus validate_levels(
        const Depth20SideUpdate& update
    ) noexcept {
        if (update.size > kLiveDepthLevels) {
            return MarketStateUpdateStatus::InvalidDepthSize;
        }
        for (std::size_t index = 0; index < update.size; ++index) {
            if (update.price_ticks[index] <= 0 ||
                update.quantity_lots[index] <= 0) {
                return MarketStateUpdateStatus::InvalidLevel;
            }
            if (index == 0) {
                continue;
            }
            if constexpr (S == Side::Buy) {
                if (update.price_ticks[index] >= update.price_ticks[index - 1]) {
                    return MarketStateUpdateStatus::InvalidPriceOrder;
                }
            } else {
                if (update.price_ticks[index] <= update.price_ticks[index - 1]) {
                    return MarketStateUpdateStatus::InvalidPriceOrder;
                }
            }
        }
        return MarketStateUpdateStatus::Applied;
    }

    template <Side S>
    [[nodiscard]] MarketStateUpdateStatus replace_side(
        const Depth20SideUpdate& update
    ) noexcept {
        const MarketStateUpdateStatus level_status = validate_levels<S>(update);
        if (level_status != MarketStateUpdateStatus::Applied) {
            return level_status;
        }
        SideStorage& storage = side_storage<S>();
        const MarketStateUpdateStatus clock_status =
            validate_clock(update.clock, storage);
        if (clock_status != MarketStateUpdateStatus::Applied) {
            return clock_status;
        }

        if (update.size != 0) {
            const std::int64_t new_best = update.price_ticks[0];
            if constexpr (S == Side::Buy) {
                const std::int64_t ask =
                    bbo_.ask_price_ticks.load(std::memory_order_relaxed);
                if (ask != 0 && new_best >= ask) {
                    return MarketStateUpdateStatus::CrossedBook;
                }
            } else {
                const std::int64_t bid =
                    bbo_.bid_price_ticks.load(std::memory_order_relaxed);
                if (bid != 0 && new_best <= bid) {
                    return MarketStateUpdateStatus::CrossedBook;
                }
            }
        }

        // Odd publication sequence marks a write in progress. Payload stores
        // are relaxed because the final release increment publishes them.
        publication_sequence_.fetch_add(1, std::memory_order_acq_rel);
        write_side_unpublished<S>(update, storage);
        publication_sequence_.fetch_add(1, std::memory_order_release);
        return MarketStateUpdateStatus::Applied;
    }

    [[nodiscard]] MarketStateUpdateStatus replace_book(
        const Depth20SideUpdate& bids,
        const Depth20SideUpdate& asks
    ) noexcept {
        const MarketStateUpdateStatus bid_levels = validate_levels<Side::Buy>(bids);
        if (bid_levels != MarketStateUpdateStatus::Applied) {
            return bid_levels;
        }
        const MarketStateUpdateStatus ask_levels = validate_levels<Side::Sell>(asks);
        if (ask_levels != MarketStateUpdateStatus::Applied) {
            return ask_levels;
        }
        const MarketStateUpdateStatus bid_clock = validate_clock(bids.clock, bids_);
        if (bid_clock != MarketStateUpdateStatus::Applied) {
            return bid_clock;
        }
        const MarketStateUpdateStatus ask_clock = validate_clock(asks.clock, asks_);
        if (ask_clock != MarketStateUpdateStatus::Applied) {
            return ask_clock;
        }
        if (bids.size != 0 && asks.size != 0 &&
            bids.price_ticks[0] >= asks.price_ticks[0]) {
            return MarketStateUpdateStatus::CrossedBook;
        }

        publication_sequence_.fetch_add(1, std::memory_order_acq_rel);
        write_side_unpublished<Side::Buy>(bids, bids_);
        write_side_unpublished<Side::Sell>(asks, asks_);
        publication_sequence_.fetch_add(1, std::memory_order_release);
        return MarketStateUpdateStatus::Applied;
    }

    static MarketStateUpdateStatus validate_clock(
        const MarketClockIdentity& next,
        const SideStorage& storage
    ) noexcept {
        if (!clock_is_present(next)) {
            return MarketStateUpdateStatus::InvalidClock;
        }
        const MarketClockIdentity previous = load_clock(storage);
        if (next.generation <= previous.generation) {
            return MarketStateUpdateStatus::GenerationRegressed;
        }
        if (previous.generation != 0 && clock_regressed(next, previous)) {
            return MarketStateUpdateStatus::ClockRegressed;
        }
        return MarketStateUpdateStatus::Applied;
    }

    template <Side S>
    void write_side_unpublished(
        const Depth20SideUpdate& update,
        SideStorage& storage
    ) noexcept {
        for (std::size_t index = 0; index < kLiveDepthLevels; ++index) {
            const bool active = index < update.size;
            storage.price_ticks[index].store(
                active ? update.price_ticks[index] : 0,
                std::memory_order_relaxed
            );
            storage.quantity_lots[index].store(
                active ? update.quantity_lots[index] : 0,
                std::memory_order_relaxed
            );
        }
        storage.size.store(update.size, std::memory_order_relaxed);
        storage.source_ts_ns.store(
            update.clock.source_ts_ns,
            std::memory_order_relaxed
        );
        storage.exchange_ts_ns.store(
            update.clock.exchange_ts_ns,
            std::memory_order_relaxed
        );
        storage.receive_ts_ns.store(
            update.clock.receive_ts_ns,
            std::memory_order_relaxed
        );
        storage.visible_ts_ns.store(
            update.clock.visible_ts_ns,
            std::memory_order_relaxed
        );
        storage.generation.store(
            update.clock.generation,
            std::memory_order_relaxed
        );

        const std::int64_t best_price =
            update.size == 0 ? 0 : update.price_ticks[0];
        const std::int64_t best_quantity =
            update.size == 0 ? 0 : update.quantity_lots[0];
        if constexpr (S == Side::Buy) {
            bbo_.bid_price_ticks.store(best_price, std::memory_order_relaxed);
            bbo_.bid_quantity_lots.store(
                best_quantity,
                std::memory_order_relaxed
            );
            bbo_.bid_generation.store(
                update.clock.generation,
                std::memory_order_relaxed
            );
            bbo_.bid_visible_ts_ns.store(
                update.clock.visible_ts_ns,
                std::memory_order_relaxed
            );
        } else {
            bbo_.ask_price_ticks.store(best_price, std::memory_order_relaxed);
            bbo_.ask_quantity_lots.store(
                best_quantity,
                std::memory_order_relaxed
            );
            bbo_.ask_generation.store(
                update.clock.generation,
                std::memory_order_relaxed
            );
            bbo_.ask_visible_ts_ns.store(
                update.clock.visible_ts_ns,
                std::memory_order_relaxed
            );
        }
    }

    template <typename Reader>
    void read_consistent(Reader&& reader) const noexcept {
        std::uint32_t attempts = 0;
        for (;;) {
            const std::uint64_t before =
                publication_sequence_.load(std::memory_order_acquire);
            if ((before & 1U) != 0U) {
                if ((++attempts & 63U) == 0U) {
                    std::this_thread::yield();
                }
                continue;
            }
            reader();
            const std::uint64_t after =
                publication_sequence_.load(std::memory_order_acquire);
            if (before == after) {
                return;
            }
            if ((++attempts & 63U) == 0U) {
                std::this_thread::yield();
            }
        }
    }

    alignas(kDestructiveInterferenceBytes) SideStorage bids_{};
    alignas(kDestructiveInterferenceBytes) SideStorage asks_{};
    alignas(kDestructiveInterferenceBytes) BboHotCache bbo_{};
    alignas(kDestructiveInterferenceBytes)
        std::atomic<std::uint64_t> publication_sequence_{0};
    alignas(kDestructiveInterferenceBytes)
        std::atomic<bool> writer_claimed_{false};
    alignas(kDestructiveInterferenceBytes)
        std::atomic_flag writer_in_progress_ = ATOMIC_FLAG_INIT;
};

}  // namespace narrowgate_cpp
