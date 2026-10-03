#include "book_fusion.hpp"

#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>

namespace narrowgate_cpp {
namespace py = pybind11;
namespace {
using I = std::int64_t;
using Key = std::pair<I, I>;  // side: 0 bid, 1 ask; exact fixed-point price
constexpr std::size_t kColumns = 14;
// presentation E, real E, local us, snapshot, native, U, u, pu, last, T,
// side, price units, amount units, original source row.
using Row = std::array<I, kColumns>;
using MessageKey = std::tuple<I, I, I, I, I, I, I>;
struct Level { I amount = 0; Row origin{}; };
struct Source {
    std::map<Key, Level> book;
    std::set<Key> different;
    bool initialized = false;
    bool bridge = false;
    I last_id = -1;
    I observed = 0;
    I local = 0;
    I last_presentation = -1;
    I last_snapshot_e = -1;
    I last_snapshot_id = -1;
    Row last_message{};
};
struct Output {
    std::vector<I> timestamp, observed, local, source, snapshot, side, price, amount;
    std::vector<I> reason, event, transaction, first, final, previous, last, source_row;
    std::vector<I> normalized_timestamp, normalized_observed, normalized_local, normalized_source;
    std::vector<I> normalized_levels, normalized_carried;
    void clear() { *this = Output{}; }
};

template<class T> py::array_t<T> array_copy(const std::vector<T>& values) {
    py::array_t<T> result(values.size());
    std::copy(values.begin(), values.end(), result.mutable_data());
    return result;
}

class BookFusion {
public:
    BookFusion(I sources, I preferred, I minimum_levels)
        : sources_(static_cast<std::size_t>(sources)), preferred_(preferred),
          minimum_levels_(minimum_levels) {
        if (sources < 1 || sources > 16 || preferred < 0 || preferred >= sources || minimum_levels < 1)
            throw std::invalid_argument("invalid fusion source/depth configuration");
    }

    py::dict push(py::array_t<I, py::array::c_style | py::array::forcecast> source_ids,
                  py::array_t<I, py::array::c_style | py::array::forcecast> rows) {
        if (source_ids.ndim() != 1 || rows.ndim() != 2 || rows.shape(1) != kColumns ||
            rows.shape(0) != source_ids.shape(0))
            throw std::invalid_argument("fusion expects source_ids[N] and rows[N,14]");
        const auto ids = source_ids.unchecked<1>();
        const auto data = rows.unchecked<2>();
        {
            py::gil_scoped_release release;
            for (py::ssize_t n = 0; n < rows.shape(0); ++n) {
                const I id = ids(n);
                if (id < 0 || id >= static_cast<I>(sources_.size()))
                    throw std::invalid_argument("fusion source index outside declared sources");
                Row row{};
                for (std::size_t c = 0; c < kColumns; ++c) row[c] = data(n, c);
                if (row[0] <= 0 || row[1] <= 0 || row[1] > row[0])
                    throw std::invalid_argument("fusion presentation precedes actual exchange observation");
                if (pending_time_ > row[0])
                    throw std::invalid_argument("fusion input presentation clock regressed");
                if (pending_time_ != row[0]) {
                    process_pending();
                    pending_time_ = row[0];
                }
                pending_[id].push_back(row);
                ++raw_rows_;
            }
        }
        return take_output();
    }

    py::dict finish() {
        { py::gil_scoped_release release; process_pending(); seed_opening(); sample_until(sample_end_ - 1); }
        return take_output();
    }

    void output_window(I start_us, I end_us) {
        if (raw_rows_ || start_us <= 0 || end_us <= start_us)
            throw std::invalid_argument("invalid fusion raw output window");
        output_start_ = start_us;
        output_end_ = end_us;
    }

    void raw_diff_output(bool enabled, bool allow_legacy_continuation) {
        if (raw_rows_ || !pending_.empty() || selected_ >= 0)
            throw std::invalid_argument("configure fusion output before input or restore");
        raw_diff_output_ = enabled;
        allow_legacy_continuation_ = allow_legacy_continuation;
    }

    void sample(I start_us, I end_us, I cadence_us) {
        if (raw_rows_ || start_us <= 0 || end_us <= start_us || cadence_us <= 0 || start_us % cadence_us)
            throw std::invalid_argument("invalid fusion sampling window");
        next_sample_ = start_us;
        sample_end_ = end_us;
        sample_step_ = cadence_us;
    }

    py::dict stats() const {
        py::dict value;
        value["raw_rows"] = raw_rows_;
        value["logical_messages"] = messages_;
        value["accepted_messages"] = accepted_;
        value["invalid_messages"] = invalid_;
        value["pre_snapshot_messages"] = pre_snapshot_;
        value["duplicate_or_stale_messages"] = duplicates_;
        value["sequence_gaps"] = gaps_;
        value["source_switches"] = switches_;
        value["output_rows"] = output_rows_;
        value["output_messages"] = output_messages_;
        value["observation_refresh_messages"] = refreshes_;
        value["no_valid_source_times"] = unavailable_;
        value["equal_clock_conflicting_states"] = raw_diff_output_ ? py::cast(conflicts_) : py::none();
        value["equal_clock_conflicts_checked"] = raw_diff_output_;
        value["older_fallback_suppressed"] = older_suppressed_;
        value["normalized_rows"] = normalized_rows_;
        value["normalized_unknown_buckets"] = normalized_unknown_;
        value["normalized_carried_rows"] = normalized_carried_;
        value["max_observation_age_us"] = max_age_;
        value["future_fill_violations"] = 0;
        value["native_sequence_authority"] = false;
        value["byte_lossless_archive"] = false;
        value["raw_diff_output"] = raw_diff_output_;
        value["legacy_continuation_seed"] = legacy_continuation_seed_;
        value["aged_fallback_captures"] = fallback_captures_;
        std::vector<I> per_source, observed;
        for (std::size_t n = 0; n < sources_.size(); ++n) {
            auto found = selected_counts_.find(static_cast<I>(n));
            per_source.push_back(found == selected_counts_.end() ? 0 : found->second);
            observed.push_back(sources_[n].observed);
        }
        value["selected_messages_by_source"] = per_source;
        value["last_observed_us_by_source"] = observed;
        return value;
    }

    py::dict state() const {
        if (!pending_.empty()) throw std::logic_error("finish fusion before exporting continuation");
        py::dict out;
        out["schema"] = raw_diff_output_ ? "book_fusion.continuation.v1" : "book_fusion.continuation.v2";
        out["raw_diff_output"] = raw_diff_output_;
        out["view_source"] = view_source_;
        out["legacy_continuation_seed"] = legacy_continuation_seed_;
        out["source_count"] = sources_.size();
        out["preferred"] = preferred_;
        out["minimum_levels"] = minimum_levels_;
        out["presentation_us"] = pending_time_;
        out["selected_source"] = selected_;
        out["global_observed_us"] = global_observed_;
        out["global_local_us"] = global_local_;
        out["last_sample_observed_us"] = last_sample_observed_;
        auto levels = [](const auto& book) {
            std::vector<std::vector<I>> values;
            values.reserve(book.size());
            for (const auto& [key, level] : book) {
                std::vector<I> row{key.first, key.second, level.amount};
                row.insert(row.end(), level.origin.begin(), level.origin.end());
                values.push_back(std::move(row));
            }
            return values;
        };
        out["global_levels"] = levels(view_book());
        py::list sources;
        for (const auto& source : sources_) {
            py::dict value;
            value["initialized"] = source.initialized;
            value["bridge"] = source.bridge;
            value["last_id"] = source.last_id;
            value["observed_us"] = source.observed;
            value["local_us"] = source.local;
            value["presentation_us"] = source.last_presentation;
            value["last_snapshot_e"] = source.last_snapshot_e;
            value["last_snapshot_id"] = source.last_snapshot_id;
            value["last_message"] = source.last_message;
            value["levels"] = levels(source.book);
            sources.append(value);
        }
        out["sources"] = sources;
        return out;
    }

    void restore(const py::dict& state) {
        if (raw_rows_ || !pending_.empty()) throw std::invalid_argument("restore requires a fresh fusion instance");
        const auto schema = py::cast<std::string>(state["schema"]);
        const bool legacy = schema == "book_fusion.continuation.v1";
        if ((!legacy && schema != "book_fusion.continuation.v2") ||
            (raw_diff_output_ && !legacy) ||
            (!raw_diff_output_ && legacy && !allow_legacy_continuation_) ||
            py::cast<I>(state["source_count"]) != static_cast<I>(sources_.size()) ||
            py::cast<I>(state["preferred"]) != preferred_ ||
            py::cast<I>(state["minimum_levels"]) != minimum_levels_)
            throw std::invalid_argument("fusion continuation configuration mismatch");
        auto load_levels = [](py::handle rows) {
            std::map<Key, Level> result;
            for (const auto& row : py::cast<std::vector<std::vector<I>>>(rows)) {
                if (row.size() != kColumns + 3 || row[0] < 0 || row[0] > 1 || row[1] <= 0 || row[2] <= 0)
                    throw std::invalid_argument("invalid fusion continuation level");
                Row origin{};
                std::copy(row.begin() + 3, row.end(), origin.begin());
                if (!result.emplace(Key{row[0], row[1]}, Level{row[2], origin}).second)
                    throw std::invalid_argument("duplicate fusion continuation level");
            }
            return result;
        };
        // Construct separately; invalid checkpoints never mutate this instance.
        auto global = load_levels(state["global_levels"]);
        std::vector<Source> restored;
        for (py::handle item : state["sources"]) {
            auto value = py::cast<py::dict>(item);
            Source source;
            source.initialized = py::cast<bool>(value["initialized"]);
            source.bridge = py::cast<bool>(value["bridge"]);
            source.last_id = py::cast<I>(value["last_id"]);
            source.observed = py::cast<I>(value["observed_us"]);
            source.local = py::cast<I>(value["local_us"]);
            source.last_presentation = py::cast<I>(value["presentation_us"]);
            source.last_snapshot_e = py::cast<I>(value["last_snapshot_e"]);
            source.last_snapshot_id = py::cast<I>(value["last_snapshot_id"]);
            source.last_message = py::cast<Row>(value["last_message"]);
            source.book = load_levels(value["levels"]);
            restored.push_back(std::move(source));
        }
        if (restored.size() != sources_.size()) throw std::invalid_argument("fusion checkpoint source count");
        const I selected = py::cast<I>(state["selected_source"]);
        if (selected < -1 || selected >= static_cast<I>(sources_.size())) throw std::invalid_argument("fusion checkpoint selected source");
        const I presentation = py::cast<I>(state["presentation_us"]);
        const I observed = py::cast<I>(state["global_observed_us"]);
        const I local = py::cast<I>(state["global_local_us"]);
        const I last_sample_observed = state.contains("last_sample_observed_us")
            ? py::cast<I>(state["last_sample_observed_us"]) : observed;
        const bool legacy_seed = (!raw_diff_output_ && legacy) ||
            (state.contains("legacy_continuation_seed") && py::cast<bool>(state["legacy_continuation_seed"]));
        I view_source = -1;
        if (!legacy) {
            if (!state.contains("raw_diff_output") || py::cast<bool>(state["raw_diff_output"]))
                throw std::invalid_argument("fusion checkpoint output semantics mismatch");
            view_source = py::cast<I>(state["view_source"]);
            if (view_source < -1 || view_source >= static_cast<I>(restored.size()) ||
                (view_source >= 0 && (view_source != selected || !valid(restored[view_source]) ||
                                     restored[view_source].observed != observed ||
                                     restored[view_source].local != local)))
                throw std::invalid_argument("invalid fusion checkpoint selected view");
            if (view_source >= 0) {
                const auto& source_book = restored[view_source].book;
                if (source_book.size() != global.size()) throw std::invalid_argument("fusion checkpoint view mismatch");
                for (const auto& [key, level] : global) {
                    const auto found = source_book.find(key);
                    if (found == source_book.end() || found->second.amount != level.amount ||
                        found->second.origin != level.origin)
                        throw std::invalid_argument("fusion checkpoint view mismatch");
                }
            }
        }
        global_ = std::move(global);
        sources_ = std::move(restored);
        pending_time_ = presentation;
        selected_ = selected;
        global_observed_ = observed;
        global_local_ = local;
        last_sample_observed_ = last_sample_observed;
        view_source_ = view_source;
        legacy_continuation_seed_ = legacy_seed;
        if (raw_diff_output_) {
            for (auto& source : sources_) {
                for (const auto& [key, value] : global_) mark_difference(source, key);
                for (const auto& [key, value] : source.book) mark_difference(source, key);
            }
        } else if (view_source_ >= 0) {
            global_.clear();
        }
        force_initial_snapshot_ = true;
    }

private:
    const std::map<Key, Level>& view_book() const {
        return !raw_diff_output_ && view_source_ >= 0 ? sources_[view_source_].book : global_;
    }
    bool is_sampled_source(const Source& source) const {
        return !raw_diff_output_ && view_source_ >= 0 && &source == &sources_[view_source_];
    }
    // Normal updates retain only touched old levels. A full snapshot/reset is
    // uncommon and needs the previous map once, not on every provider switch.
    void remember_level(const Source& source, const Key& key) {
        if (!is_sampled_source(source) || view_reset_saved_ || view_undo_.contains(key)) return;
        const auto found = source.book.find(key);
        view_undo_.emplace(key, found == source.book.end() ? Level{} : found->second);
    }
    void apply_view_undo(std::map<Key, Level>& book) const {
        for (const auto& [key, level] : view_undo_) {
            if (level.amount == 0) book.erase(key); else book[key] = level;
        }
    }
    void remember_reset(const Source& source) {
        if (!is_sampled_source(source) || view_reset_saved_) return;
        view_before_reset_ = source.book;
        apply_view_undo(view_before_reset_);
        view_reset_saved_ = true;
        view_undo_.clear();
    }
    void discard_view_undo() {
        view_undo_.clear();
        view_before_reset_.clear();
        view_reset_saved_ = false;
    }
    void retain_aged_view() {
        if (raw_diff_output_ || view_source_ < 0) return;
        if (view_reset_saved_) global_ = std::move(view_before_reset_);
        else {
            // With no mutation, the current source map is already the verified
            // view and can continue to be sampled without a copy.
            if (view_undo_.empty()) return;
            global_ = sources_[view_source_].book;
            apply_view_undo(global_);
        }
        view_source_ = -1;
        ++fallback_captures_;
        discard_view_undo();
    }
    I amount(const std::map<Key, Level>& book, const Key& key) const {
        const auto found = book.find(key);
        return found == book.end() ? 0 : found->second.amount;
    }
    void mark_difference(Source& source, const Key& key) {
        if (!raw_diff_output_) return;
        if (amount(source.book, key) == amount(global_, key)) source.different.erase(key);
        else source.different.insert(key);
    }
    void reset_source(Source& source) {
        remember_reset(source);
        if (!raw_diff_output_) {
            source.book.clear();
            return;
        }
        std::set<Key> touched;
        for (const auto& [key, value] : source.book) touched.insert(key);
        for (const auto& [key, value] : global_) touched.insert(key);
        source.book.clear();
        source.different = std::move(touched);
        for (auto it = source.different.begin(); it != source.different.end();) {
            if (amount(global_, *it) == 0) it = source.different.erase(it); else ++it;
        }
    }
    void invalidate(Source& source) {
        source.initialized = false;
        source.bridge = false;
        source.last_id = -1;
        reset_source(source);
    }
    bool valid(const Source& source) const {
        if (!source.initialized) return false;
        const auto ask = source.book.lower_bound({1, std::numeric_limits<I>::min()});
        if (ask == source.book.begin() || ask == source.book.end()) return false;
        const auto bid = std::prev(ask);
        if (bid->first.second >= ask->first.second) return false;
        I bids = 0, asks = 0;
        for (auto it = ask; it != source.book.end() && asks < minimum_levels_; ++it) ++asks;
        for (auto it = ask; it != source.book.begin() && bids < minimum_levels_;) { --it; ++bids; }
        return bids >= minimum_levels_ && asks >= minimum_levels_;
    }
    void apply_message(Source& source, const std::vector<Row>& rows) {
        ++messages_;
        const Row& message = rows.front();
        const bool snapshot = message[3] != 0, native = message[4] != 0;
        if (!native && message[1] < source.observed) { ++duplicates_; return; }
        for (const auto& row : rows) {
            if ((row[10] != 0 && row[10] != 1) || row[11] <= 0 || row[12] < 0) {
                ++invalid_; invalidate(source); return;
            }
        }
        if (snapshot) {
            const I snapshot_id = message[8] >= 0 ? message[8] : message[6];
            if (native && snapshot_id < 0) { ++invalid_; invalidate(source); return; }
            if (native && source.last_snapshot_e == message[1] && source.last_snapshot_id == snapshot_id) {
                ++duplicates_; return;
            }
            // An older recorder snapshot may not rewind a newer source state.
            if (native && source.initialized && snapshot_id < source.last_id) { ++duplicates_; return; }
            reset_source(source);
            source.initialized = true;
            source.bridge = native;
            source.last_id = snapshot_id;
            source.last_snapshot_e = message[1];
            source.last_snapshot_id = snapshot_id;
            source.observed = 0;
            source.local = 0;
        } else {
            if (!source.initialized) { ++pre_snapshot_; return; }
            if (native) {
                if (message[6] < 0) { ++invalid_; invalidate(source); return; }
                if (message[6] <= source.last_id) { ++duplicates_; return; }
                bool contiguous;
                if (source.bridge) {
                    contiguous = (message[5] >= 0 && message[5] <= source.last_id && source.last_id <= message[6])
                              || message[7] == source.last_id;
                } else {
                    contiguous = message[7] >= 0 ? message[7] == source.last_id
                               : message[5] >= 0 && message[5] <= source.last_id + 1;
                }
                if (!contiguous) { ++gaps_; invalidate(source); return; }
                source.bridge = false;
                source.last_id = message[6];
            }
        }
        for (const auto& row : rows) {
            Key key{row[10], row[11]};
            remember_level(source, key);
            if (row[12] == 0) source.book.erase(key);
            else source.book[key] = Level{row[12], row};
            mark_difference(source, key);
            source.observed = std::max(source.observed, row[1]);
            source.local = std::max<I>(0, row[2]);
        }
        source.last_presentation = message[0];
        source.last_message = message;
        ++accepted_;
    }

    void append(const Key& key, I quantity, const Row& provenance, const Source& source,
                I source_id, bool snapshot, I reason) {
        if (output_start_ > 0 && (pending_time_ < output_start_ || pending_time_ >= output_end_)) return;
        output_.timestamp.push_back(pending_time_);
        output_.observed.push_back(source.observed);
        output_.local.push_back(source.local);
        output_.source.push_back(source_id);
        output_.snapshot.push_back(snapshot);
        output_.side.push_back(key.first);
        output_.price.push_back(key.second);
        output_.amount.push_back(quantity);
        output_.reason.push_back(reason);
        output_.event.push_back(provenance[1]);
        output_.transaction.push_back(provenance[9]);
        output_.first.push_back(provenance[5]);
        output_.final.push_back(provenance[6]);
        output_.previous.push_back(provenance[7]);
        output_.last.push_back(provenance[8]);
        output_.source_row.push_back(provenance[13]);
        ++output_rows_;
    }
    void process_pending() {
        if (pending_.empty()) return;
        sample_until(pending_time_);
        if (pending_time_ >= output_start_) seed_opening();
        for (auto& [source_id, rows] : pending_) {
            std::map<MessageKey, std::vector<Row>> grouped;
            std::vector<MessageKey> order;
            for (const auto& row : rows) {
                const bool native = row[4] != 0, snapshot = row[3] != 0;
                MessageKey key = native
                    ? MessageKey{row[1], snapshot ? -1 : row[9], row[3], snapshot ? -1 : row[5], row[6], snapshot ? -1 : row[7], row[8]}
                    : MessageKey{row[1], row[2], row[3], -1, -1, -1, -1};
                if (!grouped.contains(key)) order.push_back(key);
                grouped[key].push_back(row);
            }
            if (rows.front()[4]) {
                std::stable_sort(order.begin(), order.end(), [&](const auto& left, const auto& right) {
                    const auto& l = grouped.at(left).front();
                    const auto& r = grouped.at(right).front();
                    const I li = l[3] && l[8] >= 0 ? l[8] : l[6];
                    const I ri = r[3] && r[8] >= 0 ? r[8] : r[6];
                    return std::pair{li, -l[3]} < std::pair{ri, -r[3]};
                });
            }
            for (const auto& key : order) apply_message(sources_[source_id], grouped.at(key));
        }
        pending_.clear();
        I selected = -1;
        for (I n = 0; n < static_cast<I>(sources_.size()); ++n) {
            if (!valid(sources_[n])) continue;
            if (selected < 0 || sources_[n].observed > sources_[selected].observed ||
                (sources_[n].observed == sources_[selected].observed && n == preferred_)) selected = n;
        }
        if (selected < 0) { ++unavailable_; retain_aged_view(); return; }
        auto& source = sources_[selected];
        if (source.observed > pending_time_) throw std::logic_error("fusion future observation");
        // An invalidated current source does not justify rewinding the last
        // published book to an older secondary state. Retain it as aged.
        if (source.observed < global_observed_) { ++older_suppressed_; retain_aged_view(); return; }
        if (selected == selected_ && source.last_presentation < pending_time_) { retain_aged_view(); return; }
        if (!raw_diff_output_) {
            if (selected_ >= 0 && selected != selected_) ++switches_;
            view_source_ = selected;
            global_.clear();
            discard_view_undo();
            ++selected_counts_[selected];
            selected_ = selected;
            global_observed_ = source.observed;
            global_local_ = source.local;
            return;
        }
        for (I n = 0; n < static_cast<I>(sources_.size()); ++n) {
            if (n == selected || !valid(sources_[n]) || sources_[n].observed != source.observed) continue;
            bool different = false;
            // Outside this small union both sources equal global_. Avoid a
            // full-depth scan on every same-clock provider comparison.
            for (const auto& key : source.different)
                if (amount(source.book, key) != amount(sources_[n].book, key)) { different = true; break; }
            if (!different) for (const auto& key : sources_[n].different)
                if (amount(source.book, key) != amount(sources_[n].book, key)) { different = true; break; }
            if (different) ++conflicts_;
        }
        const bool initial = selected_ < 0 || force_initial_snapshot_;
        const bool switched = !initial && selected != selected_;
        if (switched) ++switches_;
        std::vector<Key> changed(source.different.begin(), source.different.end());
        if (initial) {
            changed.clear();
            for (const auto& [key, value] : source.book) changed.push_back(key);
            // A daily opening snapshot is self-contained, while its preserved
            // observation clock still describes the inherited state honestly.
            global_.clear();
            for (auto& other : sources_) {
                other.different.clear();
                for (const auto& [key, value] : other.book) other.different.insert(key);
            }
        }
        for (const auto& key : changed) {
            const auto found = source.book.find(key);
            const I quantity = found == source.book.end() ? 0 : found->second.amount;
            const auto& provenance = found == source.book.end() ? source.last_message : found->second.origin;
            append(key, quantity, provenance, source, selected, initial, switched ? 1 : 0);
            if (quantity == 0) global_.erase(key); else global_[key] = found->second;
            for (auto& other : sources_) mark_difference(other, key);
        }
        if (changed.empty()) {
            const auto ask = source.book.lower_bound({1, std::numeric_limits<I>::min()});
            const auto bid = std::prev(ask);
            append(bid->first, bid->second.amount, source.last_message, source, selected, false, 2);
            ++refreshes_;
        }
        ++output_messages_;
        ++selected_counts_[selected];
        selected_ = selected;
        global_observed_ = source.observed;
        global_local_ = source.local;
        force_initial_snapshot_ = false;
    }
    py::dict take_output() {
        py::dict value;
#define FIELD(name) value[#name] = array_copy(output_.name)
        FIELD(timestamp); FIELD(observed); FIELD(local); FIELD(source); FIELD(snapshot);
        FIELD(side); FIELD(price); FIELD(amount); FIELD(reason); FIELD(event); FIELD(transaction);
        FIELD(first); FIELD(final); FIELD(previous); FIELD(last); FIELD(source_row);
        FIELD(normalized_timestamp); FIELD(normalized_observed); FIELD(normalized_local);
        FIELD(normalized_source); FIELD(normalized_levels); FIELD(normalized_carried);
#undef FIELD
        output_.clear();
        return value;
    }
    void sample_until(I boundary) {
        if (next_sample_ <= 0) return;
        while (next_sample_ <= boundary && next_sample_ < sample_end_) {
            const auto& book = view_book();
            const auto ask = book.lower_bound({1, std::numeric_limits<I>::min()});
            if (selected_ >= 0 && global_observed_ > 0 && global_observed_ < next_sample_
                    && ask != book.begin() && ask != book.end()) {
                auto bid = ask;
                auto offer = ask;
                std::vector<I> levels;
                levels.reserve(static_cast<std::size_t>(minimum_levels_ * 4));
                for (I level = 0; level < minimum_levels_ && bid != book.begin() && offer != book.end(); ++level) {
                    --bid;
                    levels.insert(levels.end(), {bid->first.second, bid->second.amount,
                                                offer->first.second, offer->second.amount});
                    ++offer;
                }
                if (static_cast<I>(levels.size()) == minimum_levels_ * 4) {
                    const bool carried = global_observed_ <= last_sample_observed_;
                    output_.normalized_timestamp.push_back(next_sample_ / 1000);
                    output_.normalized_observed.push_back(global_observed_);
                    output_.normalized_local.push_back(global_local_);
                    output_.normalized_source.push_back(selected_);
                    output_.normalized_carried.push_back(carried);
                    output_.normalized_levels.insert(output_.normalized_levels.end(), levels.begin(), levels.end());
                    ++normalized_rows_;
                    normalized_carried_ += carried;
                    max_age_ = std::max(max_age_, next_sample_ - global_observed_);
                    last_sample_observed_ = global_observed_;
                } else ++normalized_unknown_;
            } else ++normalized_unknown_;
            next_sample_ += sample_step_;
        }
    }
    void seed_opening() {
        if (!raw_diff_output_) return;
        if (opening_written_ || output_start_ <= 0) return;
        opening_written_ = true;
        if (global_.empty() || selected_ < 0 || global_observed_ >= output_start_) return;
        Source source;
        source.observed = global_observed_;
        source.local = global_local_;
        const I before = pending_time_;
        pending_time_ = output_start_;
        for (const auto& [key, level] : global_)
            append(key, level.amount, level.origin, source, selected_, true, 3);
        pending_time_ = before;
        ++output_messages_;
        // The carried opening snapshot already makes this daily raw file
        // self-contained; the next actual source event is an ordinary delta.
        force_initial_snapshot_ = false;
    }
    std::vector<Source> sources_;
    I preferred_, minimum_levels_, pending_time_ = -1, selected_ = -1;
    std::map<I, std::vector<Row>> pending_;
    std::map<Key, Level> global_;
    std::map<Key, Level> view_undo_, view_before_reset_;
    I view_source_ = -1, fallback_captures_ = 0;
    bool raw_diff_output_ = true, allow_legacy_continuation_ = false;
    bool legacy_continuation_seed_ = false, view_reset_saved_ = false;
    std::map<I, I> selected_counts_;
    Output output_;
    I raw_rows_ = 0, messages_ = 0, accepted_ = 0, invalid_ = 0, pre_snapshot_ = 0;
    I duplicates_ = 0, gaps_ = 0, switches_ = 0, output_rows_ = 0, output_messages_ = 0;
    I refreshes_ = 0, unavailable_ = 0, conflicts_ = 0;
    I global_observed_ = 0, older_suppressed_ = 0;
    I global_local_ = 0;
    I next_sample_ = 0, sample_end_ = 0, sample_step_ = 0, last_sample_observed_ = 0;
    I normalized_rows_ = 0, normalized_unknown_ = 0, normalized_carried_ = 0, max_age_ = 0;
    bool force_initial_snapshot_ = false;
    I output_start_ = 0, output_end_ = 0;
    bool opening_written_ = false;
};
}

void bind_book_fusion(py::module_& module) {
    module.attr("BOOK_FUSION_SCHEMA") = "narrowgate.book_fusion.v1";
    py::class_<BookFusion>(module, "BookFusion")
        .def(py::init<I, I, I>(), py::arg("sources"), py::arg("preferred_source") = 0,
             py::arg("minimum_levels") = 20)
        .def("push_rows", &BookFusion::push)
        .def("finish", &BookFusion::finish)
        .def("stats", &BookFusion::stats)
        .def("continuation", &BookFusion::state)
        .def("restore", &BookFusion::restore)
        .def("configure_sampling", &BookFusion::sample)
        .def("configure_raw_diff_output", &BookFusion::raw_diff_output,
             py::arg("enabled") = true, py::arg("allow_legacy_continuation") = false)
        .def("configure_output", &BookFusion::output_window);
}
}
