#include "live_cooldown.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace narrowgate_cpp {
namespace {

constexpr std::int64_t kWindowWidthNs = 100'000'000;
constexpr double kNanosecondsPerSecond = 1'000'000'000.0;
constexpr double kMillisecondsPerSecond = 1'000.0;
constexpr std::array<double, 3> kSellHalfLives{4.0, 16.0, 256.0};
constexpr std::array<double, 10> kBuyHalfLives{
    0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0,
};
constexpr std::string_view kSellShortPredicate =
    "predicate::ema_pair_h4s_h16s:cross_age_le_slow";
constexpr std::string_view kSellLongPredicate =
    "predicate::ema_pair_h16s_h256s:cross_age_le_fast";
constexpr std::string_view kCampaignPredicate =
    "predicate::m0::campaign_age_gt_control_duration";

constexpr F05TriState tri_not(F05TriState value) noexcept {
    if (value == F05TriState::Unobserved) {
        return value;
    }
    return value == F05TriState::True ? F05TriState::False
                                      : F05TriState::True;
}

bool fixed_action_matches(const F05BooleanRule& rule) {
    constexpr std::string_view prefix = "FIXED_";
    constexpr std::string_view suffix = "S";
    const std::string_view action = rule.action_id;
    if (!action.starts_with(prefix) || !action.ends_with(suffix) ||
        rule.duration_ms <= 0 || rule.duration_ms % 1'000 != 0) {
        return false;
    }
    const auto body = action.substr(
        prefix.size(),
        action.size() - prefix.size() - suffix.size()
    );
    if (body.empty()) {
        return false;
    }
    std::int64_t seconds = 0;
    for (const char value : body) {
        if (value < '0' || value > '9' ||
            seconds > (std::numeric_limits<std::int64_t>::max() - 9) / 10) {
            return false;
        }
        seconds = seconds * 10 + static_cast<std::int64_t>(value - '0');
    }
    return seconds > 0 &&
           seconds <= std::numeric_limits<std::int64_t>::max() / 1'000 &&
           seconds * 1'000 == rule.duration_ms;
}

}  // namespace

NativeLiveCooldownHotPath::NativeLiveCooldownHotPath(
    LiveCooldownProfile profile,
    const F05BooleanPolicy& policy,
    double warmup_s,
    double max_feature_age_s
)
    : profile_(profile),
      warmup_s_(warmup_s),
      max_feature_age_s_(max_feature_age_s) {
    if (!std::isfinite(warmup_s_) || warmup_s_ <= 0.0 ||
        !std::isfinite(max_feature_age_s_) || max_feature_age_s_ <= 0.0) {
        throw std::invalid_argument("live_cooldown_window_config_invalid");
    }
    if (profile_ == LiveCooldownProfile::BuyE3 && warmup_s_ < 2'048.0) {
        throw std::invalid_argument("live_cooldown_buy_warmup_too_short");
    }
    compile_policy(policy);
}

void NativeLiveCooldownHotPath::compile_policy(const F05BooleanPolicy& policy) {
    if (policy.default_action != kF05BooleanCooldownControlAction ||
        policy.predicate_columns.empty() || policy.rules.empty()) {
        throw std::invalid_argument("live_cooldown_policy_structure_invalid");
    }
    if (policy.predicate_columns.size() > kLiveCooldownMaxPredicates ||
        policy.rules.size() > kLiveCooldownMaxRules) {
        throw std::invalid_argument("live_cooldown_policy_capacity_exceeded");
    }
    predicate_count_ = static_cast<std::uint16_t>(policy.predicate_columns.size());
    rule_count_ = static_cast<std::uint16_t>(policy.rules.size());

    for (std::size_t index = 0; index < policy.predicate_columns.size(); ++index) {
        const auto& name = policy.predicate_columns[index];
        if (name.empty() ||
            std::find(policy.predicate_columns.begin(),
                      policy.predicate_columns.begin() + static_cast<std::ptrdiff_t>(index),
                      name) != policy.predicate_columns.begin() +
                                  static_cast<std::ptrdiff_t>(index)) {
            throw std::invalid_argument("live_cooldown_predicate_columns_invalid");
        }
        if (profile_ == LiveCooldownProfile::SellSelected) {
            if (name == kSellShortPredicate) {
                sell_short_predicate_index_ = static_cast<std::int16_t>(index);
            } else if (name == kSellLongPredicate) {
                sell_long_predicate_index_ = static_cast<std::int16_t>(index);
            } else if (name == kCampaignPredicate) {
                sell_campaign_predicate_index_ = static_cast<std::int16_t>(index);
            }
        }
    }

    for (std::size_t rule_index = 0; rule_index < policy.rules.size(); ++rule_index) {
        const auto& source_rule = policy.rules[rule_index];
        if (!fixed_action_matches(source_rule) || source_rule.clauses.empty()) {
            throw std::invalid_argument("live_cooldown_rule_invalid");
        }
        if (clause_count_ + source_rule.clauses.size() >
            kLiveCooldownMaxClauses) {
            throw std::invalid_argument("live_cooldown_clause_capacity_exceeded");
        }
        auto& target_rule = rules_[rule_index];
        target_rule.clause_offset = clause_count_;
        target_rule.clause_count =
            static_cast<std::uint16_t>(source_rule.clauses.size());
        target_rule.duration_ms = source_rule.duration_ms;
        for (const auto& source_clause : source_rule.clauses) {
            if (source_clause.literals.empty() ||
                literal_count_ + source_clause.literals.size() >
                    kLiveCooldownMaxLiterals) {
                throw std::invalid_argument("live_cooldown_literal_capacity_exceeded");
            }
            auto& target_clause = clauses_[clause_count_++];
            target_clause.literal_offset = literal_count_;
            target_clause.literal_count =
                static_cast<std::uint16_t>(source_clause.literals.size());
            for (const auto& source_literal : source_clause.literals) {
                if (source_literal.predicate_index >= predicate_count_) {
                    throw std::invalid_argument("live_cooldown_literal_unbound");
                }
                auto& target_literal = literals_[literal_count_++];
                target_literal.predicate_index =
                    static_cast<std::uint16_t>(source_literal.predicate_index);
                target_literal.negated = source_literal.negated;
            }
        }
    }

    if (profile_ == LiveCooldownProfile::SellSelected) {
        if (predicate_count_ != 3 || sell_short_predicate_index_ < 0 ||
            sell_long_predicate_index_ < 0 || sell_campaign_predicate_index_ < 0) {
            throw std::invalid_argument("live_cooldown_sell_predicates_drifted");
        }
        ema_count_ = static_cast<std::uint8_t>(kSellHalfLives.size());
        std::copy(kSellHalfLives.begin(), kSellHalfLives.end(), half_lives_s_.begin());
        pair_count_ = 2;
        pair_indices_[0] = PairIndexPod{0, 1};
        pair_indices_[1] = PairIndexPod{1, 2};
        return;
    }

    if (policy.ema_half_lives_s.size() != kBuyHalfLives.size() ||
        !std::equal(policy.ema_half_lives_s.begin(),
                    policy.ema_half_lives_s.end(), kBuyHalfLives.begin()) ||
        policy.predicate_pairs.size() != kLiveCooldownMaxPairs ||
        policy.predicate_definitions.size() != predicate_count_) {
        throw std::invalid_argument("live_cooldown_buy_feature_identity_drifted");
    }
    ema_count_ = static_cast<std::uint8_t>(kBuyHalfLives.size());
    std::copy(kBuyHalfLives.begin(), kBuyHalfLives.end(), half_lives_s_.begin());
    pair_count_ = static_cast<std::uint8_t>(policy.predicate_pairs.size());
    for (std::size_t index = 0; index < policy.predicate_pairs.size(); ++index) {
        const auto& pair = policy.predicate_pairs[index];
        if (pair.fast_ema_index >= ema_count_ || pair.slow_ema_index >= ema_count_ ||
            pair.fast_ema_index >= pair.slow_ema_index) {
            throw std::invalid_argument("live_cooldown_buy_pair_invalid");
        }
        pair_indices_[index] = PairIndexPod{
            static_cast<std::uint8_t>(pair.fast_ema_index),
            static_cast<std::uint8_t>(pair.slow_ema_index),
        };
    }
    for (const auto& source : policy.predicate_definitions) {
        if (source.predicate_index >= predicate_count_ ||
            definitions_[source.predicate_index].configured ||
            (source.metric != F05PredicateMetric::CampaignAgeGtControl &&
             source.pair_index >= pair_count_)) {
            throw std::invalid_argument("live_cooldown_buy_definition_invalid");
        }
        definitions_[source.predicate_index] = DefinitionPod{
            source.metric,
            static_cast<std::uint8_t>(source.pair_index),
            source.threshold_enabled,
            true,
            source.threshold,
        };
    }
    if (!std::all_of(definitions_.begin(),
                     definitions_.begin() + predicate_count_,
                     [](const DefinitionPod& value) { return value.configured; })) {
        throw std::invalid_argument("live_cooldown_buy_definition_missing");
    }
}

void NativeLiveCooldownHotPath::reset_feature_state() noexcept {
    pending_ = false;
    pending_left_ns_ = 0;
    pending_mid_ = 0.0;
    feature_ready_ts_ns_ = 0;
    warmup_start_right_ns_ = 0;
    last_window_right_ns_ = 0;
    ema_initialized_ = false;
    current_window_observed_ = false;
    last_observed_ts_ns_ = 0;
    ema_.fill(0.0);
    velocity_.fill(0.0);
    acceleration_.fill(0.0);
    pairs_.fill(PairStatePod{});
    audit_.warmup_admitted = false;
    audit_.feature_ready_ts_ns = 0;
    audit_.warmup_start_right_ts_ns = 0;
    audit_.last_window_right_ts_ns = 0;
    ++audit_.resets;
}

void NativeLiveCooldownHotPath::reset() noexcept {
    std::lock_guard lock(mutex_);
    reset_feature_state();
}

void NativeLiveCooldownHotPath::update_pair(
    std::size_t pair_index,
    double fast,
    double slow,
    std::int64_t timestamp_ns
) noexcept {
    const double distance = fast - slow;
    const std::int8_t sign = distance > 0.0 ? 1 : distance < 0.0 ? -1 : 0;
    if (sign == 0) {
        return;
    }
    auto& state = pairs_[pair_index];
    if (state.effective_sign == 0) {
        state.effective_sign = sign;
        state.arrangement_start_ts_ns = timestamp_ns;
    } else if (sign != state.effective_sign) {
        state.effective_sign = sign;
        state.arrangement_start_ts_ns = timestamp_ns;
        state.last_cross_ts_ns = timestamp_ns;
        state.last_cross_direction = sign;
    }
}

template <LiveCooldownProfile Profile>
void NativeLiveCooldownHotPath::update_ema(
    std::int64_t timestamp_ns,
    double value
) noexcept {
    constexpr std::size_t count =
        Profile == LiveCooldownProfile::BuyE3 ? kBuyHalfLives.size()
                                               : kSellHalfLives.size();
    current_window_observed_ = true;
    if (!ema_initialized_) {
        for (std::size_t index = 0; index < count; ++index) {
            ema_[index] = value;
            velocity_[index] = 0.0;
            acceleration_[index] = 0.0;
        }
        ema_initialized_ = true;
        last_observed_ts_ns_ = timestamp_ns;
        return;
    }

    const double delta_s =
        static_cast<double>(timestamp_ns - last_observed_ts_ns_) /
        kNanosecondsPerSecond;
    const auto previous = ema_;
    const auto previous_velocity = velocity_;
    for (std::size_t index = 0; index < count; ++index) {
        const double decay =
            std::exp(-std::log(2.0) * delta_s / half_lives_s_[index]);
        const double current =
            decay * previous[index] + (1.0 - decay) * value;
        ema_[index] = current;
        if constexpr (Profile == LiveCooldownProfile::BuyE3) {
            const double velocity = (current - previous[index]) / delta_s;
            velocity_[index] = velocity;
            acceleration_[index] =
                (velocity - previous_velocity[index]) / delta_s;
        }
    }
    if constexpr (Profile == LiveCooldownProfile::BuyE3) {
        for (std::size_t index = 0; index < pair_count_; ++index) {
            const auto pair = pair_indices_[index];
            update_pair(index, ema_[pair.fast], ema_[pair.slow], timestamp_ns);
        }
    } else {
        update_pair(0, ema_[0], ema_[1], timestamp_ns);
        update_pair(1, ema_[1], ema_[2], timestamp_ns);
    }
    last_observed_ts_ns_ = timestamp_ns;
}

void NativeLiveCooldownHotPath::emit_window(
    std::int64_t left_ns,
    std::int64_t feature_ready_ts_ns,
    double mid,
    bool source_gap
) noexcept {
    const std::int64_t right_ns = left_ns + kWindowWidthNs;
    if (source_gap) {
        current_window_observed_ = false;
        ++audit_.gap_windows;
    } else {
        if (warmup_start_right_ns_ == 0) {
            warmup_start_right_ns_ = right_ns;
        }
        if (profile_ == LiveCooldownProfile::BuyE3) {
            update_ema<LiveCooldownProfile::BuyE3>(right_ns, mid);
        } else {
            update_ema<LiveCooldownProfile::SellSelected>(right_ns, mid);
        }
    }
    last_window_right_ns_ = right_ns;
    feature_ready_ts_ns_ = feature_ready_ts_ns;
    ++audit_.completed_windows;
    if (warmup_start_right_ns_ != 0) {
        const double elapsed_s =
            static_cast<double>(right_ns - warmup_start_right_ns_) /
            kNanosecondsPerSecond;
        audit_.warmup_admitted = elapsed_s >= warmup_s_;
    }
    audit_.feature_ready_ts_ns = feature_ready_ts_ns_;
    audit_.warmup_start_right_ts_ns = warmup_start_right_ns_;
    audit_.last_window_right_ts_ns = last_window_right_ns_;
}

LiveCooldownObserveStatus NativeLiveCooldownHotPath::observe_depth(
    std::int64_t receive_ts_ns,
    double best_bid,
    double best_ask
) noexcept {
    std::lock_guard lock(mutex_);
    if (receive_ts_ns <= 0 || !std::isfinite(best_bid) ||
        !std::isfinite(best_ask) || !(best_bid > 0.0) ||
        !(best_bid < best_ask)) {
        ++audit_.invalid_updates;
        return LiveCooldownObserveStatus::InvalidCallback;
    }
    ++audit_.updates;
    const double mid = (best_bid + best_ask) / 2.0;
    if (!std::isfinite(mid)) {
        ++audit_.invalid_updates;
        return LiveCooldownObserveStatus::InvalidCallback;
    }
    const std::int64_t left_ns =
        (receive_ts_ns / kWindowWidthNs) * kWindowWidthNs;
    if (!pending_) {
        pending_ = true;
        pending_left_ns_ = left_ns;
        pending_mid_ = mid;
        return LiveCooldownObserveStatus::PendingOnly;
    }
    if (left_ns < pending_left_ns_) {
        ++audit_.out_of_order_updates;
        return LiveCooldownObserveStatus::OutOfOrderIgnored;
    }
    if (left_ns == pending_left_ns_) {
        pending_mid_ = mid;
        return LiveCooldownObserveStatus::PendingOnly;
    }

    const std::int64_t gap_windows =
        (left_ns - pending_left_ns_) / kWindowWidthNs - 1;
    const double gap_s =
        static_cast<double>(gap_windows * kWindowWidthNs) /
        kNanosecondsPerSecond;
    if (gap_s > max_feature_age_s_) {
        reset_feature_state();
        ++audit_.gap_resets;
        pending_ = true;
        pending_left_ns_ = left_ns;
        pending_mid_ = mid;
        return LiveCooldownObserveStatus::GapReset;
    }

    emit_window(pending_left_ns_, receive_ts_ns, pending_mid_, false);
    for (std::int64_t offset = 1; offset <= gap_windows; ++offset) {
        emit_window(
            pending_left_ns_ + offset * kWindowWidthNs,
            receive_ts_ns,
            0.0,
            true
        );
    }
    pending_left_ns_ = left_ns;
    pending_mid_ = mid;
    return LiveCooldownObserveStatus::Applied;
}

template <LiveCooldownProfile Profile>
LiveCooldownDecisionPod NativeLiveCooldownHotPath::evaluate_impl(
    std::int64_t decision_ts_ns,
    double campaign_age_s,
    std::int64_t baseline_duration_ms
) noexcept {
    LiveCooldownDecisionPod output;
    output.duration_ms = baseline_duration_ms;
    output.predicate_count = predicate_count_;
    output.feature_ready_ts_ns = feature_ready_ts_ns_;
    output.feature_age_ms = feature_ready_ts_ns_ > 0
        ? static_cast<double>(
              decision_ts_ns <= feature_ready_ts_ns_
                  ? 0
                  : decision_ts_ns - feature_ready_ts_ns_) /
              1'000'000.0
        : std::numeric_limits<double>::infinity();
    output.predicate_values.fill(
        static_cast<std::int8_t>(F05TriState::Unobserved)
    );
    if (feature_ready_ts_ns_ <= 0 || warmup_start_right_ns_ == 0) {
        output.status = LiveCooldownDecisionStatus::NoCompletedWindow;
        return output;
    }
    if (!audit_.warmup_admitted) {
        output.status = LiveCooldownDecisionStatus::WarmupIncomplete;
        return output;
    }
    if (output.feature_age_ms > max_feature_age_s_ * kMillisecondsPerSecond) {
        output.status = LiveCooldownDecisionStatus::FeatureStateStale;
        return output;
    }
    if (!current_window_observed_) {
        output.status = Profile == LiveCooldownProfile::BuyE3
            ? LiveCooldownDecisionStatus::SelectedPredicateUnobserved
            : LiveCooldownDecisionStatus::LatestWindowUnobserved;
        return output;
    }

    const auto write = [&](std::size_t index, F05TriState state) {
        output.predicate_values[index] = static_cast<std::int8_t>(state);
    };
    if constexpr (Profile == LiveCooldownProfile::SellSelected) {
        const auto cross_state = [&](const PairStatePod& pair) {
            if (pair.last_cross_ts_ns == 0) {
                return F05TriState::Unobserved;
            }
            const double age_s =
                (static_cast<double>(decision_ts_ns) -
                 static_cast<double>(pair.last_cross_ts_ns)) /
                kNanosecondsPerSecond;
            if (!std::isfinite(age_s) || age_s < 0.0) {
                return F05TriState::Unobserved;
            }
            return age_s <= 16.0 ? F05TriState::True : F05TriState::False;
        };
        write(static_cast<std::size_t>(sell_short_predicate_index_),
              cross_state(pairs_[0]));
        write(static_cast<std::size_t>(sell_long_predicate_index_),
              cross_state(pairs_[1]));
        write(static_cast<std::size_t>(sell_campaign_predicate_index_),
              campaign_age_s * 1'000.0 >
                      static_cast<double>(baseline_duration_ms)
                  ? F05TriState::True
                  : F05TriState::False);
    } else {
        for (std::size_t index = 0; index < predicate_count_; ++index) {
            const auto& definition = definitions_[index];
            if (definition.metric == F05PredicateMetric::CampaignAgeGtControl) {
                write(index,
                      !std::isfinite(campaign_age_s) || campaign_age_s < 0.0
                          ? F05TriState::Unobserved
                          : campaign_age_s * 1'000.0 >
                                    static_cast<double>(baseline_duration_ms)
                                ? F05TriState::True
                                : F05TriState::False);
                continue;
            }
            const auto pair_indices = pair_indices_[definition.pair_index];
            const auto& pair = pairs_[definition.pair_index];
            const double distance =
                ema_[pair_indices.fast] - ema_[pair_indices.slow];
            const double distance_velocity =
                velocity_[pair_indices.fast] - velocity_[pair_indices.slow];
            const double distance_acceleration =
                acceleration_[pair_indices.fast] -
                acceleration_[pair_indices.slow];
            const auto numeric = [&](double value, bool observed = true) {
                if (!observed || !std::isfinite(value) ||
                    !definition.threshold_enabled) {
                    return F05TriState::Unobserved;
                }
                return value >= definition.threshold ? F05TriState::True
                                                     : F05TriState::False;
            };
            switch (definition.metric) {
            case F05PredicateMetric::PositiveOrdering:
                write(index, pair.effective_sign == 0
                    ? F05TriState::Unobserved
                    : pair.effective_sign > 0 ? F05TriState::True
                                              : F05TriState::False);
                break;
            case F05PredicateMetric::LastCrossPositive:
                write(index, pair.last_cross_ts_ns == 0
                    ? F05TriState::Unobserved
                    : pair.last_cross_direction > 0 ? F05TriState::True
                                                     : F05TriState::False);
                break;
            case F05PredicateMetric::Expanding:
                write(index, distance * distance_velocity > 0.0
                    ? F05TriState::True : F05TriState::False);
                break;
            case F05PredicateMetric::Converging:
                write(index, distance * distance_velocity < 0.0
                    ? F05TriState::True : F05TriState::False);
                break;
            case F05PredicateMetric::AbsDistance:
                write(index, numeric(std::abs(distance)));
                break;
            case F05PredicateMetric::CrossAgeS:
                write(index, numeric(
                    pair.last_cross_ts_ns == 0
                        ? 0.0
                        : (static_cast<double>(decision_ts_ns) -
                           static_cast<double>(pair.last_cross_ts_ns)) /
                              kNanosecondsPerSecond,
                    pair.last_cross_ts_ns != 0));
                break;
            case F05PredicateMetric::ArrangementPersistenceS:
                write(index, numeric(
                    pair.arrangement_start_ts_ns == 0
                        ? 0.0
                        : (static_cast<double>(decision_ts_ns) -
                           static_cast<double>(pair.arrangement_start_ts_ns)) /
                              kNanosecondsPerSecond,
                    pair.arrangement_start_ts_ns != 0));
                break;
            case F05PredicateMetric::SignedDistance:
                write(index, numeric(distance));
                break;
            case F05PredicateMetric::SignedDistanceVelocity:
                write(index, numeric(distance_velocity));
                break;
            case F05PredicateMetric::SignedDistanceAcceleration:
                write(index, numeric(distance_acceleration));
                break;
            case F05PredicateMetric::CampaignAgeGtControl:
                break;
            }
        }
        if (std::any_of(
                output.predicate_values.begin(),
                output.predicate_values.begin() + predicate_count_,
                [](std::int8_t value) {
                    return value ==
                        static_cast<std::int8_t>(F05TriState::Unobserved);
                })) {
            output.status =
                LiveCooldownDecisionStatus::SelectedPredicateUnobserved;
            return output;
        }
    }

    const auto literal_state = [&](const LiteralPod& literal) {
        auto value = static_cast<F05TriState>(
            output.predicate_values[literal.predicate_index]
        );
        return literal.negated ? tri_not(value) : value;
    };
    const auto evaluate_clause = [&](const ClausePod& clause) {
        bool unobserved = false;
        for (std::size_t index = 0; index < clause.literal_count; ++index) {
            const auto value = literal_state(
                literals_[clause.literal_offset + index]
            );
            if (value == F05TriState::False) {
                return F05TriState::False;
            }
            unobserved = unobserved || value == F05TriState::Unobserved;
        }
        return unobserved ? F05TriState::Unobserved : F05TriState::True;
    };
    for (std::size_t rule_index = 0; rule_index < rule_count_; ++rule_index) {
        const auto& rule = rules_[rule_index];
        bool unobserved = false;
        bool matched = false;
        for (std::size_t clause_index = 0;
             clause_index < rule.clause_count;
             ++clause_index) {
            const auto state = evaluate_clause(
                clauses_[rule.clause_offset + clause_index]
            );
            if (state == F05TriState::True) {
                matched = true;
                break;
            }
            unobserved = unobserved || state == F05TriState::Unobserved;
        }
        if (matched) {
            output.status = LiveCooldownDecisionStatus::RuleMatched;
            output.duration_ms = rule.duration_ms;
            output.matched_rule_index = static_cast<std::int32_t>(rule_index);
            output.support_valid = true;
            return output;
        }
        if (unobserved) {
            output.status = LiveCooldownDecisionStatus::RuleUnobserved;
            output.detail_index = static_cast<std::int32_t>(rule_index);
            return output;
        }
    }
    output.status = LiveCooldownDecisionStatus::NoRuleMatched;
    output.support_valid = true;
    return output;
}

LiveCooldownDecisionPod NativeLiveCooldownHotPath::evaluate(
    std::int64_t decision_ts_ns,
    double campaign_age_s,
    std::int64_t baseline_duration_ms
) noexcept {
    std::lock_guard lock(mutex_);
    return profile_ == LiveCooldownProfile::BuyE3
        ? evaluate_impl<LiveCooldownProfile::BuyE3>(
              decision_ts_ns, campaign_age_s, baseline_duration_ms)
        : evaluate_impl<LiveCooldownProfile::SellSelected>(
              decision_ts_ns, campaign_age_s, baseline_duration_ms);
}

LiveCooldownAuditPod NativeLiveCooldownHotPath::audit() const noexcept {
    std::lock_guard lock(mutex_);
    return audit_;
}

LiveCooldownFeatureSnapshotPod
NativeLiveCooldownHotPath::feature_snapshot() const noexcept {
    std::lock_guard lock(mutex_);
    LiveCooldownFeatureSnapshotPod output;
    output.ema_count = ema_count_;
    output.pair_count = pair_count_;
    output.current_window_observed = current_window_observed_;
    output.ema_initialized = ema_initialized_;
    output.last_observed_ts_ns = last_observed_ts_ns_;
    output.ema = ema_;
    output.velocity = velocity_;
    output.acceleration = acceleration_;
    for (std::size_t index = 0; index < pair_count_; ++index) {
        output.effective_sign[index] = pairs_[index].effective_sign;
        output.last_cross_direction[index] = pairs_[index].last_cross_direction;
        output.arrangement_start_ts_ns[index] =
            pairs_[index].arrangement_start_ts_ns;
        output.last_cross_ts_ns[index] = pairs_[index].last_cross_ts_ns;
    }
    return output;
}

template void NativeLiveCooldownHotPath::update_ema<
    LiveCooldownProfile::SellSelected>(std::int64_t, double) noexcept;
template void NativeLiveCooldownHotPath::update_ema<
    LiveCooldownProfile::BuyE3>(std::int64_t, double) noexcept;

}  // namespace narrowgate_cpp
