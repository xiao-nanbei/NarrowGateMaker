#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace narrowgate_cpp {

inline constexpr std::string_view kF05RepeatedBooleanCooldownAbi =
    "f05_repeated_boolean_cooldown_streaming.v3";
inline constexpr std::string_view kF05BooleanCooldownControlAction =
    "CONTROL_85N";
inline constexpr std::int64_t kF05BooleanCooldownWindowWidthNs = 100'000'000;
inline constexpr std::int64_t kF05BooleanCooldownControlUnitMs = 85'000;

enum class F05TriState : std::int8_t {
    Unobserved = -1,
    False = 0,
    True = 1,
};

struct F05BooleanLiteral {
    std::size_t predicate_index = 0;
    bool negated = false;
    bool operator==(const F05BooleanLiteral&) const = default;
};

struct F05BooleanClause {
    std::vector<F05BooleanLiteral> literals;
    bool operator==(const F05BooleanClause&) const = default;
};

struct F05BooleanRule {
    std::string action_id;
    std::int64_t duration_ms = 0;
    std::vector<F05BooleanClause> clauses;
    bool operator==(const F05BooleanRule&) const = default;
};

enum class F05PredicateMetric : std::uint8_t {
    InventoryLifecycleAgeGtControl = 0,
    PositiveOrdering = 1,
    LastCrossPositive = 2,
    Expanding = 3,
    Converging = 4,
    AbsDistance = 5,
    CrossAgeS = 6,
    ArrangementPersistenceS = 7,
    SignedDistance = 8,
    SignedDistanceVelocity = 9,
    SignedDistanceAcceleration = 10,
};

struct F05PredicatePair {
    std::size_t fast_ema_index = 0;
    std::size_t slow_ema_index = 0;
    bool operator==(const F05PredicatePair&) const = default;
};

struct F05PredicateDefinition {
    std::size_t predicate_index = 0;
    F05PredicateMetric metric = F05PredicateMetric::InventoryLifecycleAgeGtControl;
    std::size_t pair_index = 0;
    bool threshold_enabled = false;
    double threshold = 0.0;
    bool operator==(const F05PredicateDefinition&) const = default;
};

struct F05BooleanPolicy {
    std::string policy_sha256;
    std::string predicate_bundle_sha256;
    std::vector<std::string> predicate_columns;
    std::vector<F05BooleanRule> rules;
    std::vector<double> ema_half_lives_s;
    std::vector<F05PredicatePair> predicate_pairs;
    std::vector<F05PredicateDefinition> predicate_definitions;
    std::string default_action = std::string(kF05BooleanCooldownControlAction);
    bool semantic_equal(const F05BooleanPolicy& other) const {
        return predicate_columns == other.predicate_columns && rules == other.rules &&
            ema_half_lives_s == other.ema_half_lives_s &&
            predicate_pairs == other.predicate_pairs &&
            predicate_definitions == other.predicate_definitions &&
            default_action == other.default_action;
    }
};

}  // namespace narrowgate_cpp
