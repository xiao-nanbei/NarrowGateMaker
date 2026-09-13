#include "f05_streaming_boolean_cooldown_parity.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <utility>

namespace narrowgate::f05::parity {
namespace {

class ProtocolError final : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

std::vector<std::string> split_tabs(const std::string& line) {
    std::vector<std::string> fields;
    std::size_t start = 0;
    while (true) {
        const auto end = line.find('\t', start);
        fields.emplace_back(line.substr(start, end - start));
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return fields;
}

std::vector<std::string> read_fields(std::istream& input, std::string_view tag) {
    std::string line;
    if (!std::getline(input, line)) {
        throw ProtocolError("unexpected_end_of_protocol:" + std::string(tag));
    }
    auto fields = split_tabs(line);
    if (fields.empty() || fields.front() != tag) {
        throw ProtocolError("unexpected_protocol_tag:" + std::string(tag));
    }
    return fields;
}

template <typename Integer>
Integer parse_integer(const std::string& value, std::string_view label) {
    Integer result{};
    const auto* begin = value.data();
    const auto* end = begin + value.size();
    const auto [position, error] = std::from_chars(begin, end, result);
    if (error != std::errc{} || position != end) {
        throw ProtocolError("invalid_integer:" + std::string(label));
    }
    return result;
}

bool parse_bool(const std::string& value, std::string_view label) {
    if (value == "0") {
        return false;
    }
    if (value == "1") {
        return true;
    }
    throw ProtocolError("invalid_bool:" + std::string(label));
}

double parse_double(const std::string& value, std::string_view label) {
    char* position = nullptr;
    const auto parsed = std::strtod(value.c_str(), &position);
    if (position != value.c_str() + value.size() || !std::isfinite(parsed)) {
        throw ProtocolError("invalid_double:" + std::string(label));
    }
    return parsed;
}

std::optional<double> parse_optional_double(
    const std::string& value,
    std::string_view label
) {
    if (value == "-") {
        return std::nullopt;
    }
    return parse_double(value, label);
}

std::string one_value(
    std::istream& input,
    std::string_view tag,
    std::string_view label
) {
    const auto fields = read_fields(input, tag);
    if (fields.size() != 2 || fields[1].empty()) {
        throw ProtocolError("invalid_protocol_field:" + std::string(label));
    }
    return fields[1];
}

bool is_lower_sha256(const std::string& value) {
    return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char ch) {
        return std::isdigit(static_cast<unsigned char>(ch)) != 0 ||
               (ch >= 'a' && ch <= 'f');
    });
}

std::optional<std::int64_t> fixed_duration_ms(const std::string& action) {
    constexpr std::string_view prefix = "FIXED_";
    if (!action.starts_with(prefix) || action.size() <= prefix.size() + 1 ||
        action.back() != 'S') {
        return std::nullopt;
    }
    const auto digits = std::string_view(action).substr(
        prefix.size(), action.size() - prefix.size() - 1
    );
    std::int64_t seconds{};
    const auto* begin = digits.data();
    const auto* end = begin + digits.size();
    const auto [position, error] = std::from_chars(begin, end, seconds);
    if (error != std::errc{} || position != end || seconds <= 0 ||
        seconds > std::numeric_limits<std::int64_t>::max() / 1'000) {
        return std::nullopt;
    }
    return seconds * 1'000;
}

bool is_sell_action(const std::string& action) {
    static const std::set<std::string> actions{
        "FIXED_79S",
        "FIXED_166S",
        "FIXED_211S",
        "FIXED_349S",
        "FIXED_660S",
        "FIXED_686S",
        "FIXED_1748S",
    };
    return actions.contains(action);
}

TriState tri_not(TriState value) {
    if (value == TriState::Unobserved) {
        return TriState::Unobserved;
    }
    return value == TriState::True ? TriState::False : TriState::True;
}

TriState evaluate_literal(const Literal& literal, const CaseInput& input) {
    const auto found = input.predicates.find(literal.predicate);
    if (found == input.predicates.end()) {
        throw ProtocolError("missing_policy_predicate:" + literal.predicate);
    }
    return literal.negated ? tri_not(found->second) : found->second;
}

TriState evaluate_clause(const Clause& clause, const CaseInput& input) {
    bool unobserved = false;
    for (const auto& literal : clause.literals) {
        const auto state = evaluate_literal(literal, input);
        if (state == TriState::False) {
            return TriState::False;
        }
        unobserved = unobserved || state == TriState::Unobserved;
    }
    return unobserved ? TriState::Unobserved : TriState::True;
}

TriState evaluate_rule(const Rule& rule, const CaseInput& input) {
    bool unobserved = false;
    for (const auto& clause : rule.clauses) {
        const auto state = evaluate_clause(clause, input);
        if (state == TriState::True) {
            return TriState::True;
        }
        unobserved = unobserved || state == TriState::Unobserved;
    }
    return unobserved ? TriState::Unobserved : TriState::False;
}

std::string clause_key(const Clause& clause) {
    std::string key;
    for (const auto& literal : clause.literals) {
        key.append(literal.predicate);
        key.push_back(literal.negated ? '1' : '0');
        key.push_back('\0');
    }
    return key;
}

std::string validate_policy(const Policy& policy) {
    if (!is_lower_sha256(policy.expected_file_sha256)) {
        return "expected_policy_sha256_is_not_sha256";
    }
    if (!is_lower_sha256(policy.file_sha256)) {
        return "policy_file_sha256_is_not_sha256";
    }
    if (policy.file_sha256 != policy.expected_file_sha256) {
        return "policy_file_sha256_mismatch";
    }
    if (policy.identity != kOwnerPolicyIdentity ||
        policy.schema_version != kOwnerPolicySchema ||
        policy.buy_selection != kControlAction) {
        return "owner_policy_identity_drifted";
    }
    if (policy.side != "SELL" || policy.default_action != kControlAction) {
        return "owner_policy_side_or_default_drifted";
    }
    if (policy.action_authorized || policy.live_authorized) {
        return "owner_policy_permissions_drifted";
    }
    if (policy.rules.empty()) {
        return "policy_rules_missing";
    }

    std::set<std::string> derived_predicates;
    for (const auto& rule : policy.rules) {
        if (!is_sell_action(rule.action) || fixed_duration_ms(rule.action) == std::nullopt) {
            return "boolean_policy_payload_invalid";
        }
        if (rule.clauses.empty()) {
            return "boolean_policy_payload_invalid";
        }
        std::string previous_clause_key;
        bool first_clause = true;
        for (const auto& clause : rule.clauses) {
            if (clause.literals.empty()) {
                return "boolean_policy_payload_invalid";
            }
            std::set<std::string> names;
            std::optional<std::tuple<std::string, bool>> previous_literal;
            for (const auto& literal : clause.literals) {
                if (literal.predicate.empty() || !names.insert(literal.predicate).second) {
                    return "boolean_policy_payload_invalid";
                }
                const auto current = std::tuple{literal.predicate, literal.negated};
                if (previous_literal.has_value() && current < *previous_literal) {
                    return "boolean_policy_payload_invalid";
                }
                previous_literal = current;
                derived_predicates.insert(literal.predicate);
            }
            const auto key = clause_key(clause);
            if (!first_clause && key <= previous_clause_key) {
                return "boolean_policy_payload_invalid";
            }
            previous_clause_key = key;
            first_clause = false;
        }
    }
    const std::vector<std::string> derived(
        derived_predicates.begin(), derived_predicates.end()
    );
    if (policy.predicate_columns != derived) {
        return "policy_predicate_columns_drifted";
    }
    const auto literal_matches = [](
                                     const Literal& literal,
                                     std::string_view predicate,
                                     bool negated
                                 ) {
        return literal.predicate == predicate && literal.negated == negated;
    };
    if (
        policy.rules.size() != 3 || policy.rules[0].action != "FIXED_1748S" ||
        policy.rules[0].clauses.size() != 2 ||
        policy.rules[0].clauses[0].literals.size() != 2 ||
        !literal_matches(
            policy.rules[0].clauses[0].literals[0], kShortCrossPredicate, false
        ) ||
        !literal_matches(
            policy.rules[0].clauses[0].literals[1], kCampaignAgePredicate, false
        ) ||
        policy.rules[0].clauses[1].literals.size() != 2 ||
        !literal_matches(
            policy.rules[0].clauses[1].literals[0], kShortCrossPredicate, true
        ) ||
        !literal_matches(
            policy.rules[0].clauses[1].literals[1], kCampaignAgePredicate, false
        ) ||
        policy.rules[1].action != "FIXED_166S" ||
        policy.rules[1].clauses.size() != 1 ||
        policy.rules[1].clauses[0].literals.size() != 2 ||
        !literal_matches(
            policy.rules[1].clauses[0].literals[0], kLongCrossPredicate, false
        ) ||
        !literal_matches(
            policy.rules[1].clauses[0].literals[1], kCampaignAgePredicate, true
        ) ||
        policy.rules[2].action != "FIXED_211S" ||
        policy.rules[2].clauses.size() != 1 ||
        policy.rules[2].clauses[0].literals.size() != 2 ||
        !literal_matches(
            policy.rules[2].clauses[0].literals[0], kLongCrossPredicate, true
        ) ||
        !literal_matches(
            policy.rules[2].clauses[0].literals[1], kCampaignAgePredicate, true
        )
    ) {
        return "owner_policy_streaming_literal_closure_drifted";
    }
    return {};
}

Decision control_decision(
    const Policy& policy,
    const CaseInput& input,
    std::string reason,
    bool support_valid,
    std::int64_t duration_ms
) {
    return Decision{
        .case_id = input.case_id,
        .snapshot_id = input.snapshot_id,
        .action_id = kControlAction,
        .duration_ms = duration_ms,
        .matched_rule_index = std::nullopt,
        .support_valid = support_valid,
        .policy_sha256 = policy.file_sha256,
        .fallback_reason = std::move(reason),
    };
}

Policy read_policy(std::istream& input) {
    const auto contract = read_fields(input, "CONTRACT");
    if (contract.size() != 2 || contract[1] != kProtocolIdentity) {
        throw ProtocolError("protocol_identity_drifted");
    }
    Policy policy;
    policy.identity = one_value(input, "POLICY_IDENTITY", "policy_identity");
    policy.schema_version = one_value(input, "POLICY_SCHEMA", "policy_schema");
    policy.file_sha256 = one_value(input, "POLICY_FILE_SHA256", "policy_file_sha256");
    policy.expected_file_sha256 = one_value(
        input, "EXPECTED_POLICY_FILE_SHA256", "expected_policy_file_sha256"
    );
    policy.side = one_value(input, "POLICY_SIDE", "policy_side");
    policy.default_action = one_value(input, "DEFAULT_ACTION", "default_action");
    policy.buy_selection = one_value(input, "SELECTION_BUY", "selection_buy");
    policy.action_authorized = parse_bool(
        one_value(input, "ACTION_AUTHORIZED", "action_authorized"),
        "action_authorized"
    );
    policy.live_authorized = parse_bool(
        one_value(input, "LIVE_AUTHORIZED", "live_authorized"),
        "live_authorized"
    );

    const auto predicate_header = read_fields(input, "PREDICATE_COLUMNS");
    if (predicate_header.size() != 2) {
        throw ProtocolError("invalid_predicate_columns_header");
    }
    const auto predicate_count = parse_integer<std::size_t>(
        predicate_header[1], "predicate_columns"
    );
    policy.predicate_columns.reserve(predicate_count);
    for (std::size_t index = 0; index < predicate_count; ++index) {
        policy.predicate_columns.push_back(
            one_value(input, "PREDICATE_COLUMN", "predicate_column")
        );
    }

    const auto rules_header = read_fields(input, "RULES");
    if (rules_header.size() != 2) {
        throw ProtocolError("invalid_rules_header");
    }
    const auto rule_count = parse_integer<std::size_t>(rules_header[1], "rules");
    policy.rules.reserve(rule_count);
    for (std::size_t rule_index = 0; rule_index < rule_count; ++rule_index) {
        const auto rule_fields = read_fields(input, "RULE");
        if (rule_fields.size() != 3) {
            throw ProtocolError("invalid_rule");
        }
        Rule rule;
        rule.action = rule_fields[1];
        const auto clause_count = parse_integer<std::size_t>(
            rule_fields[2], "rule_clause_count"
        );
        rule.clauses.reserve(clause_count);
        for (std::size_t clause_index = 0; clause_index < clause_count; ++clause_index) {
            const auto clause_fields = read_fields(input, "CLAUSE");
            if (clause_fields.size() != 2) {
                throw ProtocolError("invalid_clause");
            }
            Clause clause;
            const auto literal_count = parse_integer<std::size_t>(
                clause_fields[1], "clause_literal_count"
            );
            clause.literals.reserve(literal_count);
            for (std::size_t literal_index = 0; literal_index < literal_count;
                 ++literal_index) {
                const auto literal_fields = read_fields(input, "LITERAL");
                if (literal_fields.size() != 3) {
                    throw ProtocolError("invalid_literal");
                }
                clause.literals.push_back(Literal{
                    .predicate = literal_fields[1],
                    .negated = parse_bool(literal_fields[2], "literal_negated"),
                });
            }
            rule.clauses.push_back(std::move(clause));
        }
        policy.rules.push_back(std::move(rule));
    }
    return policy;
}

std::vector<CaseInput> read_cases(
    std::istream& input,
    const std::vector<std::string>& header
) {
    if (header.size() != 2) {
        throw ProtocolError("invalid_cases_header");
    }
    const auto count = parse_integer<std::size_t>(header[1], "cases");
    std::vector<CaseInput> cases;
    cases.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        const auto fields = read_fields(input, "CASE");
        if (fields.size() != 14) {
            throw ProtocolError("invalid_case");
        }
        CaseInput input_case;
        input_case.case_id = fields[1];
        input_case.snapshot_id = fields[2];
        input_case.feature_side = fields[3];
        input_case.m0_side = fields[4];
        input_case.role_at_fill = fields[5];
        input_case.baseline_duration_ms = parse_integer<std::int64_t>(
            fields[6], "baseline_duration_ms"
        );
        input_case.snapshot_baseline_duration_ms = parse_integer<std::int64_t>(
            fields[7], "snapshot_baseline_duration_ms"
        );
        input_case.policy_input_valid = parse_bool(fields[8], "policy_input_valid");
        input_case.feature_block = fields[9];
        input_case.support_valid = parse_bool(fields[10], "support_valid");
        input_case.channel_support_valid = parse_bool(
            fields[11], "channel_support_valid"
        );
        input_case.snapshot_fallback_reason = fields[12] == "-" ? "" : fields[12];
        const auto predicate_count = parse_integer<std::size_t>(
            fields[13], "case_predicate_count"
        );
        for (std::size_t predicate_index = 0; predicate_index < predicate_count;
             ++predicate_index) {
            const auto predicate_fields = read_fields(input, "PREDICATE");
            if (predicate_fields.size() != 3) {
                throw ProtocolError("invalid_case_predicate");
            }
            const auto raw_state = parse_integer<int>(
                predicate_fields[2], "predicate_state"
            );
            if (raw_state < -1 || raw_state > 1) {
                throw ProtocolError("predicate_not_three_valued:" + predicate_fields[1]);
            }
            if (!input_case.predicates.emplace(
                    predicate_fields[1], static_cast<TriState>(raw_state)
                ).second) {
                throw ProtocolError("duplicate_case_predicate:" + predicate_fields[1]);
            }
        }
        const auto end_case = read_fields(input, "END_CASE");
        if (end_case.size() != 1) {
            throw ProtocolError("invalid_end_case");
        }
        cases.push_back(std::move(input_case));
    }
    const auto end = read_fields(input, "END");
    if (end.size() != 1) {
        throw ProtocolError("invalid_protocol_end");
    }
    return cases;
}

void write_decision(std::ostream& output, const Decision& decision) {
    output << "RESULT\t" << decision.case_id << '\t' << decision.snapshot_id << '\t'
           << decision.action_id << '\t' << decision.duration_ms << '\t';
    if (decision.matched_rule_index.has_value()) {
        output << *decision.matched_rule_index;
    } else {
        output << -1;
    }
    output << '\t' << (decision.support_valid ? 1 : 0) << '\t'
           << decision.policy_sha256 << '\t'
           << (decision.fallback_reason.empty() ? "-" : decision.fallback_reason)
           << '\n';
}

}  // namespace

Evaluator::Evaluator(Policy policy)
    : policy_(std::move(policy)), binding_error_(validate_policy(policy_)) {}

const std::string& Evaluator::binding_error() const noexcept {
    return binding_error_;
}

Decision Evaluator::evaluate(const CaseInput& input) const {
    auto baseline_duration_ms = input.baseline_duration_ms;
    if (baseline_duration_ms <= 0) {
        return control_decision(
            policy_, input, "baseline_duration_ms_invalid", false,
            kMinimumControlDurationMs
        );
    }
    if (!binding_error_.empty()) {
        return control_decision(
            policy_, input, "runtime_binding_invalid:" + binding_error_, false,
            baseline_duration_ms
        );
    }
    if (!input.policy_input_valid) {
        const auto reason = input.snapshot_fallback_reason.empty()
                                ? "snapshot_policy_input_invalid"
                                : input.snapshot_fallback_reason;
        return control_decision(
            policy_, input, "snapshot_invalid:" + reason, false, baseline_duration_ms
        );
    }
    if (input.snapshot_baseline_duration_ms != baseline_duration_ms) {
        return control_decision(
            policy_, input, "snapshot_baseline_duration_drifted", false,
            baseline_duration_ms
        );
    }
    if (input.feature_side != input.m0_side ||
        (input.feature_side != "BUY" && input.feature_side != "SELL")) {
        return control_decision(
            policy_, input, "snapshot_side_inconsistent", false, baseline_duration_ms
        );
    }
    if (input.role_at_fill != "opener" && input.role_at_fill != "add") {
        return control_decision(
            policy_, input, "snapshot_invalid:role_not_exposure_increasing", false,
            baseline_duration_ms
        );
    }
    if (input.feature_block != "M2") {
        return control_decision(
            policy_, input, "snapshot_feature_block_not_m2", false,
            baseline_duration_ms
        );
    }
    if (!input.support_valid || !input.channel_support_valid) {
        return control_decision(
            policy_, input, "snapshot_m2_support_invalid", false,
            baseline_duration_ms
        );
    }
    if (input.feature_side == "BUY") {
        return control_decision(
            policy_, input, "buy_control_by_contract", true, baseline_duration_ms
        );
    }

    try {
        for (std::size_t index = 0; index < policy_.rules.size(); ++index) {
            const auto& rule = policy_.rules[index];
            const auto state = evaluate_rule(rule, input);
            if (state == TriState::True) {
                const auto duration = fixed_duration_ms(rule.action);
                if (!duration.has_value()) {
                    throw ProtocolError("unsupported_duration_action:" + rule.action);
                }
                return Decision{
                    .case_id = input.case_id,
                    .snapshot_id = input.snapshot_id,
                    .action_id = rule.action,
                    .duration_ms = *duration,
                    .matched_rule_index = index,
                    .support_valid = true,
                    .policy_sha256 = policy_.file_sha256,
                    .fallback_reason = {},
                };
            }
            if (state == TriState::Unobserved) {
                return control_decision(
                    policy_, input, "rule_unobserved:" + std::to_string(index), false,
                    baseline_duration_ms
                );
            }
        }
    } catch (const ProtocolError& error) {
        return control_decision(
            policy_, input, error.what(), false, baseline_duration_ms
        );
    }
    return control_decision(
        policy_, input, "no_rule_matched", true, baseline_duration_ms
    );
}

namespace {

void update_streaming_pair(
    StreamingPairState& state,
    double fast,
    double slow,
    std::int64_t timestamp
) {
    const auto distance = fast - slow;
    const auto raw_sign = distance > 0.0 ? 1 : distance < 0.0 ? -1 : 0;
    if (raw_sign == 0) {
        return;
    }
    if (state.effective_sign == 0) {
        state.effective_sign = raw_sign;
        state.arrangement_start_ts_ns = timestamp;
        return;
    }
    if (raw_sign != state.effective_sign) {
        state.effective_sign = raw_sign;
        state.arrangement_start_ts_ns = timestamp;
        state.last_cross_ts_ns = timestamp;
        state.last_cross_direction = raw_sign;
    }
}

std::optional<double> streaming_cross_age(
    const StreamingPairState& state,
    bool observed,
    std::int64_t decision_ts_ns
) {
    if (!observed || !state.last_cross_ts_ns.has_value()) {
        return std::nullopt;
    }
    const auto delta = decision_ts_ns - *state.last_cross_ts_ns;
    if (delta < 0) {
        return std::nullopt;
    }
    return static_cast<double>(delta) / 1'000'000'000.0;
}

TriState cross_age_predicate(const std::optional<double>& age) {
    if (!age.has_value() || !std::isfinite(*age) || *age < 0.0) {
        return TriState::Unobserved;
    }
    return *age <= 16.0 ? TriState::True : TriState::False;
}

TriState campaign_age_predicate(
    const std::optional<double>& age,
    std::int64_t baseline_duration_ms
) {
    if (!age.has_value() || !std::isfinite(*age) || *age < 0.0) {
        return TriState::Unobserved;
    }
    return *age * 1'000.0 > static_cast<double>(baseline_duration_ms)
               ? TriState::True
               : TriState::False;
}

void validate_streaming_checkpoint(const StreamingCheckpoint& checkpoint) {
    if (checkpoint.warmup_admitted && checkpoint.warmup_identity.empty()) {
        throw ProtocolError("admitted_warmup_requires_bound_identity");
    }
    if (checkpoint.ema_initialized != checkpoint.last_observed_ts_ns.has_value()) {
        throw ProtocolError("streaming_checkpoint_ema_clock_inconsistent");
    }
    if (checkpoint.ema_initialized &&
        !std::all_of(checkpoint.ema.begin(), checkpoint.ema.end(), [](double value) {
            return std::isfinite(value);
        })) {
        throw ProtocolError("streaming_checkpoint_ema_nonfinite");
    }
    for (const auto* pair : {&checkpoint.short_pair, &checkpoint.long_pair}) {
        if (pair->effective_sign < -1 || pair->effective_sign > 1 ||
            pair->last_cross_direction < -1 || pair->last_cross_direction > 1) {
            throw ProtocolError("streaming_checkpoint_pair_sign_invalid");
        }
        if (pair->last_cross_ts_ns.has_value() && pair->last_cross_direction == 0) {
            throw ProtocolError("streaming_checkpoint_cross_direction_missing");
        }
    }
}

}  // namespace

MinimalMidStreamingState::MinimalMidStreamingState(
    bool warmup_admitted,
    std::string warmup_identity
) {
    state_.warmup_admitted = warmup_admitted;
    state_.warmup_identity = std::move(warmup_identity);
    validate_streaming_checkpoint(state_);
}

void MinimalMidStreamingState::update(const StreamingObservation& observation) {
    if (observation.right_ts_ns - observation.left_ts_ns != kBaseWindowWidthNs) {
        throw ProtocolError("window_width_drifted");
    }
    if (observation.left_ts_ns % kBaseWindowWidthNs != 0 ||
        observation.right_ts_ns % kBaseWindowWidthNs != 0) {
        throw ProtocolError("window_not_aligned_to_frozen_grid");
    }
    if (observation.feature_ready_ts_ns < observation.right_ts_ns) {
        throw ProtocolError("window_ready_before_right_edge");
    }
    if (state_.last_right_ts_ns.has_value()) {
        if (observation.right_ts_ns <= *state_.last_right_ts_ns) {
            throw ProtocolError("window_right_edge_did_not_increase");
        }
        if (observation.left_ts_ns != *state_.last_right_ts_ns) {
            throw ProtocolError("missing_windows_not_explicit_source_gaps");
        }
        if (observation.feature_ready_ts_ns < *state_.last_feature_ready_ts_ns) {
            throw ProtocolError("feature_ready_clock_regressed");
        }
        if (observation.market_generation <= *state_.last_market_generation) {
            throw ProtocolError("market_generation_did_not_increase");
        }
        if (observation.depth_generation < *state_.last_depth_generation) {
            throw ProtocolError("depth_generation_regressed");
        }
    }

    const auto invalid_window = observation.source_gap || observation.source_stale;
    if (invalid_window) {
        ++state_.gap_window_count;
    }
    const auto observed = !invalid_window && observation.mid_usdc_per_btc.has_value();
    state_.current_window_observed = observed;
    if (observed) {
        const auto value = *observation.mid_usdc_per_btc;
        if (!std::isfinite(value)) {
            throw ProtocolError("observed_mid_is_nonfinite");
        }
        if (!state_.ema_initialized) {
            state_.ema = {value, value, value};
            state_.ema_initialized = true;
            state_.last_observed_ts_ns = observation.right_ts_ns;
        } else {
            if (observation.right_ts_ns <= *state_.last_observed_ts_ns) {
                throw ProtocolError("channel_ema_clock_did_not_increase");
            }
            constexpr std::array<double, 3> half_lives{4.0, 16.0, 256.0};
            const auto delta_s = static_cast<double>(
                                     observation.right_ts_ns -
                                     *state_.last_observed_ts_ns
                                 ) /
                                 1'000'000'000.0;
            const auto prior = state_.ema;
            for (std::size_t index = 0; index < state_.ema.size(); ++index) {
                const auto decay = std::exp(-std::log(2.0) * delta_s /
                                            half_lives[index]);
                state_.ema[index] = decay * prior[index] + (1.0 - decay) * value;
            }
            update_streaming_pair(
                state_.short_pair, state_.ema[0], state_.ema[1],
                observation.right_ts_ns
            );
            update_streaming_pair(
                state_.long_pair, state_.ema[1], state_.ema[2],
                observation.right_ts_ns
            );
            state_.last_observed_ts_ns = observation.right_ts_ns;
        }
    }
    state_.last_right_ts_ns = observation.right_ts_ns;
    state_.last_feature_ready_ts_ns = observation.feature_ready_ts_ns;
    state_.last_market_generation = observation.market_generation;
    state_.last_depth_generation = observation.depth_generation;
    ++state_.window_count;
}

StreamingCheckpoint MinimalMidStreamingState::checkpoint() const {
    return state_;
}

void MinimalMidStreamingState::restore(const StreamingCheckpoint& checkpoint) {
    validate_streaming_checkpoint(checkpoint);
    state_ = checkpoint;
}

void MinimalMidStreamingState::reset_unbound() {
    state_ = StreamingCheckpoint{};
}

StreamingSnapshot MinimalMidStreamingState::materialize(
    const StreamingObservation& observation,
    const Evaluator& evaluator
) const {
    if (!state_.last_right_ts_ns.has_value() ||
        *state_.last_right_ts_ns != observation.right_ts_ns) {
        throw ProtocolError("streaming_materialization_window_drifted");
    }
    if (observation.decision_ts_ns < observation.feature_ready_ts_ns) {
        throw ProtocolError("feature_ready_state_crossed_decision_cutoff");
    }
    const auto short_age = streaming_cross_age(
        state_.short_pair, state_.current_window_observed,
        observation.decision_ts_ns
    );
    const auto long_age = streaming_cross_age(
        state_.long_pair, state_.current_window_observed,
        observation.decision_ts_ns
    );
    std::map<std::string, TriState> predicates{
        {kShortCrossPredicate, cross_age_predicate(short_age)},
        {kCampaignAgePredicate,
         campaign_age_predicate(
             observation.campaign_age_s, observation.baseline_duration_ms
         )},
        {kLongCrossPredicate, cross_age_predicate(long_age)},
    };
    const auto support = state_.warmup_admitted && state_.current_window_observed;
    CaseInput input_case{
        .case_id = observation.case_id,
        .snapshot_id = observation.snapshot_id,
        .feature_side = observation.feature_side,
        .m0_side = observation.m0_side,
        .role_at_fill = observation.role_at_fill,
        .baseline_duration_ms = observation.baseline_duration_ms,
        .snapshot_baseline_duration_ms =
            observation.snapshot_baseline_duration_ms,
        .policy_input_valid = observation.policy_input_valid,
        .feature_block = observation.feature_block,
        .support_valid = support,
        .channel_support_valid = state_.current_window_observed,
        .snapshot_fallback_reason = {},
        .predicates = predicates,
    };
    std::array<std::optional<double>, 3> ema;
    if (state_.ema_initialized) {
        for (std::size_t index = 0; index < ema.size(); ++index) {
            ema[index] = state_.ema[index];
        }
    }
    return StreamingSnapshot{
        .case_id = observation.case_id,
        .snapshot_id = observation.snapshot_id,
        .right_ts_ns = observation.right_ts_ns,
        .feature_ready_ts_ns = observation.feature_ready_ts_ns,
        .decision_ts_ns = observation.decision_ts_ns,
        .market_generation = observation.market_generation,
        .depth_generation = observation.depth_generation,
        .window_count = state_.window_count,
        .gap_window_count = state_.gap_window_count,
        .current_window_observed = state_.current_window_observed,
        .warmup_admitted = state_.warmup_admitted,
        .support_valid = support,
        .last_observed_ts_ns = state_.last_observed_ts_ns,
        .ema = ema,
        .short_pair = state_.short_pair,
        .long_pair = state_.long_pair,
        .short_cross_age_s = short_age,
        .long_cross_age_s = long_age,
        .predicates = predicates,
        .decision = evaluator.evaluate(input_case),
    };
}

namespace {

StreamingObservation parse_streaming_observation(
    const std::vector<std::string>& fields
) {
    if (fields.size() != 20) {
        throw ProtocolError("invalid_stream_observation");
    }
    return StreamingObservation{
        .case_id = fields[1],
        .snapshot_id = fields[2],
        .left_ts_ns = parse_integer<std::int64_t>(fields[3], "left_ts_ns"),
        .right_ts_ns = parse_integer<std::int64_t>(fields[4], "right_ts_ns"),
        .feature_ready_ts_ns =
            parse_integer<std::int64_t>(fields[5], "feature_ready_ts_ns"),
        .decision_ts_ns =
            parse_integer<std::int64_t>(fields[6], "decision_ts_ns"),
        .market_generation =
            parse_integer<std::int64_t>(fields[7], "market_generation"),
        .depth_generation =
            parse_integer<std::int64_t>(fields[8], "depth_generation"),
        .mid_usdc_per_btc = parse_optional_double(fields[9], "mid_usdc_per_btc"),
        .source_gap = parse_bool(fields[10], "source_gap"),
        .source_stale = parse_bool(fields[11], "source_stale"),
        .feature_side = fields[12],
        .m0_side = fields[13],
        .role_at_fill = fields[14],
        .baseline_duration_ms =
            parse_integer<std::int64_t>(fields[15], "baseline_duration_ms"),
        .snapshot_baseline_duration_ms = parse_integer<std::int64_t>(
            fields[16], "snapshot_baseline_duration_ms"
        ),
        .campaign_age_s = parse_optional_double(fields[17], "campaign_age_s"),
        .policy_input_valid = parse_bool(fields[18], "policy_input_valid"),
        .feature_block = fields[19],
    };
}

void write_optional_i64(
    std::ostream& output,
    const std::optional<std::int64_t>& value
) {
    if (value.has_value()) {
        output << *value;
    } else {
        output << '-';
    }
}

void write_optional_double(
    std::ostream& output,
    const std::optional<double>& value
) {
    if (value.has_value()) {
        output << std::setprecision(17) << *value;
    } else {
        output << '-';
    }
}

void write_streaming_snapshot(
    std::ostream& output,
    const StreamingSnapshot& snapshot
) {
    output << "STREAM_RESULT\t" << snapshot.case_id << '\t'
           << snapshot.snapshot_id << '\t' << snapshot.right_ts_ns << '\t'
           << snapshot.feature_ready_ts_ns << '\t' << snapshot.decision_ts_ns
           << '\t' << snapshot.market_generation << '\t'
           << snapshot.depth_generation << '\t' << snapshot.window_count << '\t'
           << snapshot.gap_window_count << '\t'
           << (snapshot.current_window_observed ? 1 : 0) << '\t'
           << (snapshot.warmup_admitted ? 1 : 0) << '\t'
           << (snapshot.support_valid ? 1 : 0) << '\t';
    write_optional_i64(output, snapshot.last_observed_ts_ns);
    for (const auto& value : snapshot.ema) {
        output << '\t';
        write_optional_double(output, value);
    }
    output << '\t' << snapshot.short_pair.effective_sign << '\t';
    write_optional_i64(output, snapshot.short_pair.arrangement_start_ts_ns);
    output << '\t';
    write_optional_i64(output, snapshot.short_pair.last_cross_ts_ns);
    output << '\t' << snapshot.short_pair.last_cross_direction << '\t';
    write_optional_double(output, snapshot.short_cross_age_s);
    output << '\t' << snapshot.long_pair.effective_sign << '\t';
    write_optional_i64(output, snapshot.long_pair.arrangement_start_ts_ns);
    output << '\t';
    write_optional_i64(output, snapshot.long_pair.last_cross_ts_ns);
    output << '\t' << snapshot.long_pair.last_cross_direction << '\t';
    write_optional_double(output, snapshot.long_cross_age_s);
    output << '\t' << static_cast<int>(snapshot.predicates.at(kShortCrossPredicate))
           << '\t'
           << static_cast<int>(snapshot.predicates.at(kCampaignAgePredicate))
           << '\t' << static_cast<int>(snapshot.predicates.at(kLongCrossPredicate))
           << '\t' << snapshot.decision.action_id << '\t'
           << snapshot.decision.duration_ms << '\t';
    if (snapshot.decision.matched_rule_index.has_value()) {
        output << *snapshot.decision.matched_rule_index;
    } else {
        output << -1;
    }
    output << '\t' << (snapshot.decision.support_valid ? 1 : 0) << '\t'
           << snapshot.decision.policy_sha256 << '\t'
           << (snapshot.decision.fallback_reason.empty()
                   ? "-"
                   : snapshot.decision.fallback_reason)
           << '\n';
}

void run_stream_protocol(
    Policy policy,
    std::istream& input,
    std::ostream& output,
    const std::vector<std::string>& init
) {
    if (init.size() != 3) {
        throw ProtocolError("invalid_stream_init");
    }
    const auto warmup_admitted = parse_bool(init[1], "warmup_admitted");
    const auto warmup_identity = init[2] == "-" ? "" : init[2];
    MinimalMidStreamingState state(warmup_admitted, warmup_identity);
    const Evaluator evaluator(std::move(policy));
    std::map<std::string, StreamingCheckpoint> checkpoints;

    const auto command_header = read_fields(input, "STREAM_COMMANDS");
    if (command_header.size() != 2) {
        throw ProtocolError("invalid_stream_commands_header");
    }
    const auto command_count = parse_integer<std::size_t>(
        command_header[1], "stream_command_count"
    );
    for (std::size_t index = 0; index < command_count; ++index) {
        std::string line;
        if (!std::getline(input, line)) {
            throw ProtocolError("unexpected_end_of_stream_commands");
        }
        const auto fields = split_tabs(line);
        if (fields.empty()) {
            throw ProtocolError("empty_stream_command");
        }
        if (fields[0] == "OBSERVE") {
            auto observation = parse_streaming_observation(fields);
            state.update(observation);
            write_streaming_snapshot(
                output, state.materialize(observation, evaluator)
            );
        } else if (fields[0] == "SAVE") {
            if (fields.size() != 2 || fields[1].empty() ||
                !checkpoints.emplace(fields[1], state.checkpoint()).second) {
                throw ProtocolError("invalid_or_duplicate_stream_checkpoint");
            }
        } else if (fields[0] == "RESTORE") {
            if (fields.size() != 2) {
                throw ProtocolError("invalid_stream_restore");
            }
            const auto found = checkpoints.find(fields[1]);
            if (found == checkpoints.end()) {
                throw ProtocolError("stream_checkpoint_not_found");
            }
            state.restore(found->second);
        } else if (fields[0] == "RESET_UNBOUND") {
            if (fields.size() != 1) {
                throw ProtocolError("invalid_stream_reset");
            }
            state.reset_unbound();
        } else {
            throw ProtocolError("unknown_stream_command:" + fields[0]);
        }
    }
    const auto end = read_fields(input, "END");
    if (end.size() != 1) {
        throw ProtocolError("invalid_protocol_end");
    }
}

}  // namespace

int run_cli(std::istream& input, std::ostream& output, std::ostream& error) {
    try {
        auto policy = read_policy(input);
        std::string line;
        if (!std::getline(input, line)) {
            throw ProtocolError("unexpected_end_after_policy");
        }
        const auto header = split_tabs(line);
        if (!header.empty() && header[0] == "CASES") {
            auto cases = read_cases(input, header);
            const Evaluator evaluator(std::move(policy));
            for (const auto& input_case : cases) {
                write_decision(output, evaluator.evaluate(input_case));
            }
        } else if (!header.empty() && header[0] == "STREAM_INIT") {
            run_stream_protocol(std::move(policy), input, output, header);
        } else {
            throw ProtocolError("unknown_protocol_payload");
        }
        return 0;
    } catch (const std::exception& exception) {
        error << "protocol_error:" << exception.what() << '\n';
        return 2;
    }
}

}  // namespace narrowgate::f05::parity

int main() {
    return narrowgate::f05::parity::run_cli(std::cin, std::cout, std::cerr);
}
