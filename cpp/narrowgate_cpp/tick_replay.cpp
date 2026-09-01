#include "tick_replay.hpp"
#include "dynamic_fill_hazard.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cmath>
#include <cstdio>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory_resource>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace narrowgate_cpp {
namespace {

constexpr std::uint8_t kSyncAdjustEventCode = 4;
constexpr std::uint8_t kSyncCensorEventCode = 5;
constexpr std::uint8_t kP3ReachBudgetUnsupported = 255;
constexpr std::int64_t kP3ReachBudgetBucketMs = 10'000;
constexpr std::size_t kP3ReachBudgetGridSize = 1'180;
constexpr int kP3ReachBudgetGridMinTicks = 5;
constexpr std::string_view kLossCooldownSemantics =
    "round_trip_realized_pnl_policy_clock_flip_fee_split_v2";
constexpr std::string_view kLossCooldownSnapshotSchema =
    "narrowgate_loss_cooldown_snapshot.v2";
constexpr std::string_view kSyncAdjustSemantics =
    "system_event_after_market_same_ms_v1";
constexpr std::string_view kF05ShortCrossPredicate =
    "predicate::ema_pair_h4s_h16s:cross_age_le_slow";
constexpr std::string_view kF05LongCrossPredicate =
    "predicate::ema_pair_h16s_h256s:cross_age_le_fast";
constexpr std::string_view kF05CampaignAgePredicate =
    "predicate::m0::campaign_age_gt_control_duration";
constexpr std::array<double, 3> kF05SelectedHalfLivesS{4.0, 16.0, 256.0};
constexpr double kF05CrossAgeThresholdS = 16.0;

bool is_lower_sha256(std::string_view value) {
  return value.size() == 64 &&
         std::all_of(value.begin(), value.end(), [](char ch) {
           return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         });
}

std::uint32_t f05_sha_rotr(std::uint32_t value, std::uint32_t count) {
  return (value >> count) | (value << (32U - count));
}

std::string f05_sha256(std::string_view input) {
  static constexpr std::array<std::uint32_t, 64> k{
      0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU,
      0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U,
      0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U,
      0xc19bf174U, 0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
      0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU, 0x983e5152U,
      0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
      0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU,
      0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
      0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U,
      0xd6990624U, 0xf40e3585U, 0x106aa070U, 0x19a4c116U, 0x1e376c08U,
      0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU,
      0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
      0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
  };
  std::vector<std::uint8_t> bytes(input.begin(), input.end());
  const auto bit_length = static_cast<std::uint64_t>(bytes.size()) * 8U;
  bytes.push_back(0x80U);
  while (bytes.size() % 64 != 56) {
    bytes.push_back(0U);
  }
  for (int shift = 56; shift >= 0; shift -= 8) {
    bytes.push_back(static_cast<std::uint8_t>(bit_length >> shift));
  }
  std::array<std::uint32_t, 8> hash{
      0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
      0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U,
  };
  for (std::size_t offset = 0; offset < bytes.size(); offset += 64) {
    std::array<std::uint32_t, 64> words{};
    for (std::size_t index = 0; index < 16; ++index) {
      const auto base = offset + index * 4;
      words[index] = (static_cast<std::uint32_t>(bytes[base]) << 24U) |
                     (static_cast<std::uint32_t>(bytes[base + 1]) << 16U) |
                     (static_cast<std::uint32_t>(bytes[base + 2]) << 8U) |
                     static_cast<std::uint32_t>(bytes[base + 3]);
    }
    for (std::size_t index = 16; index < words.size(); ++index) {
      const auto s0 = f05_sha_rotr(words[index - 15], 7U) ^
                      f05_sha_rotr(words[index - 15], 18U) ^
                      (words[index - 15] >> 3U);
      const auto s1 = f05_sha_rotr(words[index - 2], 17U) ^
                      f05_sha_rotr(words[index - 2], 19U) ^
                      (words[index - 2] >> 10U);
      words[index] = words[index - 16] + s0 + words[index - 7] + s1;
    }
    auto a = hash[0];
    auto b = hash[1];
    auto c = hash[2];
    auto d = hash[3];
    auto e = hash[4];
    auto f = hash[5];
    auto g = hash[6];
    auto h = hash[7];
    for (std::size_t index = 0; index < words.size(); ++index) {
      const auto s1 =
          f05_sha_rotr(e, 6U) ^ f05_sha_rotr(e, 11U) ^ f05_sha_rotr(e, 25U);
      const auto choice = (e & f) ^ ((~e) & g);
      const auto temp1 = h + s1 + choice + k[index] + words[index];
      const auto s0 =
          f05_sha_rotr(a, 2U) ^ f05_sha_rotr(a, 13U) ^ f05_sha_rotr(a, 22U);
      const auto majority = (a & b) ^ (a & c) ^ (b & c);
      const auto temp2 = s0 + majority;
      h = g;
      g = f;
      f = e;
      e = d + temp1;
      d = c;
      c = b;
      b = a;
      a = temp1 + temp2;
    }
    hash[0] += a;
    hash[1] += b;
    hash[2] += c;
    hash[3] += d;
    hash[4] += e;
    hash[5] += f;
    hash[6] += g;
    hash[7] += h;
  }
  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (const auto word : hash) {
    output << std::setw(8) << word;
  }
  return output.str();
}

std::string f05_optional_i64(const std::optional<std::int64_t> &value) {
  return value.has_value() ? std::to_string(*value) : "-";
}

std::string f05_double_bits(double value) {
  std::ostringstream output;
  output << std::hex << std::setfill('0') << std::setw(16)
         << std::bit_cast<std::uint64_t>(value);
  return output.str();
}

void append_f05_pair(std::ostringstream &output, std::string_view prefix,
                     const F05CooldownPairState &pair) {
  output << prefix << ".effective_sign=" << pair.effective_sign << '\n'
         << prefix << ".arrangement_start="
         << f05_optional_i64(pair.arrangement_start_ts_ns) << '\n'
         << prefix << ".last_cross=" << f05_optional_i64(pair.last_cross_ts_ns)
         << '\n'
         << prefix << ".last_cross_direction=" << pair.last_cross_direction
         << '\n';
}

void append_f05_lineage(std::ostringstream &output, std::string_view prefix,
                        const F05CooldownLineageState &lineage) {
  output << prefix << ".active=" << static_cast<int>(lineage.active) << '\n'
         << prefix << ".side=" << side_name(lineage.side) << '\n'
         << prefix << ".revision=" << lineage.revision << '\n'
         << prefix << ".campaign_id=" << lineage.campaign_id << '\n'
         << prefix << ".fill_ts_ms=" << lineage.fill_ts_ms << '\n'
         << prefix << ".deadline_ts_ms=" << lineage.deadline_ts_ms << '\n'
         << prefix << ".consecutive_units_bits="
         << f05_double_bits(lineage.consecutive_units_after) << '\n'
         << prefix << ".duration_ms=" << lineage.duration_ms << '\n'
         << prefix << ".action_id=" << lineage.action_id << '\n'
         << prefix << ".coverage_reason=" << lineage.coverage_reason_code
         << '\n';
}

std::string
f05_checkpoint_payload(const F05RepeatedBooleanCooldownCheckpoint &checkpoint) {
  std::ostringstream output;
  output << "abi=" << checkpoint.abi_version << '\n'
         << "qualification_under_test="
         << static_cast<int>(checkpoint.qualification_under_test) << '\n'
         << "qualification_sha256=" << checkpoint.parity_qualification_sha256
         << '\n'
         << "qualification_scope=" << checkpoint.qualification_scope << '\n'
         << "feature_clock_semantics=" << checkpoint.feature_clock_semantics
         << '\n'
         << "policy_sha256=" << checkpoint.policy_sha256 << '\n'
         << "predicate_bundle_sha256=" << checkpoint.predicate_bundle_sha256
         << '\n'
         << "buy_policy_sha256=" << checkpoint.buy_policy_sha256 << '\n'
         << "buy_predicate_bundle_sha256="
         << checkpoint.buy_predicate_bundle_sha256
         << '\n'
         << "warmup_s_bits=" << f05_double_bits(checkpoint.warmup_s) << '\n'
         << "max_feature_age_s_bits="
         << f05_double_bits(checkpoint.max_feature_age_s) << '\n'
         << "warmup_admitted=" << static_cast<int>(checkpoint.warmup_admitted)
         << '\n'
         << "warmup_start_right="
         << f05_optional_i64(checkpoint.warmup_start_right_ts_ns) << '\n'
         << "last_right=" << f05_optional_i64(checkpoint.last_right_ts_ns)
         << '\n'
         << "last_input_ready="
         << f05_optional_i64(checkpoint.last_input_ready_ts_ns) << '\n'
         << "last_feature_ready="
         << f05_optional_i64(checkpoint.last_feature_ready_ts_ns) << '\n'
         << "last_market_generation="
         << f05_optional_i64(checkpoint.last_market_generation) << '\n'
         << "last_depth_generation="
         << f05_optional_i64(checkpoint.last_depth_generation) << '\n'
         << "ema_initialized=" << static_cast<int>(checkpoint.ema_initialized)
         << '\n'
         << "buy_ema_initialized="
         << static_cast<int>(checkpoint.buy_ema_initialized)
         << '\n'
         << "current_window_observed="
         << static_cast<int>(checkpoint.current_window_observed) << '\n'
         << "current_channel_support_valid="
         << static_cast<int>(checkpoint.current_channel_support_valid) << '\n'
         << "last_observed=" << f05_optional_i64(checkpoint.last_observed_ts_ns)
         << '\n';
  for (std::size_t index = 0; index < checkpoint.ema.size(); ++index) {
    output << "ema" << index << "=" << f05_double_bits(checkpoint.ema[index])
           << '\n';
  }
  for (std::size_t index = 0; index < checkpoint.buy_ema.size(); ++index) {
    output << "buy_ema" << index << "="
           << f05_double_bits(checkpoint.buy_ema[index]) << '\n'
           << "buy_velocity" << index << "="
           << f05_double_bits(checkpoint.buy_velocity[index]) << '\n'
           << "buy_acceleration" << index << "="
           << f05_double_bits(checkpoint.buy_acceleration[index]) << '\n';
  }
  for (std::size_t index = 0; index < checkpoint.buy_pairs.size(); ++index) {
    append_f05_pair(output, "buy_pair" + std::to_string(index),
                    checkpoint.buy_pairs[index]);
  }
  append_f05_pair(output, "short_pair", checkpoint.short_pair);
  append_f05_pair(output, "long_pair", checkpoint.long_pair);
  append_f05_lineage(output, "buy_lineage", checkpoint.buy_lineage);
  append_f05_lineage(output, "sell_lineage", checkpoint.sell_lineage);
  const auto &audit = checkpoint.audit;
  output << "audit.window_count=" << audit.window_count << '\n'
         << "audit.gap_window_count=" << audit.gap_window_count << '\n'
         << "audit.feature_state_reset_count="
         << audit.feature_state_reset_count << '\n'
         << "audit.evaluation_count=" << audit.evaluation_count << '\n'
         << "audit.supported_count=" << audit.supported_count << '\n'
         << "audit.fallback_count=" << audit.fallback_count << '\n'
         << "audit.nonbaseline_count=" << audit.nonbaseline_count << '\n'
         << "audit.buy_control_count=" << audit.buy_control_count << '\n'
         << "audit.reducing_bypass_count=" << audit.reducing_bypass_count
         << '\n'
         << "audit.lineage_count=" << audit.lineage_count << '\n'
         << "audit.lineage_clear_count=" << audit.lineage_clear_count << '\n';
  return output.str();
}

F05TriState f05_tri_not(F05TriState value) {
  if (value == F05TriState::Unobserved) {
    return value;
  }
  return value == F05TriState::True ? F05TriState::False : F05TriState::True;
}

F05TriState f05_clause_state(const F05BooleanClause &clause,
                             const std::vector<F05TriState> &predicates) {
  bool unobserved = false;
  for (const auto &literal : clause.literals) {
    auto state = predicates.at(literal.predicate_index);
    if (literal.negated) {
      state = f05_tri_not(state);
    }
    if (state == F05TriState::False) {
      return F05TriState::False;
    }
    unobserved = unobserved || state == F05TriState::Unobserved;
  }
  return unobserved ? F05TriState::Unobserved : F05TriState::True;
}

F05TriState f05_rule_state(const F05BooleanRule &rule,
                           const std::vector<F05TriState> &predicates) {
  bool unobserved = false;
  for (const auto &clause : rule.clauses) {
    const auto state = f05_clause_state(clause, predicates);
    if (state == F05TriState::True) {
      return F05TriState::True;
    }
    unobserved = unobserved || state == F05TriState::Unobserved;
  }
  return unobserved ? F05TriState::Unobserved : F05TriState::False;
}

void update_f05_pair(F05CooldownPairState &pair, double fast, double slow,
                     std::int64_t timestamp_ns) {
  const auto distance = fast - slow;
  const int sign = distance > 0.0 ? 1 : distance < 0.0 ? -1 : 0;
  if (sign == 0) {
    return;
  }
  if (pair.effective_sign == 0) {
    pair.effective_sign = sign;
    pair.arrangement_start_ts_ns = timestamp_ns;
    return;
  }
  if (sign != pair.effective_sign) {
    pair.effective_sign = sign;
    pair.arrangement_start_ts_ns = timestamp_ns;
    pair.last_cross_ts_ns = timestamp_ns;
    pair.last_cross_direction = sign;
  }
}

std::vector<F05TriState> materialize_f05_declarative_predicates(
    const F05BooleanPolicy &policy,
    const std::vector<double> &ema,
    const std::vector<double> &velocity,
    const std::vector<double> &acceleration,
    const std::vector<F05CooldownPairState> &pairs,
    const F05CooldownFillInput &input,
    std::int64_t baseline_duration_ms) {
  std::vector<F05TriState> output(policy.predicate_columns.size(),
                                 F05TriState::Unobserved);
  const auto numeric_state = [](std::optional<double> value,
                                const F05PredicateDefinition &definition) {
    if (!value.has_value() || !std::isfinite(*value) ||
        !definition.threshold_enabled) {
      return F05TriState::Unobserved;
    }
    return *value >= definition.threshold ? F05TriState::True
                                           : F05TriState::False;
  };
  for (const auto &definition : policy.predicate_definitions) {
    if (definition.metric == F05PredicateMetric::CampaignAgeGtControl) {
      output[definition.predicate_index] =
          input.campaign_age_s * 1'000.0 >
                  static_cast<double>(baseline_duration_ms)
              ? F05TriState::True
              : F05TriState::False;
      continue;
    }
    const auto &indices = policy.predicate_pairs.at(definition.pair_index);
    const auto &pair = pairs.at(definition.pair_index);
    const auto distance = ema.at(indices.fast_ema_index) -
                          ema.at(indices.slow_ema_index);
    const auto distance_velocity = velocity.at(indices.fast_ema_index) -
                                   velocity.at(indices.slow_ema_index);
    const auto distance_acceleration =
        acceleration.at(indices.fast_ema_index) -
        acceleration.at(indices.slow_ema_index);
    switch (definition.metric) {
    case F05PredicateMetric::PositiveOrdering:
      output[definition.predicate_index] =
          pair.effective_sign == 0
              ? F05TriState::Unobserved
              : pair.effective_sign > 0 ? F05TriState::True
                                         : F05TriState::False;
      break;
    case F05PredicateMetric::LastCrossPositive:
      output[definition.predicate_index] =
          pair.last_cross_ts_ns.has_value()
              ? pair.last_cross_direction > 0 ? F05TriState::True
                                                : F05TriState::False
              : F05TriState::Unobserved;
      break;
    case F05PredicateMetric::Expanding:
      output[definition.predicate_index] =
          distance * distance_velocity > 0.0 ? F05TriState::True
                                             : F05TriState::False;
      break;
    case F05PredicateMetric::Converging:
      output[definition.predicate_index] =
          distance * distance_velocity < 0.0 ? F05TriState::True
                                             : F05TriState::False;
      break;
    case F05PredicateMetric::AbsDistance:
      output[definition.predicate_index] =
          numeric_state(std::abs(distance), definition);
      break;
    case F05PredicateMetric::CrossAgeS: {
      const auto value = pair.last_cross_ts_ns.has_value()
                             ? std::optional<double>(
                                   static_cast<double>(
                                       input.decision_ts_ns -
                                       *pair.last_cross_ts_ns) /
                                   1'000'000'000.0)
                             : std::nullopt;
      output[definition.predicate_index] = numeric_state(value, definition);
      break;
    }
    case F05PredicateMetric::ArrangementPersistenceS: {
      const auto value = pair.arrangement_start_ts_ns.has_value()
                             ? std::optional<double>(
                                   static_cast<double>(
                                       input.decision_ts_ns -
                                       *pair.arrangement_start_ts_ns) /
                                   1'000'000'000.0)
                             : std::nullopt;
      output[definition.predicate_index] = numeric_state(value, definition);
      break;
    }
    case F05PredicateMetric::SignedDistance:
      output[definition.predicate_index] = numeric_state(distance, definition);
      break;
    case F05PredicateMetric::SignedDistanceVelocity:
      output[definition.predicate_index] =
          numeric_state(distance_velocity, definition);
      break;
    case F05PredicateMetric::SignedDistanceAcceleration:
      output[definition.predicate_index] =
          numeric_state(distance_acceleration, definition);
      break;
    case F05PredicateMetric::CampaignAgeGtControl:
      break;
    }
  }
  return output;
}

std::string
validate_f05_boolean_policy(const F05BooleanPolicy &policy,
                            std::string_view error_prefix,
                            bool require_declarative_features) {
  const auto error = [error_prefix](std::string_view suffix) {
    return std::string(error_prefix) + std::string(suffix);
  };
  if (!is_lower_sha256(policy.policy_sha256)) {
    return error("policy_sha256_invalid");
  }
  if (!is_lower_sha256(policy.predicate_bundle_sha256)) {
    return error("predicate_bundle_sha256_invalid");
  }
  if (policy.default_action != kF05BooleanCooldownControlAction) {
    return error("policy_default_action_invalid");
  }
  if (policy.predicate_columns.empty() || policy.rules.empty()) {
    return error("policy_structure_empty");
  }
  std::set<std::string> names;
  for (const auto &name : policy.predicate_columns) {
    if (name.empty() || !names.insert(name).second) {
      return error("policy_predicate_columns_invalid");
    }
  }
  for (const auto &rule : policy.rules) {
    if (rule.action_id.empty() || rule.duration_ms <= 0 ||
        rule.clauses.empty()) {
      return error("policy_rule_invalid");
    }
    constexpr std::string_view prefix = "FIXED_";
    constexpr std::string_view suffix = "S";
    const std::string_view action = rule.action_id;
    if (!action.starts_with(prefix) || !action.ends_with(suffix)) {
      return error("policy_action_identity_invalid");
    }
    const auto seconds_text = action.substr(
        prefix.size(), action.size() - prefix.size() - suffix.size());
    std::int64_t seconds = 0;
    const auto [end, parse_error] = std::from_chars(
        seconds_text.data(), seconds_text.data() + seconds_text.size(), seconds);
    if (parse_error != std::errc{} ||
        end != seconds_text.data() + seconds_text.size() || seconds <= 0 ||
        seconds > std::numeric_limits<std::int64_t>::max() / 1'000 ||
        rule.duration_ms != seconds * 1'000) {
      return error("policy_action_duration_drifted");
    }
    for (const auto &clause : rule.clauses) {
      if (clause.literals.empty()) {
        return error("policy_clause_invalid");
      }
      for (const auto &literal : clause.literals) {
        if (literal.predicate_index >= policy.predicate_columns.size()) {
          return error("policy_literal_index_invalid");
        }
      }
    }
  }
  if (!require_declarative_features) {
    return {};
  }
  if (policy.ema_half_lives_s.empty() || policy.predicate_pairs.empty() ||
      policy.predicate_definitions.size() != policy.predicate_columns.size()) {
    return error("policy_feature_definition_incomplete");
  }
  if (!std::all_of(policy.ema_half_lives_s.begin(),
                   policy.ema_half_lives_s.end(), [](double value) {
                     return std::isfinite(value) && value > 0.0;
                   }) ||
      !std::is_sorted(policy.ema_half_lives_s.begin(),
                      policy.ema_half_lives_s.end()) ||
      std::adjacent_find(policy.ema_half_lives_s.begin(),
                         policy.ema_half_lives_s.end()) !=
          policy.ema_half_lives_s.end()) {
    return error("policy_ema_half_lives_invalid");
  }
  for (const auto &pair : policy.predicate_pairs) {
    if (pair.fast_ema_index >= policy.ema_half_lives_s.size() ||
        pair.slow_ema_index >= policy.ema_half_lives_s.size() ||
        pair.fast_ema_index >= pair.slow_ema_index) {
      return error("policy_ema_pair_invalid");
    }
  }
  std::vector<bool> covered(policy.predicate_columns.size(), false);
  for (const auto &definition : policy.predicate_definitions) {
    if (definition.predicate_index >= covered.size() ||
        covered[definition.predicate_index]) {
      return error("policy_predicate_definition_index_invalid");
    }
    covered[definition.predicate_index] = true;
    if (definition.metric != F05PredicateMetric::CampaignAgeGtControl &&
        definition.pair_index >= policy.predicate_pairs.size()) {
      return error("policy_predicate_definition_pair_invalid");
    }
    if (definition.threshold_enabled &&
        !std::isfinite(definition.threshold)) {
      return error("policy_predicate_threshold_invalid");
    }
  }
  if (!std::all_of(covered.begin(), covered.end(), [](bool value) {
        return value;
      })) {
    return error("policy_predicate_definition_incomplete");
  }
  return {};
}

std::string
validate_f05_policy(const F05RepeatedBooleanCooldownConfig &config) {
  if (!std::isfinite(config.warmup_s) || config.warmup_s <= 0.0 ||
      !std::isfinite(config.max_feature_age_s) ||
      config.max_feature_age_s <= 0.0) {
    return "cpp_streaming_clock_config_invalid";
  }
  if (config.qualification_scope != "synthetic_mechanics_only" &&
      config.qualification_scope != "synthetic_full_replay_smoke" &&
      config.qualification_scope != "real_day_all_arm_full_replay_v21" &&
      config.qualification_scope != "real_day_all_arm_full_replay_v22" &&
      config.qualification_scope != "real_day_all_arm_full_replay_v23" &&
      config.qualification_scope != "current_receive_time_full_replay_v1") {
    return "cpp_qualification_scope_invalid";
  }
  if (config.feature_clock_semantics != "receive_time_selected_mid_v1" &&
      config.feature_clock_semantics != "receive_time_full_mid_ema_bank_v1" &&
      config.feature_clock_semantics != "historical_exchange_m2_v1") {
    return "cpp_feature_clock_semantics_invalid";
  }
  if ((config.qualification_scope == "real_day_all_arm_full_replay_v21" ||
       config.qualification_scope == "real_day_all_arm_full_replay_v22" ||
       config.qualification_scope == "real_day_all_arm_full_replay_v23") &&
      config.feature_clock_semantics != "historical_exchange_m2_v1") {
    return "cpp_real_day_feature_clock_semantics_invalid";
  }
  if (config.qualification_under_test &&
      (config.parity_qualified ||
       !config.parity_qualification_sha256.empty())) {
    return "qualification_under_test_conflicts_with_parity";
  }
  if (config.qualification_under_test &&
      config.qualification_scope != "current_receive_time_full_replay_v1") {
    return "qualification_under_test_scope_invalid";
  }
  if (config.parity_qualified !=
      !config.parity_qualification_sha256.empty()) {
    return "parity_qualification_identity_incomplete";
  }
  if (config.parity_qualified &&
      !is_lower_sha256(config.parity_qualification_sha256)) {
    return "parity_qualification_sha256_invalid";
  }
  if (const auto error =
          validate_f05_boolean_policy(config.policy, "", false);
      !error.empty()) {
    return error;
  }
  const bool buy_enabled = !config.buy_policy.policy_sha256.empty();
  if (buy_enabled) {
    if (config.feature_clock_semantics !=
            "receive_time_full_mid_ema_bank_v1" ||
        config.qualification_scope != "current_receive_time_full_replay_v1") {
      return "buy_policy_clock_or_scope_invalid";
    }
    if (const auto error = validate_f05_boolean_policy(
            config.buy_policy, "buy_", true);
        !error.empty()) {
      return error;
    }
  }
  return {};
}

enum class BerInventoryRole : std::uint8_t {
    Opener = 0,
    Add = 1,
    Reducing = 2,
    MixedCrossZero = 3,
};

struct RoleSafeCapResult {
    double bid = 0.0;
    double ask = 0.0;
    bool hit = false;
    double excess = 0.0;
    bool feasible = true;
};

BerInventoryRole ber_inventory_role_for_target(
    Side side,
    double inventory,
    double target_quantity,
    double epsilon_btc = 1e-10
) {
    if (!std::isfinite(target_quantity) || target_quantity <= 0.0) {
        throw std::invalid_argument(
            "BER target quantity must be finite and positive"
        );
    }
    const double epsilon = std::max(epsilon_btc, 0.0);
    if (std::abs(inventory) <= epsilon) {
        return BerInventoryRole::Opener;
    }
    const double signed_quantity =
        side == Side::Buy ? target_quantity : -target_quantity;
    if (inventory * signed_quantity > 0.0) {
        return BerInventoryRole::Add;
    }
    const double inventory_after = inventory + signed_quantity;
    if (std::abs(inventory_after) <= epsilon || inventory * inventory_after > 0.0) {
        return BerInventoryRole::Reducing;
    }
    return BerInventoryRole::MixedCrossZero;
}

QuoteCoreResult compose_ber_exposure_add_only_quote(
    const QuoteCoreResult& ber_quote,
    const QuoteCoreResult& bypass_quote,
    double inventory,
    double target_buy_quantity,
    double target_sell_quantity
) {
    const auto buy_role = ber_inventory_role_for_target(
        Side::Buy, inventory, target_buy_quantity
    );
    const auto sell_role = ber_inventory_role_for_target(
        Side::Sell, inventory, target_sell_quantity
    );
    const bool buy_uses_ber =
        buy_role == BerInventoryRole::Add ||
        buy_role == BerInventoryRole::MixedCrossZero;
    const bool sell_uses_ber =
        sell_role == BerInventoryRole::Add ||
        sell_role == BerInventoryRole::MixedCrossZero;

    QuoteCoreResult out = ber_quote;
    out.bid_price = buy_uses_ber ? ber_quote.bid_price : bypass_quote.bid_price;
    out.ask_price = sell_uses_ber ? ber_quote.ask_price : bypass_quote.ask_price;
    out.buy = buy_uses_ber ? ber_quote.buy : bypass_quote.buy;
    out.sell = sell_uses_ber ? ber_quote.sell : bypass_quote.sell;
    out.spread = out.ask_price - out.bid_price;
    if (!std::isfinite(out.spread) || out.spread <= 0.0) {
        throw std::runtime_error(
            "role-safe BER composition produced a non-positive spread"
        );
    }
    out.delta_after_cap = out.spread;
    out.half_d = 0.5 * out.spread;
    out.flags.bid_adverse = out.buy.side_adverse;
    out.flags.ask_adverse = out.sell.side_adverse;
    out.flags.defense_guard = out.buy.defense_guard || out.sell.defense_guard;
    out.flags.mid_guard = out.buy.mid_guard || out.sell.mid_guard;
    out.flags.post_only = out.buy.post_only || out.sell.post_only;
    out.flags.cap_exposure_block =
        out.buy.cap_exposure_block || out.sell.cap_exposure_block;
    return out;
}

RoleSafeCapResult apply_role_safe_spread_cap(
    double mid,
    double bid,
    double ask,
    double max_spread,
    double tick_size,
    Side preserve_side
) {
    const double tick = std::max(std::abs(tick_size), 1e-12);
    const double spread = ask - bid;
    if (mid <= 0.0 || max_spread <= 0.0 || spread <= max_spread + 1e-12) {
        return RoleSafeCapResult{bid, ask, false, 0.0, true};
    }
    if (ask <= bid) {
        return RoleSafeCapResult{bid, ask, true, 0.0, false};
    }
    const double excess = spread - max_spread;
    if (preserve_side == Side::Sell) {
        const double candidate_bid = ceil_tick(ask - max_spread, tick);
        if (candidate_bid >= mid || candidate_bid >= ask) {
            return RoleSafeCapResult{bid, ask, true, excess, false};
        }
        return RoleSafeCapResult{candidate_bid, ask, true, excess, true};
    }
    const double candidate_ask = floor_tick(bid + max_spread, tick);
    if (candidate_ask <= mid || candidate_ask <= bid) {
        return RoleSafeCapResult{bid, ask, true, excess, false};
    }
    return RoleSafeCapResult{bid, candidate_ask, true, excess, true};
}

struct P3ReachBudgetEpisode {
    std::int64_t last_evaluated_bucket_ms = -1;
    std::int64_t active_bucket_ms = -1;
    std::uint8_t selected_k = 0;
    bool active = false;
    bool saw_nonflat_inventory = false;

    void deactivate() noexcept {
        active_bucket_ms = -1;
        selected_k = 0;
        active = false;
        saw_nonflat_inventory = false;
    }
};

void require_same_size(std::size_t expected, std::size_t actual, const char* name) {
    if (actual != expected) [[unlikely]] {
        throw std::invalid_argument(std::string(name) + " length mismatch");
    }
}

std::size_t advance_index(ArrayView<std::int64_t> ts, std::size_t idx, std::int64_t target_ts) {
    // 主 trade loop 的时间戳单调递增，BBO/L2/ML/quote-EV 用游标推进即可。
    // 只有回看任意 target_ts 的诊断路径才使用 index_at_or_before() 二分。
    while (idx + 1 < ts.size() && ts.data()[idx + 1] <= target_ts) {
        ++idx;
    }
    return idx;
}

std::int64_t round_ties_to_even(double value) {
    if (!std::isfinite(value)) {
        throw std::invalid_argument("cannot round a non-finite fair-center shift");
    }
    const double lower = std::floor(value);
    const double fraction = value - lower;
    if (fraction < 0.5) {
        return static_cast<std::int64_t>(lower);
    }
    if (fraction > 0.5) {
        return static_cast<std::int64_t>(lower + 1.0);
    }
    const auto lower_tick = static_cast<std::int64_t>(lower);
    return lower_tick % 2 == 0 ? lower_tick : lower_tick + 1;
}

std::ptrdiff_t index_at_or_before(ArrayView<std::int64_t> ts, std::int64_t target_ts) {
    if (ts.size() == 0) {
        return -1;
    }
    std::size_t lo = 0;
    std::size_t hi = ts.size();
    while (lo < hi) {
        const std::size_t mid = lo + (hi - lo) / 2;
        if (ts.data()[mid] <= target_ts) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo == 0) {
        return -1;
    }
    return static_cast<std::ptrdiff_t>(lo - 1);
}

double queue_ahead_estimate(double queue_base, double queue_decay, double dist_from_mid) {
    return std::max(0.0, queue_base * std::exp(-queue_decay * std::max(0.0, dist_from_mid)));
}

double value_at_or_default(ArrayView<double> values, std::size_t idx, double fallback) {
    if (values.size() == 0) {
        return fallback;
    }
    if (idx >= values.size()) {
        throw std::out_of_range("per-trade parameter index out of range");
    }
    return values.data()[idx];
}

bool fill_gate(double probability, std::mt19937_64& rng) {
    probability = clamp(probability, 0.0, 1.0);
    if (probability >= 1.0) {
        return true;
    }
    if (probability <= 0.0) {
        return false;
    }
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) <= probability;
}

enum class LossCooldownTransition : std::uint8_t {
    Inactive = 0,
    Active = 1,
    Triggered = 2,
    Expired = 3,
};

struct ConsecutiveLossCooldownState {
    int max_consecutive_losses = 0;
    std::int64_t cooldown_ms = 0;
    double inventory = 0.0;
    double avg_entry = 0.0;
    double open_commission = 0.0;
    double round_trip_pnl = 0.0;
    int consecutive_losses = 0;
    std::int64_t cooldown_until_ms = 0;
    std::int64_t last_cancel_ts_ms = -1;
    bool threshold_pending = false;
    std::int64_t trigger_count = 0;
    std::int64_t expiry_count = 0;
    std::int64_t losing_round_trips = 0;
    std::int64_t winning_or_flat_round_trips = 0;
    int max_observed_consecutive_losses = 0;

    [[nodiscard]] bool enabled() const noexcept {
        return max_consecutive_losses > 0 && cooldown_ms > 0;
    }

    [[nodiscard]] bool active(std::int64_t now_ms) const noexcept {
        return enabled() && now_ms < cooldown_until_ms;
    }

    LossCooldownTransition on_policy_clock(std::int64_t now_ms) {
        if (cooldown_until_ms > 0) {
            if (now_ms < cooldown_until_ms) {
                return LossCooldownTransition::Active;
            }
            cooldown_until_ms = 0;
            consecutive_losses = 0;
            threshold_pending = false;
            ++expiry_count;
            return LossCooldownTransition::Expired;
        }
        if (enabled() && threshold_pending) {
            threshold_pending = false;
            cooldown_until_ms = now_ms + cooldown_ms;
            ++trigger_count;
            return LossCooldownTransition::Triggered;
        }
        return LossCooldownTransition::Inactive;
    }

    template <Side S>
    void on_fill(double quantity, double price, double commission) {
        if (!std::isfinite(quantity) || !std::isfinite(price) ||
            !std::isfinite(commission) || quantity <= 0.0 ||
            price <= 0.0) {
            throw std::invalid_argument("invalid consecutive-loss fill");
        }
        const double signed_qty = is_buy_v<S> ? quantity : -quantity;
        if (std::abs(inventory) <= 1e-10) {
            inventory = signed_qty;
            avg_entry = price;
            open_commission = commission;
            round_trip_pnl = 0.0;
            return;
        }
        if ((inventory > 0.0 && signed_qty > 0.0) ||
            (inventory < 0.0 && signed_qty < 0.0)) {
            const double old_abs = std::abs(inventory);
            const double new_abs = old_abs + quantity;
            avg_entry = (avg_entry * old_abs + price * quantity) / new_abs;
            inventory += signed_qty;
            open_commission += commission;
            return;
        }

        const double old_inventory = inventory;
        const double old_abs = std::abs(old_inventory);
        const double close_qty = std::min(quantity, old_abs);
        const double closing_fee = commission * close_qty / quantity;
        const double opening_fee = commission - closing_fee;
        const double open_fee_share = old_abs > 1e-10
            ? open_commission * close_qty / old_abs
            : open_commission;
        const double realized = old_inventory > 0.0
            ? (price - avg_entry) * close_qty - closing_fee - open_fee_share
            : (avg_entry - price) * close_qty - closing_fee - open_fee_share;
        open_commission -= open_fee_share;
        round_trip_pnl += realized;
        const double remaining = old_abs - close_qty;
        if (remaining < 1e-10) {
            if (round_trip_pnl < 0.0) {
                ++losing_round_trips;
                if (enabled()) {
                    ++consecutive_losses;
                } else {
                    consecutive_losses = 0;
                }
            } else {
                consecutive_losses = 0;
                ++winning_or_flat_round_trips;
            }
            max_observed_consecutive_losses = std::max(
                max_observed_consecutive_losses,
                consecutive_losses
            );
            threshold_pending = enabled() &&
                consecutive_losses >= max_consecutive_losses;

            const double flip_qty = quantity - close_qty;
            if (flip_qty > 1e-10) {
                inventory = is_buy_v<S> ? flip_qty : -flip_qty;
                avg_entry = price;
                open_commission = opening_fee;
            } else {
                inventory = 0.0;
                avg_entry = 0.0;
                open_commission = 0.0;
            }
            round_trip_pnl = 0.0;
        } else {
            inventory += signed_qty;
        }
    }
};

std::uint64_t splitmix64(std::uint64_t value) {
    std::uint64_t z = value + UINT64_C(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30U)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27U)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31U);
}

enum class LatencyOperation : std::uint64_t {
    NewOrder = 1,
    Cancel = 2,
    FragileTtlCancel = 3,
    QueueValueCancel = 4,
    DecisionToGateway = 6,
};

enum class RandomPassiveOperation : std::uint64_t {
    TimingJitter = 1,
    SideMirror = 2,
};

double keyed_random_passive_unit(
    std::int64_t seed,
    std::int64_t event_ts_ms,
    std::int64_t action_identity,
    RandomPassiveOperation operation
) {
    std::uint64_t mixed = static_cast<std::uint64_t>(seed);
    const std::array<std::uint64_t, 3> components{
        static_cast<std::uint64_t>(event_ts_ms),
        static_cast<std::uint64_t>(action_identity),
        static_cast<std::uint64_t>(operation),
    };
    for (const auto component : components) {
        mixed = splitmix64(mixed ^ component);
    }
    constexpr double inv_two_to_53 = 1.0 / 9007199254740992.0;
    const std::uint64_t draw = splitmix64(
        mixed ^ UINT64_C(0xA0761D6478BD642F)
    );
    return static_cast<double>(draw >> 11U) * inv_two_to_53;
}

std::int64_t sample_latency_ms(
    std::int64_t base_ms,
    std::int64_t jitter_ms,
    const std::vector<double>* samples,
    const TickReplayParams& params,
    std::int64_t event_ts_ms,
    Side side,
    LatencyOperation operation,
    std::int64_t order_ts_ms
) {
    const std::int64_t latency_seed = params.latency_seed >= 0
        ? params.latency_seed
        : params.rng_seed + 17;
    std::uint64_t mixed = static_cast<std::uint64_t>(latency_seed);
    const std::array<std::uint64_t, 4> components{
        static_cast<std::uint64_t>(event_ts_ms),
        static_cast<std::uint64_t>(order_ts_ms),
        side == Side::Buy ? UINT64_C(1) : UINT64_C(2),
        static_cast<std::uint64_t>(operation),
    };
    for (const auto component : components) {
        mixed = splitmix64(mixed ^ component);
    }

    double latency = static_cast<double>(std::max<std::int64_t>(0, base_ms));
    if (samples != nullptr && !samples->empty()) {
        latency = std::max(
            0.0,
            (*samples)[static_cast<std::size_t>(mixed % samples->size())]
        );
    }
    constexpr double inv_two_to_53 = 1.0 / 9007199254740992.0;
    const std::uint64_t jitter_draw = splitmix64(
        mixed ^ UINT64_C(0xD1B54A32D192ED03)
    );
    const double jitter_unit = static_cast<double>(jitter_draw >> 11U) * inv_two_to_53;
    if (jitter_ms > 0) {
        latency += (2.0 * jitter_unit - 1.0) * static_cast<double>(jitter_ms);
    }
    if (params.latency_stress_enabled &&
        params.latency_stress_spike_probability > 0.0) {
        const std::uint64_t spike_draw = splitmix64(
            mixed ^ UINT64_C(0x94D049BB133111EB)
        );
        const double spike_unit = static_cast<double>(spike_draw >> 11U) * inv_two_to_53;
        if (spike_unit < params.latency_stress_spike_probability) {
            latency *= std::max(1.0, params.latency_stress_spike_multiplier);
        }
    }
    return std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(std::max(0.0, latency)))
    );
}

bool decision_to_gateway_latency_enabled(const TickReplayParams& params) {
    return std::any_of(
        params.decision_to_gateway_latency_samples_ms.begin(),
        params.decision_to_gateway_latency_samples_ms.end(),
        [](double sample) { return sample > 0.0; }
    );
}

std::int64_t decision_gateway_request_ts(
    const TickReplayParams& params,
    std::int64_t decision_ts_ms
) {
    if (!decision_to_gateway_latency_enabled(params)) {
        return decision_ts_ms;
    }
    const std::int64_t latency_seed =
        params.decision_to_gateway_latency_seed >= 0
        ? params.decision_to_gateway_latency_seed
        : (params.latency_seed >= 0 ? params.latency_seed : params.rng_seed + 17);
    std::uint64_t mixed = static_cast<std::uint64_t>(latency_seed);
    const std::array<std::uint64_t, 2> components{
        static_cast<std::uint64_t>(decision_ts_ms),
        static_cast<std::uint64_t>(LatencyOperation::DecisionToGateway),
    };
    for (const auto component : components) {
        mixed = splitmix64(mixed ^ component);
    }
    const auto sample = std::max(
        0.0,
        params.decision_to_gateway_latency_samples_ms[
            static_cast<std::size_t>(
                mixed % params.decision_to_gateway_latency_samples_ms.size()
            )
        ]
    );
    return decision_ts_ms + std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(sample))
    );
}

struct CancelDeadlines {
    std::int64_t effective_ts = 0;
    std::int64_t ack_ts = 0;
};

struct NewOrderDeadlines {
    std::int64_t effective_ts = 0;
    std::int64_t ack_ts = 0;
};

NewOrderDeadlines sample_new_order_deadlines(
    std::int64_t request_ts,
    std::int64_t legacy_base_ms,
    std::int64_t legacy_jitter_ms,
    const std::vector<double>* legacy_samples,
    const TickReplayParams& params,
    Side side,
    LatencyOperation operation,
    std::int64_t order_ts,
    bool apply_decision_compute = true
) {
    const auto gateway_request_ts = apply_decision_compute
        ? decision_gateway_request_ts(params, request_ts)
        : request_ts;
    if (params.new_order_exchange_effective_latency_samples_ms.empty()) {
        const auto latency = sample_latency_ms(
            legacy_base_ms,
            legacy_jitter_ms,
            legacy_samples,
            params,
            request_ts,
            side,
            operation,
            order_ts
        );
        const auto deadline = gateway_request_ts + latency;
        return NewOrderDeadlines{deadline, deadline};
    }
    if (legacy_samples != nullptr && !legacy_samples->empty() &&
        legacy_samples->size() !=
            params.new_order_exchange_effective_latency_samples_ms.size()) {
        throw std::invalid_argument(
            "paired new-order effective/ACK latency samples must have equal length"
        );
    }
    const auto effective_latency = sample_latency_ms(
        legacy_base_ms,
        legacy_jitter_ms,
        &params.new_order_exchange_effective_latency_samples_ms,
        params,
        request_ts,
        side,
        operation,
        order_ts
    );
    const auto effective_ts = gateway_request_ts + effective_latency;
    if (legacy_samples == nullptr || legacy_samples->empty()) {
        return NewOrderDeadlines{effective_ts, effective_ts};
    }
    // Both arrays are row-aligned request lifecycles. Reuse the same keyed
    // index; the legacy sample is request-to-local ACK visibility, not a
    // delay added after exchange effectiveness.
    const auto ack_latency = sample_latency_ms(
        legacy_base_ms,
        legacy_jitter_ms,
        legacy_samples,
        params,
        request_ts,
        side,
        operation,
        order_ts
    );
    return NewOrderDeadlines{
        effective_ts,
        std::max(effective_ts, gateway_request_ts + ack_latency),
    };
}

CancelDeadlines sample_cancel_deadlines(
    std::int64_t request_ts,
    std::int64_t legacy_base_ms,
    std::int64_t legacy_jitter_ms,
    const std::vector<double>* legacy_samples,
    const TickReplayParams& params,
    Side side,
    LatencyOperation operation,
    std::int64_t order_ts,
    bool apply_decision_compute = false
) {
    const auto gateway_request_ts = apply_decision_compute
        ? decision_gateway_request_ts(params, request_ts)
        : request_ts;
    const bool split =
        !params.cancel_exchange_effective_latency_samples_ms.empty() ||
        !params.cancel_ack_visibility_latency_samples_ms.empty();
    if (!split) {
        const auto latency = sample_latency_ms(
            legacy_base_ms,
            legacy_jitter_ms,
            legacy_samples,
            params,
            request_ts,
            side,
            operation,
            order_ts
        );
        const auto deadline = gateway_request_ts + latency;
        return CancelDeadlines{deadline, deadline};
    }

    const std::vector<double>* effective_samples =
        !params.cancel_exchange_effective_latency_samples_ms.empty()
        ? &params.cancel_exchange_effective_latency_samples_ms
        : legacy_samples;
    const auto effective_latency = sample_latency_ms(
        legacy_base_ms,
        legacy_jitter_ms,
        effective_samples,
        params,
        request_ts,
        side,
        operation,
        order_ts
    );
    const auto effective_ts = gateway_request_ts + effective_latency;
    if (params.cancel_ack_visibility_latency_samples_ms.empty()) {
        return CancelDeadlines{effective_ts, effective_ts};
    }
    if (!params.cancel_exchange_effective_latency_samples_ms.empty() &&
        params.cancel_exchange_effective_latency_samples_ms.size() !=
            params.cancel_ack_visibility_latency_samples_ms.size()) {
        throw std::invalid_argument(
            "paired cancel effective/ACK latency samples must have equal length"
        );
    }
    // Both arrays are row-aligned request lifecycles. Reuse the same keyed
    // index; ACK samples are request-to-local visibility rather than a delay
    // added after the exchange-effective boundary.
    const auto ack_latency = sample_latency_ms(
        0,
        0,
        &params.cancel_ack_visibility_latency_samples_ms,
        params,
        request_ts,
        side,
        operation,
        order_ts
    );
    return CancelDeadlines{
        effective_ts,
        std::max(effective_ts, gateway_request_ts + ack_latency),
    };
}

std::int64_t sample_exec_book_visibility_delay_ms(
    std::int64_t decision_ts_ms,
    const TickReplayParams& params
) {
    const std::uint64_t mixed = splitmix64(
        static_cast<std::uint64_t>(decision_ts_ms) ^
        static_cast<std::uint64_t>(params.exec_book_visibility_delay_seed)
    );
    double delay_ms = 0.0;
    if (!params.exec_book_visibility_delay_samples_ms.empty()) {
        const std::size_t idx = static_cast<std::size_t>(
            mixed % params.exec_book_visibility_delay_samples_ms.size()
        );
        delay_ms = params.exec_book_visibility_delay_samples_ms[idx];
    } else {
        constexpr double inv_two_to_53 = 1.0 / 9007199254740992.0;
        const double unit = static_cast<double>(mixed >> 11U) * inv_two_to_53;
        delay_ms =
            params.exec_book_visibility_delay_mean_ms +
            (2.0 * unit - 1.0) *
                params.exec_book_visibility_delay_jitter_ms;
    }
    return std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(delay_ms))
    );
}

template <Side S>
bool trade_crosses_order(
    std::int64_t trade_price_tick,
    std::int64_t order_price_tick
) {
    if constexpr (is_buy_v<S>) {
        return trade_price_tick <= order_price_tick;
    } else {
        return trade_price_tick >= order_price_tick;
    }
}

struct BookSnapshot {
    double best_bid = 0.0;
    double best_ask = 0.0;
    double bid_qty = 0.0;
    double ask_qty = 0.0;
    double mid = 0.0;
    std::int64_t age_ms = 0;
    bool fresh = true;
    bool has_book = false;
};

struct PendingMarkout {
    std::int64_t fill_ts = 0;
    double fill_price = 0.0;
    bool is_bid = false;
    bool final_compressed = false;
    double fill_qty_btc = 0.0;
};

using ReplayOrders = std::pmr::vector<ReplayOrder>;
using PendingMarkouts = std::pmr::vector<PendingMarkout>;

double top_depth_total(const DepthView& depth, int levels) {
    const int n_levels = std::max(1, levels);
    double total = 0.0;
    int bid_levels = 0;
    for (std::size_t i = 0; i < depth.bids.size() && bid_levels < n_levels; ++i) {
        if (!depth.bids.valid(i)) {
            continue;
        }
        bool duplicate = false;
        const double px = depth.bids.price(i);
        for (std::size_t j = 0; j < i; ++j) {
            if (depth.bids.valid(j) && depth.bids.price(j) == px) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) {
            continue;
        }
        total += depth.bids.quantity(i);
        ++bid_levels;
    }
    int ask_levels = 0;
    for (std::size_t i = 0; i < depth.asks.size() && ask_levels < n_levels; ++i) {
        if (!depth.asks.valid(i)) {
            continue;
        }
        bool duplicate = false;
        const double px = depth.asks.price(i);
        for (std::size_t j = 0; j < i; ++j) {
            if (depth.asks.valid(j) && depth.asks.price(j) == px) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) {
            continue;
        }
        total += depth.asks.quantity(i);
        ++ask_levels;
    }
    return total;
}

bool duplicate_l2_price_before(
    const MatrixView<double>& prices,
    std::size_t row,
    std::size_t col,
    double tick_size
) {
    const double px = prices(row, col);
    if (!std::isfinite(px) || px <= 0.0) {
        return false;
    }
    const auto price_tick = price_to_tick_unchecked(px, tick_size);
    for (std::size_t j = 0; j < col; ++j) {
        const double prev = prices(row, j);
        if (std::isfinite(prev) && prev > 0.0 &&
            price_to_tick_unchecked(prev, tick_size) == price_tick) {
            return true;
        }
    }
    return false;
}

double top_qty_depth_total(const BookSnapshot& book) {
    return std::max(0.0, book.bid_qty) + std::max(0.0, book.ask_qty);
}

template <Side S>
bool side_matches_gate(const std::string& gate_side) {
    std::string value;
    value.reserve(gate_side.size());
    for (char ch : gate_side) {
        value.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
    }
    return value.empty() || value == "BOTH" ||
        (is_buy_v<S> && value == "BUY") ||
        (!is_buy_v<S> && (value == "SELL" || value == "ASK"));
}

template <Side S>
bool exposure_increasing_for_side(double inventory) {
    return is_buy_v<S> ? inventory >= 0.0 : inventory <= 0.0;
}

double campaign_age_s(std::int64_t ts, std::int64_t pos_open_ts, double inventory, double lot_size) {
    if (std::abs(inventory) < lot_size || pos_open_ts <= 0) {
        return 0.0;
    }
    return std::max(0.0, static_cast<double>(ts - pos_open_ts) / 1000.0);
}

template <Side S>
bool campaign_exposure_risk_active(const TickReplayParams& params,
                                   double inventory,
                                   double lot_size,
                                   std::int64_t ts,
                                   std::int64_t pos_open_ts,
                                   double inv_threshold,
                                   double age_s_threshold) {
    if (!exposure_increasing_for_side<S>(inventory)) {
        return false;
    }
    const bool inv_hit = inv_threshold > 0.0 && std::abs(inventory) >= inv_threshold;
    const bool age_hit = age_s_threshold > 0.0 &&
        campaign_age_s(ts, pos_open_ts, inventory, lot_size) >= age_s_threshold;
    return inv_hit || age_hit;
}

template <Side S>
double side_adverse_ret_from_pred(double pred_ret) {
    return is_buy_v<S> ? std::max(0.0, -pred_ret) : std::max(0.0, pred_ret);
}

template <Side S>
bool campaign_soft_gate_active(const TickReplayParams& params,
                               double inventory,
                               double lot_size,
                               std::int64_t ts,
                               std::int64_t pos_open_ts,
                               double pred_ret,
                               double refill_edge,
                               double micro_reversion_score) {
    if (!params.campaign_soft_gate_enabled) {
        return true;
    }
    if (!side_matches_gate<S>(params.campaign_soft_gate_side)) {
        return false;
    }
    const double inv_score = params.campaign_soft_gate_campaign_inv_ref > 0.0
        ? std::abs(inventory) / std::max(params.campaign_soft_gate_campaign_inv_ref, 1e-12)
        : 0.0;
    const double age_score = params.campaign_soft_gate_campaign_age_ref_s > 0.0
        ? campaign_age_s(ts, pos_open_ts, inventory, lot_size) /
            std::max(params.campaign_soft_gate_campaign_age_ref_s, 1e-12)
        : 0.0;
    const double campaign_score = std::max(inv_score, age_score);
    const double trend_score = params.campaign_soft_gate_trend_ret_ref > 0.0
        ? side_adverse_ret_from_pred<S>(pred_ret) /
            std::max(params.campaign_soft_gate_trend_ret_ref, 1e-12)
        : 0.0;
    const bool campaign_hit = campaign_score >= params.campaign_soft_gate_campaign_score;
    const bool trend_hit = trend_score >= params.campaign_soft_gate_trend_score;
    const bool refill_weak = refill_edge <= params.campaign_soft_gate_refill_edge_max;
    const bool reversion_weak = micro_reversion_score <= params.campaign_soft_gate_reversion_max;
    return campaign_hit && trend_hit && refill_weak && reversion_weak;
}

template <Side S>
double adaptive_add_cooldown_multiplier_cpp(const TickReplayParams& params,
                                            double side_markout_ema,
                                            double consecutive_units,
                                            double prev_inventory,
                                            double max_inventory,
                                            std::int64_t ts,
                                            std::int64_t pos_open_ts,
                                            double pred_ret,
                                            double refill_edge,
                                            double micro_reversion_score) {
    if (!params.adaptive_add_cooldown_enabled) {
        return 1.0;
    }
    if (!side_matches_gate<S>(params.adaptive_add_cooldown_gate_side)) {
        return 1.0;
    }
    const double inv_score = params.adaptive_add_cooldown_campaign_inv_ref > 0.0
        ? std::abs(prev_inventory) / std::max(params.adaptive_add_cooldown_campaign_inv_ref, 1e-12)
        : 0.0;
    const double age_score = params.adaptive_add_cooldown_campaign_age_ref_s > 0.0
        ? campaign_age_s(ts, pos_open_ts, prev_inventory, 0.0) /
            std::max(params.adaptive_add_cooldown_campaign_age_ref_s, 1e-12)
        : 0.0;
    const double campaign_score = clamp(std::max(inv_score, age_score), 0.0, 1.0);
    const double markout_score = clamp(
        std::max(0.0, -side_markout_ema) / std::max(params.adaptive_add_cooldown_mo_ref, 1e-12),
        0.0,
        1.0
    );
    const double flow_score = clamp(
        std::max(0.0, consecutive_units - 1.0) / std::max(params.adaptive_add_cooldown_flow_ref, 1e-12),
        0.0,
        1.0
    );
    const double trend_score = params.adaptive_add_cooldown_trend_ret_ref > 0.0
        ? clamp(
            side_adverse_ret_from_pred<S>(pred_ret) /
                std::max(params.adaptive_add_cooldown_trend_ret_ref, 1e-12),
            0.0,
            1.0
        )
        : 0.0;
    const double weak_refill = params.adaptive_add_cooldown_refill_ref > 0.0
        ? clamp(1.0 - refill_edge / std::max(params.adaptive_add_cooldown_refill_ref, 1e-12), 0.0, 1.0)
        : 0.0;
    const double good_refill = params.adaptive_add_cooldown_refill_ref > 0.0
        ? clamp(refill_edge / std::max(params.adaptive_add_cooldown_refill_ref, 1e-12), 0.0, 1.0)
        : 0.0;
    const double reversion = params.adaptive_add_cooldown_reversion_ref > 0.0
        ? clamp(micro_reversion_score / std::max(params.adaptive_add_cooldown_reversion_ref, 1e-12), 0.0, 1.0)
        : 0.0;
    if (params.adaptive_add_cooldown_gate_enabled) {
        const bool gate =
            campaign_score >= params.adaptive_add_cooldown_gate_campaign_score &&
            trend_score >= params.adaptive_add_cooldown_gate_trend_score &&
            refill_edge <= params.adaptive_add_cooldown_gate_refill_edge_max &&
            micro_reversion_score <= params.adaptive_add_cooldown_gate_reversion_max;
        return gate
            ? clamp(params.adaptive_add_cooldown_gate_mult,
                    params.adaptive_add_cooldown_min_mult,
                    params.adaptive_add_cooldown_max_mult)
            : 1.0;
    }
    const double add =
        params.adaptive_add_cooldown_w_markout * markout_score +
        params.adaptive_add_cooldown_w_flow * flow_score +
        params.adaptive_add_cooldown_w_campaign * campaign_score +
        params.adaptive_add_cooldown_w_trend * trend_score +
        params.adaptive_add_cooldown_w_refill_weak * weak_refill;
    const double sub =
        params.adaptive_add_cooldown_w_refill_good * good_refill +
        params.adaptive_add_cooldown_w_reversion * reversion;
    return clamp(1.0 + add - sub,
                 params.adaptive_add_cooldown_min_mult,
                 params.adaptive_add_cooldown_max_mult);
}

double local_extreme_rank_cpp(
    const TickReplayInput& input,
    std::size_t trade_idx,
    std::int64_t ts,
    double mid,
    const TickReplayParams& params,
    double& local_low,
    double& local_high
) {
    local_low = mid;
    local_high = mid;
    if ((!params.local_extreme_guard_enabled && params.fragile_order_ttl_s <= 0.0) || mid <= 0.0) {
        return 0.5;
    }
    const std::int64_t window_ms = std::max<std::int64_t>(
        5'000,
        static_cast<std::int64_t>(std::llround(params.local_extreme_window_s * 1000.0))
    );
    const std::int64_t start_ts = ts - window_ms;
    const auto right_idx = index_at_or_before(input.trade_ts_ms, ts);
    if (right_idx < 0) {
        return 0.5;
    }
    // Match Python's np.searchsorted(..., side="right") local-rank window:
    // all trades with the same millisecond timestamp are included, even when
    // the replay loop is currently on an earlier row with that timestamp.
    std::size_t right = std::max(trade_idx, static_cast<std::size_t>(right_idx));
    bool seen = false;
    for (std::size_t j = right; j < input.trade_ts_ms.size(); --j) {
        if (input.trade_ts_ms.data()[j] < start_ts) {
            break;
        }
        const double px = input.trade_price.data()[j];
        if (px > 0.0) {
            if (!seen) {
                local_low = px;
                local_high = px;
                seen = true;
            } else {
                local_low = std::min(local_low, px);
                local_high = std::max(local_high, px);
            }
        }
        if (j == 0) {
            break;
        }
    }
    if (!seen || local_high - local_low <= std::max(params.quote.tick_size, 0.0)) {
        return 0.5;
    }
    return clamp((mid - local_low) / (local_high - local_low), 0.0, 1.0);
}

double causal_local_rank_cpp(
    const TickReplayInput& input,
    std::size_t trade_idx,
    std::int64_t ts,
    double fallback_price,
    std::int64_t window_ms = 120'000,
    double flat_threshold = 0.0
) {
    if (input.trade_ts_ms.size() == 0 || input.trade_price.size() == 0) {
        return 0.5;
    }
    const auto right_idx = index_at_or_before(input.trade_ts_ms, ts);
    if (right_idx < 0) {
        return 0.5;
    }
    // Match Python _local_rank_at_ts(): np.searchsorted(..., side="right")
    // includes all trades with the same millisecond timestamp.  The current
    // replay row can be earlier than the last row at that timestamp.
    const std::size_t right = std::max(trade_idx, static_cast<std::size_t>(right_idx));
    if (right >= input.trade_price.size()) {
        return 0.5;
    }
    const double cur = std::isfinite(input.trade_price.data()[right])
        ? input.trade_price.data()[right]
        : fallback_price;
    if (!std::isfinite(cur)) {
        return 0.5;
    }
    const std::int64_t start_ts = ts - std::max<std::int64_t>(1'000, window_ms);
    std::size_t start = right;
    while (start > 0 && input.trade_ts_ms.data()[start - 1] >= start_ts) {
        --start;
    }
    double lo = cur;
    double hi = cur;
    for (std::size_t j = start; j <= right && j < input.trade_price.size(); ++j) {
        const double px = input.trade_price.data()[j];
        if (!std::isfinite(px)) {
            continue;
        }
        lo = std::min(lo, px);
        hi = std::max(hi, px);
    }
    if (!std::isfinite(lo) || !std::isfinite(hi) || hi - lo <= std::max(flat_threshold, 1e-12)) {
        return 0.5;
    }
    return clamp((cur - lo) / (hi - lo), 0.0, 1.0);
}

void apply_local_extreme_cpp(
    SideQuoteContext& bid_ctx,
    SideQuoteContext& ask_ctx,
    const TickReplayParams& params,
    double mid,
    double rank,
    double local_low,
    double local_high,
    double near_depth,
    TickReplaySummary& summary
) {
    if (!params.local_extreme_guard_enabled && params.fragile_order_ttl_s <= 0.0) {
        return;
    }
    double thin_threshold = params.local_extreme_thin_depth_threshold;
    if (thin_threshold <= 0.0) {
        thin_threshold = std::max(1.0, params.quote.kappa_depth_baseline * 0.5);
    }
    const bool thin_active = near_depth > 0.0 && near_depth < thin_threshold;
    const bool fragile_ttl_active = params.fragile_order_ttl_s > 0.0 && thin_active;
    const double ttl_ms = std::max(0.0, params.fragile_order_ttl_s) * 1000.0;
    if (fragile_ttl_active && ttl_ms > 0.0) {
        bid_ctx.order_ttl_ms = ttl_ms;
        ask_ctx.order_ttl_ms = ttl_ms;
    }
    bid_ctx.near_depth_total = near_depth;
    ask_ctx.near_depth_total = near_depth;
    if (!params.local_extreme_guard_enabled) {
        return;
    }
    if (params.local_extreme_require_thin_depth && !thin_active) {
        bid_ctx.local_extreme_rank = 0.5;
        ask_ctx.local_extreme_rank = 0.5;
        bid_ctx.local_extreme_low = mid;
        ask_ctx.local_extreme_low = mid;
        bid_ctx.local_extreme_high = mid;
        ask_ctx.local_extreme_high = mid;
        bid_ctx.local_extreme_window_s = params.local_extreme_window_s;
        ask_ctx.local_extreme_window_s = params.local_extreme_window_s;
        return;
    }

    const bool allowed_by_depth = thin_active || !params.local_extreme_require_thin_depth;
    const bool bid_active = allowed_by_depth && rank >= params.local_extreme_rank_threshold;
    const bool ask_active = allowed_by_depth && rank <= (1.0 - params.local_extreme_rank_threshold);
    auto update_ctx = [&](SideQuoteContext& ctx, bool active) {
        ctx.local_extreme_rank = rank;
        ctx.local_extreme_low = local_low;
        ctx.local_extreme_high = local_high;
        ctx.local_extreme_window_s = params.local_extreme_window_s;
        ctx.local_extreme_thin_depth = thin_active;
        ctx.local_extreme_guard = active;
        ctx.local_extreme_pause = active && params.local_extreme_pause;
        ctx.local_extreme_spread_mult = active ? std::max(1.0, params.local_extreme_spread_mult) : 1.0;
        if (active && ttl_ms > 0.0) {
            ctx.order_ttl_ms = ttl_ms;
        }
        if (active) {
            ctx.any_constraint_changed = true;
        }
    };
    update_ctx(bid_ctx, bid_active);
    update_ctx(ask_ctx, ask_active);
    if (bid_active || ask_active) {
        summary.local_extreme_guard_count += static_cast<std::int64_t>(bid_active) +
            static_cast<std::int64_t>(ask_active);
        summary.bid_local_extreme_guard_count += static_cast<std::int64_t>(bid_active);
        summary.ask_local_extreme_guard_count += static_cast<std::int64_t>(ask_active);
        if (params.local_extreme_pause) {
            summary.local_extreme_pause_count += static_cast<std::int64_t>(bid_active) +
                static_cast<std::int64_t>(ask_active);
        }
    }
}

TraceOrderRow make_trace_order_row(
    const SideQuoteContext& ctx,
    const QuoteCoreResult& quote,
    const QuotePrediction& pred,
    Side side,
    std::int64_t order_id,
    std::int64_t ts,
    std::int64_t activate_ts,
    double price,
    double quantity,
    double inventory,
    double mid,
    double best_bid,
    double best_ask,
    double mo_ema_bid,
    double mo_ema_ask,
    bool post_policy_cap_hit,
    bool random_passive_mirrored
) {
    TraceOrderRow row;
    row.order_id = order_id;
    row.side = side;
    row.submit_ts = ts;
    row.activate_ts = activate_ts;
    row.quote_ts = ts;
    row.price = price;
    row.quantity = quantity;
    row.raw_half_spread = quote.raw_half_spread;
    row.capped_half_spread = quote.capped_half_spread;
    row.raw_mid_shift = quote.raw_mid_shift;
    row.raw_reservation_shift = quote.raw_reservation_shift;
    row.raw_asym_shift = quote.raw_asym_shift;
    row.asym = quote.asym;
    row.inventory = inventory;
    row.dir_signal = pred.dir_10s - 0.5;
    row.pred_dir = pred.dir_10s;
    row.pred_ret = pred.ret_10s;
    row.tox_bid = pred.tox_bid;
    row.tox_ask = pred.tox_ask;
    row.book_imb = quote.book_imb;
    row.microprice_shift_bps = quote.microprice_shift_bps;
    row.near_depth_total = ctx.near_depth_total;
    row.l2_near_depth_total = ctx.l2_near_depth_total;
    row.l2_quote_flip_rate = ctx.l2_quote_flip_rate;
    row.l2_book_refresh_ratio = ctx.l2_book_refresh_ratio;
    row.l2_book_cancel_ratio = ctx.l2_book_cancel_ratio;
    row.mo_ema_bid = mo_ema_bid;
    row.mo_ema_ask = mo_ema_ask;
    row.fair = quote.fair;
    row.mid = mid;
    row.best_bid = best_bid;
    row.best_ask = best_ask;
    row.raw_pair_spread = std::max(quote.sell.raw_price - quote.buy.raw_price, 0.0);
    row.capped_pair_spread = std::max(quote.sell.pre_guard_price - quote.buy.pre_guard_price, 0.0);
    row.final_pair_spread = ctx.final_pair_spread;
    row.raw_price = ctx.raw_price;
    row.pre_guard_price = ctx.pre_guard_price;
    row.final_price = ctx.final_price;
    row.raw_quote_delta_to_bbo = ctx.raw_quote_delta_to_bbo;
    row.pre_guard_delta_to_bbo = ctx.pre_guard_delta_to_bbo;
    row.final_quote_delta_to_bbo = ctx.final_quote_delta_to_bbo;
    row.raw_distance_to_mid = ctx.raw_distance_to_mid;
    row.final_distance_to_mid = ctx.final_distance_to_mid;
    row.raw_quote_skew = quote.raw_quote_skew;
    row.final_quote_skew = ctx.final_quote_skew;
    row.raw_bias_side = quote.raw_mid_shift > 0.0
        ? BiasSide::Buy
        : (quote.raw_mid_shift < 0.0 ? BiasSide::Sell : BiasSide::Balanced);
    row.delta_cap = quote.flags.delta_cap;
    row.mid_guard = ctx.mid_guard;
    row.post_only = ctx.post_only;
    row.side_adverse = ctx.side_adverse;
    row.side_adverse_pause = ctx.side_adverse_pause;
    row.adverse_toxicity = ctx.adverse_toxicity;
    row.adverse_markout = ctx.adverse_markout;
    row.adverse_direction = ctx.adverse_direction;
    row.adverse_ret = ctx.adverse_ret;
    row.adverse_microprice = ctx.adverse_microprice;
    row.adverse_thin_depth = ctx.adverse_thin_depth;
    row.local_extreme_guard = ctx.local_extreme_guard;
    row.local_extreme_pause = ctx.local_extreme_pause;
    row.local_extreme_rank = ctx.local_extreme_rank;
    row.local_extreme_window_s = ctx.local_extreme_window_s;
    row.defense_guard = ctx.defense_guard;
    row.defense_pause = ctx.defense_pause;
    row.defense_reducing = ctx.defense_reducing;
    row.defense_emergency = ctx.defense_emergency;
    row.defense_markout = ctx.defense_markout;
    row.defense_direction = ctx.defense_direction;
    row.defense_ret = ctx.defense_ret;
    row.defense_microprice = ctx.defense_microprice;
    row.defense_spread_mult = ctx.defense_spread_mult;
    row.final_compressed = quote.flags.final_compressed || post_policy_cap_hit;
    row.bid_adverse = quote.flags.bid_adverse;
    row.ask_adverse = quote.flags.ask_adverse;
    row.buy_fill_selection_live_score = ctx.buy_fill_selection_live_score;
    row.buy_fill_selection_live_hit = ctx.buy_fill_selection_live_hit;
    row.buy_fill_selection_live_missing_features = ctx.buy_fill_selection_live_missing_features;
    row.random_passive_mirrored = random_passive_mirrored;
    row.final_guard_changed = ctx.final_guard_changed;
    row.any_constraint_changed = ctx.any_constraint_changed;
    row.remaining = quantity;
    return row;
}

void append_order_trace(
    TickReplayResult& result,
    const ReplayOrder& order,
    std::int64_t ts,
    TraceOutcome outcome,
    CancelReason reason,
    double fill_qty
) {
    if (!order.trace) {
        return;
    }
    TraceOrderRow row = *order.trace;
    row.outcome = outcome;
    row.outcome_ts = ts;
    row.lifetime_ms = std::max<std::int64_t>(0, ts - order.quote_ts);
    row.cancel_reason = reason;
    row.fill_qty = fill_qty;
    row.remaining = order.remaining;
    const bool activated = order.exchange_accepted;
    row.queue_init = activated ? order.queue_init : 0.0;
    row.queue_left = activated ? order.queue_left : 0.0;
    row.pending_cancel = order.state == OrderState::PendingCancel;
    result.quote_trace.push_back(row);
}

double markout_at_horizon(
    const TickReplayInput& input,
    std::size_t fill_idx,
    std::int64_t horizon_ms,
    double quote_price,
    Side side
) {
    const std::int64_t target = input.trade_ts_ms.data()[fill_idx] + horizon_ms;
    const auto first = input.trade_ts_ms.begin() + static_cast<std::ptrdiff_t>(fill_idx);
    auto it = std::lower_bound(first, input.trade_ts_ms.end(), target);
    if (it == input.trade_ts_ms.end()) {
        --it;
    }
    const auto index = static_cast<std::size_t>(it - input.trade_ts_ms.begin());
    const double future = input.trade_price[index];
    return side == Side::Buy ? future - quote_price : quote_price - future;
}

void append_fill_trace(
    TickReplayResult& result,
    const TickReplayInput& input,
    const ReplayOrder& order,
    std::size_t fill_idx,
    double trade_price,
    double queue_before,
    double rem_before,
    double fill_qty,
    double inventory_before_fill,
    double inventory_after_fill,
    double maker_fee,
    std::int64_t trace_window_ms,
    std::int64_t max_rows
) {
    if (max_rows <= 0 || result.fill_trace.size() >= static_cast<std::size_t>(max_rows)) {
        return;
    }
    TraceFillRow row;
    row.fill_sequence = static_cast<std::int64_t>(result.fill_trace.size());
    row.side = order.side;
    row.fill_ts = input.trade_ts_ms.data()[fill_idx];
    row.quote_ts = order.quote_ts;
    row.age_ms = std::max<std::int64_t>(0, row.fill_ts - order.quote_ts);
    row.quote_mid = order.mid_at_quote;
    row.quote_px = order.price;
    row.fill_trade_px = trade_price;
    row.quote_dist = order.side == Side::Buy ? order.mid_at_quote - order.price : order.price - order.mid_at_quote;
    row.queue_init = order.queue_init;
    row.queue_before = queue_before;
    row.rem_before = rem_before;
    row.fill_qty = fill_qty;
    row.fill_fee_rate = maker_fee;
    row.fill_fee_usdc = order.price * fill_qty * maker_fee;
    row.inventory_before_fill = inventory_before_fill;
    row.inventory_after_fill = inventory_after_fill;
    row.markout_1s = markout_at_horizon(input, fill_idx, 1'000, order.price, order.side);
    row.markout_5s = markout_at_horizon(input, fill_idx, 5'000, order.price, order.side);
    row.markout_20s = markout_at_horizon(input, fill_idx, 20'000, order.price, order.side);
    row.markout_30s = markout_at_horizon(input, fill_idx, 30'000, order.price, order.side);
    row.ev_1s = row.markout_1s - maker_fee * order.price;
    row.ev_5s = row.markout_5s - maker_fee * order.price;
    row.ev_20s = row.markout_20s - maker_fee * order.price;
    row.ev_30s = row.markout_30s - maker_fee * order.price;
    row.toxic_1s = row.markout_1s < 0.0;
    row.toxic_5s = row.markout_5s < 0.0;
    row.toxic_20s = row.markout_20s < 0.0;
    row.toxic_30s = row.markout_30s < 0.0;

    const std::int64_t start = row.fill_ts - std::max<std::int64_t>(1'000, trace_window_ms);
    const std::int64_t end = row.fill_ts + std::max<std::int64_t>(1'000, trace_window_ms);
    bool seen = false;
    double mn = trade_price;
    double mx = trade_price;
    const auto first = std::lower_bound(input.trade_ts_ms.begin(), input.trade_ts_ms.end(), start);
    const auto last = std::upper_bound(first, input.trade_ts_ms.end(), end);
    for (auto it = first; it != last; ++it) {
        const std::size_t j = static_cast<std::size_t>(it - input.trade_ts_ms.begin());
        const double px = input.trade_price.data()[j];
        if (px > 0.0) {
            mn = seen ? std::min(mn, px) : px;
            mx = seen ? std::max(mx, px) : px;
            seen = true;
        }
    }
    row.window5_min = mn;
    row.window5_max = mx;
    row.window10_min = mn;
    row.window10_max = mx;
    row.window120_min = mn;
    row.window120_max = mx;
    if (mx - mn > 1e-12) {
        row.window120_rank = clamp((trade_price - mn) / (mx - mn), 0.0, 1.0);
    }
    if (order.side == Side::Buy) {
        row.quote_window_extreme = mn;
        row.quote_window_move = order.mid_at_quote - mn;
    } else {
        row.quote_window_extreme = mx;
        row.quote_window_move = mx - order.mid_at_quote;
    }
    row.move_from_quote_mid_to_fill = std::abs(trade_price - order.mid_at_quote);
    if (order.trace) {
        row.order = *order.trace;
        // Python records the executable touch as `price` for IOC fills while
        // preserving the submitted limit in the remaining quote fields.
        row.order.price = order.price;
    }
    result.fill_trace.push_back(row);
}

template <Side S>
double apply_replay_side_policy_price(double price, double mid, double spread_mult, double tick) {
    if (mid <= 0.0 || std::abs(spread_mult - 1.0) <= 1e-12) {
        return price;
    }
    spread_mult = std::max(0.05, spread_mult);
    if constexpr (is_buy_v<S>) {
        const double dist = std::max(mid - price, tick);
        const double adjusted =
            std::floor((mid - dist * spread_mult) / tick) * tick;
        return std::min(adjusted, mid - tick);
    } else {
        const double dist = std::max(price - mid, tick);
        const double adjusted =
            std::ceil((mid + dist * spread_mult) / tick) * tick;
        return std::max(adjusted, mid + tick);
    }
}

enum CommonPolicyReasonCpp : std::uint32_t {
    PolicyReasonFillCooldown = 1U << 0,
    PolicyReasonMarkout = 1U << 2,
    PolicyReasonStaleWarn = 1U << 3,
    PolicyReasonStaleHard = 1U << 4,
    PolicyReasonBurst = 1U << 5,
    PolicyReasonThinDepth = 1U << 6,
    PolicyReasonInventoryLimit = 1U << 7,
    PolicyReasonAdverse = 1U << 9,
    PolicyReasonDefense = 1U << 12,
};

struct CommonSidePolicyCpp {
    bool allow_post = true;
    bool allow_exposure_increase = true;
    double spread_mult = 1.0;
    double size_mult = 1.0;
    std::uint32_t reason_mask = 0;
};

CommonSidePolicyCpp evaluate_common_side_policy_cpp(
    const SideQuoteContext& ctx,
    bool exposure_increasing,
    bool fill_cooldown_active,
    double inventory_ratio,
    double depth_age_s,
    double max_book_age_s,
    double markout_ema,
    double markout_spread_scale,
    double mo_ref,
    double microprice_shift_bps,
    double kappa_depth_baseline,
    double thin_depth_threshold
) {
    CommonSidePolicyCpp out;
    if (fill_cooldown_active) {
        out.allow_post = false;
        out.reason_mask |= PolicyReasonFillCooldown;
    }
    if (max_book_age_s > 0.0) {
        if (!std::isfinite(depth_age_s) || depth_age_s < 0.0 ||
            depth_age_s >= max_book_age_s) {
            out.allow_post = false;
            out.reason_mask |= PolicyReasonStaleHard;
        } else if (depth_age_s >= 0.5 * max_book_age_s) {
            out.spread_mult = std::max(out.spread_mult, 1.25);
            out.size_mult = std::min(out.size_mult, 0.65);
            out.reason_mask |= PolicyReasonStaleWarn;
        }
    }
    if (markout_ema < 0.0 && markout_spread_scale > 0.0) {
        const double severity = std::min(std::abs(markout_ema) / std::max(mo_ref, 1e-6), 1.0);
        out.spread_mult = std::max(out.spread_mult, 1.05 + 0.25 * severity);
        out.size_mult = std::min(out.size_mult, 0.85 - 0.35 * severity);
        out.reason_mask |= PolicyReasonMarkout;
    }
    if (ctx.side_adverse) {
        out.size_mult = std::min(out.size_mult, 0.70);
        out.reason_mask |= PolicyReasonAdverse;
        if (ctx.side_adverse_pause) {
            out.allow_exposure_increase = false;
        }
    }
    if (ctx.local_extreme_guard) {
        out.spread_mult = std::max(out.spread_mult, std::max(1.0, ctx.local_extreme_spread_mult));
        out.reason_mask |= PolicyReasonAdverse;
        if (ctx.local_extreme_pause) {
            out.allow_exposure_increase = false;
        }
    }
    if (ctx.defense_guard) {
        out.spread_mult = std::max(out.spread_mult, std::max(1.0, ctx.defense_spread_mult));
        out.size_mult = std::min(out.size_mult, 0.70);
        out.reason_mask |= PolicyReasonDefense;
        if (ctx.defense_pause) {
            out.allow_post = false;
        }
    }
    if (ctx.l2_quote_flip_rate >= 0.35 &&
        ctx.l2_book_cancel_ratio >= 0.04 &&
        std::abs(microprice_shift_bps) >= 0.5) {
        out.allow_exposure_increase = false;
        out.spread_mult = std::max(out.spread_mult, 1.35);
        out.size_mult = std::min(out.size_mult, 0.45);
        out.reason_mask |= PolicyReasonBurst;
    }
    const double thin_depth_thr = thin_depth_threshold > 0.0
        ? thin_depth_threshold
        : std::max(1.0, kappa_depth_baseline * 0.5);
    // Live and authoritative Python policy use the wall-clock L2 metric when
    // available; quote-core depth is only a fallback. Taking the maximum can
    // hide a thin top-of-book state and change side-policy widening.
    const double near_depth = ctx.l2_near_depth_total > 0.0
        ? ctx.l2_near_depth_total
        : ctx.near_depth_total;
    if (near_depth > 0.0 && near_depth < thin_depth_thr) {
        out.spread_mult = std::max(out.spread_mult, 1.10);
        out.size_mult = std::min(out.size_mult, 0.75);
        out.reason_mask |= PolicyReasonThinDepth;
    }
    if (inventory_ratio >= 0.98 && exposure_increasing) {
        out.allow_exposure_increase = false;
        out.reason_mask |= PolicyReasonInventoryLimit;
    }
    out.spread_mult = std::max(1.0, out.spread_mult);
    out.size_mult = clamp(out.size_mult, 0.0, 1.0);
    return out;
}

void refresh_final_context(
    SideQuoteContext& bid_ctx,
    SideQuoteContext& ask_ctx,
    double mid,
    double best_bid,
    double best_ask,
    double bid_price,
    double ask_price,
    double tick
) {
    const double pair_spread = std::max(ask_price - bid_price, tick);
    const double quote_skew = pair_spread > 1e-12
        ? ((ask_price - mid) - (mid - bid_price)) / pair_spread
        : 0.0;
    bid_ctx.final_price = bid_price;
    bid_ctx.final_distance_to_mid = mid - bid_price;
    bid_ctx.final_pair_spread = pair_spread;
    bid_ctx.final_quote_skew = quote_skew;
    bid_ctx.final_quote_delta_to_bbo = best_bid > 0.0 ? best_bid - bid_price : 0.0;
    ask_ctx.final_price = ask_price;
    ask_ctx.final_distance_to_mid = ask_price - mid;
    ask_ctx.final_pair_spread = pair_spread;
    ask_ctx.final_quote_skew = quote_skew;
    ask_ctx.final_quote_delta_to_bbo = best_ask > 0.0 ? ask_price - best_ask : 0.0;
}

BookSnapshot book_snapshot_at(
    const TickReplayInput& input,
    std::int64_t ts,
    std::size_t bbo_idx,
    std::size_t l2_idx,
    double fallback_bid,
    double fallback_ask,
    double tick_size,
    const TickReplayParams& params,
    std::int64_t age_clock_ts = -1
) {
    BookSnapshot out;
    out.best_bid = fallback_bid;
    out.best_ask = fallback_ask > fallback_bid ? fallback_ask : fallback_bid + tick_size;
    out.mid = 0.5 * (out.best_bid + out.best_ask);
    out.fresh = true;
    out.has_book = false;
    out.age_ms = 0;

    bool has_age = false;
    std::int64_t best_age_ms = std::numeric_limits<std::int64_t>::max();
    const std::int64_t age_ts = age_clock_ts >= 0 ? age_clock_ts : ts;

    if (!params.use_bar_pricing && input.bbo_ts_ms.size() > 0) {
        if (bbo_idx < input.bbo_ts_ms.size() && input.bbo_ts_ms[bbo_idx] <= ts) {
            const std::size_t i = bbo_idx;
            out.best_bid = input.bbo_best_bid.data()[i];
            out.best_ask = input.bbo_best_ask.data()[i];
            out.bid_qty = input.bbo_bid_qty.size() == input.bbo_ts_ms.size() ? input.bbo_bid_qty.data()[i] : 0.0;
            out.ask_qty = input.bbo_ask_qty.size() == input.bbo_ts_ms.size() ? input.bbo_ask_qty.data()[i] : 0.0;
            out.has_book = out.best_bid > 0.0 && out.best_ask > out.best_bid;
            best_age_ms = std::min(best_age_ms, std::max<std::int64_t>(0, age_ts - input.bbo_ts_ms.data()[i]));
            has_age = true;
        } else {
            has_age = false;
        }
    }
    if (!params.use_bar_pricing && input.l2_ts_ms.size() > 0 && !input.l2_bid_px.empty()) {
        if (l2_idx < input.l2_ts_ms.size() && input.l2_ts_ms[l2_idx] <= ts) {
            const std::size_t i = l2_idx;
            if (!out.has_book) {
                out.best_bid = input.l2_bid_px(i, 0);
                out.best_ask = input.l2_ask_px(i, 0);
                out.bid_qty = input.l2_bid_qty(i, 0);
                out.ask_qty = input.l2_ask_qty(i, 0);
                out.has_book = out.best_bid > 0.0 && out.best_ask > out.best_bid;
            }
            best_age_ms = std::min(best_age_ms, std::max<std::int64_t>(0, age_ts - input.l2_ts_ms.data()[i]));
            has_age = true;
        }
    }

    if (!params.use_bar_pricing && (input.bbo_ts_ms.size() > 0 || input.l2_ts_ms.size() > 0)) {
        out.fresh = has_age && (params.max_exec_book_age_ms <= 0 || best_age_ms <= params.max_exec_book_age_ms);
        out.age_ms = has_age ? best_age_ms : params.max_exec_book_age_ms;
    }
    if (out.best_bid > 0.0 && out.best_ask > out.best_bid) {
        out.mid = 0.5 * (out.best_bid + out.best_ask);
    }
    return out;
}

DepthView l2_depth_at(
    const TickReplayInput& input,
    std::int64_t ts,
    std::size_t l2_idx
) {
    // 返回指向 L2 矩阵当前行的零分配 DepthView；quote core 不能保存该 View。
    if (input.l2_ts_ms.empty() || input.l2_bid_px.empty() ||
        l2_idx >= input.l2_ts_ms.size() || input.l2_ts_ms[l2_idx] > ts) {
        return {};
    }
    return DepthView{
        DepthSideView{{}, input.l2_bid_px.row(l2_idx), input.l2_bid_qty.row(l2_idx)},
        DepthSideView{{}, input.l2_ask_px.row(l2_idx), input.l2_ask_qty.row(l2_idx)},
    };
}

struct L2RefillCancelMetrics {
    double quote_flip_rate = 0.0;
    double book_refresh_ratio = 0.0;
    double book_cancel_ratio = 0.0;
    double near_depth_total = 0.0;
};

double raw_l2_near_qty(const MatrixView<double>& qty, std::size_t row, int levels) {
    if (qty.empty() || row >= qty.rows || levels <= 0) {
        return 0.0;
    }
    const std::size_t n = std::min<std::size_t>(
        static_cast<std::size_t>(std::max(1, levels)),
        qty.cols
    );
    double total = 0.0;
    for (std::size_t col = 0; col < n; ++col) {
        const double q = qty(row, col);
        if (std::isfinite(q) && q > 0.0) {
            total += q;
        }
    }
    return total;
}

L2RefillCancelMetrics l2_refill_cancel_metrics_at(const TickReplayInput& input,
                                                   std::int64_t visible_l2_ts,
                                                   std::int64_t observation_ts,
                                                   std::size_t cur_idx,
                                                   const TickReplayParams& params) {
    L2RefillCancelMetrics out;
    if (input.l2_ts_ms.empty() || input.l2_bid_px.empty() || input.l2_bid_qty.empty() ||
        input.l2_ask_px.empty() || input.l2_ask_qty.empty() ||
        cur_idx >= input.l2_ts_ms.size() ||
        input.l2_ts_ms[cur_idx] > visible_l2_ts) {
        return out;
    }
    const int near_levels = std::max(1, params.l2_refill_cancel_near_levels);
    const double cur_bid = raw_l2_near_qty(input.l2_bid_qty, cur_idx, near_levels);
    const double cur_ask = raw_l2_near_qty(input.l2_ask_qty, cur_idx, near_levels);
    out.near_depth_total = cur_bid + cur_ask;

    const int policy_levels = std::max(1, params.l2_policy_depth_levels);
    const std::int64_t lookback_ms = std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(
            std::llround(params.l2_refill_cancel_lookback_s * 1000.0)
        )
    );
    const std::int64_t start_ts = observation_ts - lookback_ms;
    const auto* first = input.l2_ts_ms.data();
    const auto* last = first + input.l2_ts_ms.size();
    const auto* lower = std::lower_bound(first, last, start_ts);
    std::size_t start_idx = static_cast<std::size_t>(lower - first);
    if (start_idx > cur_idx) {
        start_idx = cur_idx;
    }

    std::int64_t sample_count = 0;
    double flip_count = 0.0;
    double refresh_sum = 0.0;
    double cancel_sum = 0.0;
    double previous_total = 0.0;
    double previous_bid = 0.0;
    double previous_ask = 0.0;
    bool have_previous = false;
    for (std::size_t idx = start_idx; idx <= cur_idx; ++idx) {
        const double bid = input.l2_bid_px(idx, 0);
        const double ask = input.l2_ask_px(idx, 0);
        if (!std::isfinite(bid) || !std::isfinite(ask) ||
            bid <= 0.0 || ask <= bid) {
            continue;
        }
        const double total_depth =
            raw_l2_near_qty(input.l2_bid_qty, idx, policy_levels) +
            raw_l2_near_qty(input.l2_ask_qty, idx, policy_levels);
        if (have_previous) {
            if (bid != previous_bid || ask != previous_ask) {
                flip_count += 1.0;
            }
            if (previous_total > 1e-12) {
                const double delta = total_depth - previous_total;
                if (delta > 0.0) {
                    refresh_sum += delta / previous_total;
                } else if (delta < 0.0) {
                    cancel_sum += -delta / previous_total;
                }
            }
        }
        previous_total = total_depth;
        previous_bid = bid;
        previous_ask = ask;
        have_previous = true;
        ++sample_count;
    }
    if (sample_count > 0) {
        const double denominator = static_cast<double>(sample_count);
        out.quote_flip_rate = flip_count / denominator;
        out.book_refresh_ratio = refresh_sum / denominator;
        out.book_cancel_ratio = cancel_sum / denominator;
    }
    return out;
}

template <Side S>
double l2_visible_queue_ahead(
    const TickReplayInput& input,
    double quote_price,
    std::int64_t target_ts,
    double tick_size
) {
    // 挂单激活时间可能落在两笔 trade 之间，所以这里仍按 target_ts 回看 L2 行。
    // 若未来在下单路径改成纯游标，必须确认 latency/cancel 生效时间不会倒退。
    if (input.l2_ts_ms.size() == 0 || input.l2_bid_px.empty()) {
        return -1.0;
    }
    const auto idx = index_at_or_before(input.l2_ts_ms, target_ts);
    if (idx < 0) {
        return -1.0;
    }
    const std::size_t row = static_cast<std::size_t>(idx);
    const auto quote_tick = price_to_tick(quote_price, tick_size);
    double visible = 0.0;
    bool matched = false;
    std::int64_t min_tick = std::numeric_limits<std::int64_t>::max();
    std::int64_t max_tick = std::numeric_limits<std::int64_t>::min();
    if constexpr (is_buy_v<S>) {
        for (std::size_t col = 0; col < input.l2_bid_px.cols; ++col) {
            const double px = input.l2_bid_px(row, col);
            if (std::isfinite(px) && px > 0.0) {
                const auto level_tick = price_to_tick_unchecked(px, tick_size);
                min_tick = std::min(min_tick, level_tick);
                max_tick = std::max(max_tick, level_tick);
            }
            const bool include = std::isfinite(px) && px > 0.0 &&
                price_to_tick_unchecked(px, tick_size) == quote_tick;
            if (include && !duplicate_l2_price_before(
                    input.l2_bid_px, row, col, tick_size)) {
                matched = true;
                visible += std::max(0.0, input.l2_bid_qty(row, col));
            }
        }
        if (!matched && min_tick != std::numeric_limits<std::int64_t>::max()
                && quote_tick < min_tick) {
            return -1.0;
        }
    } else {
        for (std::size_t col = 0; col < input.l2_ask_px.cols; ++col) {
            const double px = input.l2_ask_px(row, col);
            if (std::isfinite(px) && px > 0.0) {
                const auto level_tick = price_to_tick_unchecked(px, tick_size);
                min_tick = std::min(min_tick, level_tick);
                max_tick = std::max(max_tick, level_tick);
            }
            const bool include = std::isfinite(px) && px > 0.0 &&
                price_to_tick_unchecked(px, tick_size) == quote_tick;
            if (include && !duplicate_l2_price_before(
                    input.l2_ask_px, row, col, tick_size)) {
                matched = true;
                visible += std::max(0.0, input.l2_ask_qty(row, col));
            }
        }
        if (!matched && max_tick != std::numeric_limits<std::int64_t>::min()
                && quote_tick > max_tick) {
            return -1.0;
        }
    }
    return visible;
}

template <Side S>
double estimate_queue_ahead(
    const TickReplayInput& input,
    const TickReplayParams& params,
    double quote_price,
    double dist_from_mid,
    std::int64_t target_ts,
    double queue_base,
    double queue_decay,
    std::uint8_t* queue_seed_source = nullptr
) {
    const double visible_l2 = l2_visible_queue_ahead<S>(
        input,
        quote_price,
        target_ts,
        params.quote.tick_size
    );
    if (visible_l2 >= 0.0) {
        if (queue_seed_source != nullptr) {
            *queue_seed_source = visible_l2 > 0.0 ? 1 : 2;
        }
        return visible_l2;
    }
    if (queue_seed_source != nullptr) {
        *queue_seed_source = 3;
    }
    return queue_ahead_estimate(queue_base, queue_decay, dist_from_mid);
}

inline double nonnegative_or_one(double value) {
    return std::isfinite(value) ? std::max(0.0, value) : 1.0;
}

int bin_index_cpp(double value, const std::vector<double>& edges) {
    if (!std::isfinite(value)) {
        return 0;
    }
    int idx = 0;
    for (double edge : edges) {
        // Match Python queue_calibration._bin_index:
        // np.searchsorted(edges, value, side="right").  Values exactly on an
        // edge belong to the higher bin, which matters because quote distance
        // is tick-rounded and can land exactly on calibration edges.
        if (value < edge) {
            return idx;
        }
        ++idx;
    }
    return idx;
}

int queue_reason_index_cpp(const SideQuoteContext& ctx,
                           const TickReplayParams& params,
                           double near_depth_total) {
    const bool adverse = ctx.side_adverse;
    const bool markout = ctx.adverse_markout || ctx.defense_markout;
    const double thin_threshold = std::max(params.thin_depth_threshold, params.quote.adverse_thin_depth_threshold);
    const bool thin_depth = ctx.adverse_thin_depth ||
        (thin_threshold > 0.0 && near_depth_total > 0.0 && near_depth_total < thin_threshold);
    if (adverse) {
        return 1;
    }
    if (markout && thin_depth) {
        return 4;
    }
    if (markout) {
        return 2;
    }
    if (thin_depth) {
        return 3;
    }
    return 0;
}

template <Side S>
double queue_regime_multiplier_cpp(const TickReplayParams& params,
                                   double dist_from_mid,
                                   double local_rank,
                                   const SideQuoteContext& ctx,
                                   double near_depth_total) {
    const auto& table = is_buy_v<S> ? params.queue_regime_buy_mult : params.queue_regime_sell_mult;
    if (table.empty()) {
        return 1.0;
    }
    const std::size_t dist_bins = params.queue_regime_distance_edges.size() + 1;
    const std::size_t rank_bins = params.queue_regime_rank_edges.size() + 1;
    constexpr std::size_t reason_bins = 5;
    if (dist_bins == 0 || rank_bins == 0) {
        return 1.0;
    }
    const std::size_t dist_bin = static_cast<std::size_t>(std::max(0, bin_index_cpp(dist_from_mid, params.queue_regime_distance_edges)));
    const std::size_t rank_bin = static_cast<std::size_t>(std::max(0, bin_index_cpp(local_rank, params.queue_regime_rank_edges)));
    const std::size_t reason_bin = static_cast<std::size_t>(
        std::max(0, queue_reason_index_cpp(ctx, params, near_depth_total))
    );
    const std::size_t idx = ((std::min(dist_bin, dist_bins - 1) * rank_bins + std::min(rank_bin, rank_bins - 1))
        * reason_bins) + std::min(reason_bin, reason_bins - 1);
    if (idx >= table.size()) {
        return 1.0;
    }
    return nonnegative_or_one(table[idx]);
}

template <Side S>
double queue_mo_multiplier_cpp(const TickReplayParams& params, double markout_ema) {
    const auto& table = is_buy_v<S> ? params.queue_mo_buy_mult : params.queue_mo_sell_mult;
    if (table.empty()) {
        return 1.0;
    }
    const std::size_t mo_bins = params.queue_mo_edges.size() + 1;
    const std::size_t mo_bin = static_cast<std::size_t>(std::max(0, bin_index_cpp(markout_ema, params.queue_mo_edges)));
    const std::size_t idx = std::min(mo_bin, mo_bins - 1);
    if (idx >= table.size()) {
        return 1.0;
    }
    return nonnegative_or_one(table[idx]);
}

template <Side S>
double queue_deplete_multiplier_cpp(const TickReplayParams& params, double local_rank) {
    const auto& table = is_buy_v<S> ? params.queue_deplete_buy_mult : params.queue_deplete_sell_mult;
    if (table.empty()) {
        return 1.0;
    }
    const std::size_t rank_bins = params.queue_deplete_rank_edges.size() + 1;
    const std::size_t rank_bin = static_cast<std::size_t>(std::max(0, bin_index_cpp(local_rank, params.queue_deplete_rank_edges)));
    const std::size_t idx = std::min(rank_bin, rank_bins - 1);
    if (idx >= table.size()) {
        return 1.0;
    }
    return nonnegative_or_one(table[idx]);
}

template <Side S>
std::pair<double, bool> queue_inventory_multiplier(
    const TickReplayParams& params,
    double inventory_at_quote
) {
    const bool exposure_increasing = is_buy_v<S>
        ? inventory_at_quote >= 0.0
        : inventory_at_quote <= 0.0;
    double mult = nonnegative_or_one(params.queue_ahead_base_mult);
    if constexpr (is_buy_v<S>) {
        mult *= exposure_increasing
            ? nonnegative_or_one(params.queue_ahead_buy_exposure_mult)
            : nonnegative_or_one(params.queue_ahead_buy_reducing_mult);
    } else {
        mult *= exposure_increasing
            ? nonnegative_or_one(params.queue_ahead_sell_exposure_mult)
            : nonnegative_or_one(params.queue_ahead_sell_reducing_mult);
    }
    return {std::max(0.0, mult), exposure_increasing};
}

struct NativeTapeAdvance {
    std::int64_t exchange_ts_ns = 0;
    bool snapshot_reset = false;
    bool invalidated = false;
    std::vector<NativeBookLevelChange> changes;
};

struct NativeTapePreview {
    std::int64_t exchange_ts_ns = 0;
    std::int64_t event_count = 0;
    bool snapshot_or_gap = false;
    std::set<std::pair<bool, std::int64_t>> touched_levels;
};

// Owns the complete immutable native snapshot/delta tape while the monolithic
// replay runs.  It deliberately exposes only causal boundary operations: book
// messages strictly before an order lifecycle boundary, the lifecycle itself,
// then messages exactly at that boundary.  This mirrors the Python strict path
// without a Python callback on the hot loop.
class NativeBookTapeRuntime {
public:
    explicit NativeBookTapeRuntime(const TickReplayParams& params)
        : params_(params),
          book_(true, params.native_exchange_book_strict_after_ns, false) {
        validate();
    }

    [[nodiscard]] std::int64_t boundary_ns() const noexcept {
        return boundary_ns_;
    }

    [[nodiscard]] NativeTapePreview preview_at(std::int64_t target_ns) const {
        NativeTapePreview preview;
        preview.exchange_ts_ns = target_ns;
        std::size_t cursor = cursor_;
        while (cursor < params_.native_book_event_ts_ns.size() &&
               params_.native_book_event_ts_ns[cursor] == target_ns) {
            ++preview.event_count;
            const auto type = event_type(cursor);
            preview.snapshot_or_gap = preview.snapshot_or_gap ||
                type == NativeBookEventType::Snapshot ||
                type == NativeBookEventType::SourceGap;
            const auto [first, last] = level_bounds(cursor);
            for (std::size_t level = first; level < last; ++level) {
                preview.touched_levels.emplace(
                    params_.native_book_level_is_bid[level] != 0,
                    params_.native_book_level_price_tick[level]
                );
            }
            ++cursor;
        }
        return preview;
    }

    [[nodiscard]] NativeBookLookup lookup_strictly_before(
        bool is_bid,
        std::int64_t price_tick,
        std::int64_t target_ns
    ) const {
        const auto current = book_.lookup(is_bid, price_tick);
        if (current.asof_exchange_ts_ns < target_ns) {
            return current;
        }
        if (current.asof_exchange_ts_ns > target_ns ||
            latest_batch_ts_ns_ != target_ns) {
            NativeBookLookup unavailable = current;
            unavailable.status = "unknown";
            unavailable.reason = "strict_before_state_not_retained";
            unavailable.quantity = 0.0;
            unavailable.quantity_known = false;
            return unavailable;
        }
        if (latest_batch_discontinuous_ ||
            latest_batch_segment_id_ != latest_batch_prior_segment_id_ ||
            latest_batch_initialized_ != latest_batch_prior_initialized_) {
            NativeBookLookup unavailable = current;
            unavailable.status = "ambiguous";
            unavailable.reason = "same_timestamp_book_discontinuity";
            unavailable.quantity = 0.0;
            unavailable.quantity_known = false;
            return unavailable;
        }
        if (!latest_batch_prior_initialized_) {
            NativeBookLookup unavailable = current;
            unavailable.status = "ambiguous";
            unavailable.reason = "strict_before_sequence_unavailable";
            unavailable.quantity = 0.0;
            unavailable.quantity_known = false;
            return unavailable;
        }
        if (
            latest_batch_touched_levels_.contains({is_bid, price_tick})) {
            NativeBookLookup unavailable = current;
            unavailable.status = "ambiguous";
            unavailable.reason = "same_timestamp_level_touched";
            unavailable.quantity = 0.0;
            unavailable.quantity_known = false;
            return unavailable;
        }
        // If this batch did not touch the level and did not change the book
        // segment, the quantity is identical on both sides of the boundary.
        // Retain only the prior identity instead of copying the complete book
        // for every native message batch (which is prohibitive on full L2).
        NativeBookLookup prior = current;
        prior.asof_exchange_ts_ns = latest_batch_prior_asof_ns_;
        return prior;
    }

    template <typename Callback>
    void advance_to(
        std::int64_t target_ns,
        bool inclusive,
        TickReplaySummary& summary,
        Callback&& on_batch
    ) {
        if (target_ns < boundary_ns_) {
            // A zero-latency order may be created after the native batch at
            // the same millisecond was consumed.  The retained batch identity
            // supports its strict-before seed; older rewinds remain invalid.
            if (target_ns == latest_batch_ts_ns_) {
                return;
            }
            throw std::runtime_error("native exchange-book boundary regressed");
        }
        while (cursor_ < params_.native_book_event_ts_ns.size()) {
            const auto batch_ts = params_.native_book_event_ts_ns[cursor_];
            if (batch_ts > target_ns || (batch_ts == target_ns && !inclusive)) {
                break;
            }
            latest_batch_ts_ns_ = batch_ts;
            latest_batch_prior_asof_ns_ = book_.last_exchange_ts_ns();
            latest_batch_prior_segment_id_ = book_.segment_id();
            latest_batch_prior_initialized_ = book_.initialized();
            latest_batch_touched_levels_.clear();
            latest_batch_discontinuous_ = false;

            NativeTapeAdvance advance;
            advance.exchange_ts_ns = batch_ts;
            while (cursor_ < params_.native_book_event_ts_ns.size() &&
                   params_.native_book_event_ts_ns[cursor_] == batch_ts) {
                const std::size_t event = cursor_++;
                const auto type = event_type(event);
                latest_batch_discontinuous_ = latest_batch_discontinuous_ ||
                    type == NativeBookEventType::Snapshot ||
                    type == NativeBookEventType::SourceGap;
                std::vector<NativeBookLevel> levels;
                const auto [first, last] = level_bounds(event);
                levels.reserve(last - first);
                for (std::size_t level = first; level < last; ++level) {
                    const bool is_bid =
                        params_.native_book_level_is_bid[level] != 0;
                    const auto price_tick =
                        params_.native_book_level_price_tick[level];
                    latest_batch_touched_levels_.emplace(is_bid, price_tick);
                    levels.push_back(NativeBookLevel{
                        is_bid,
                        price_tick,
                        params_.native_book_level_quantity[level],
                    });
                }
                const auto result = book_.apply_message(
                    type,
                    batch_ts,
                    params_.native_book_receive_ts_ns[event],
                    params_.native_book_event_time_ms[event],
                    params_.native_book_transaction_time_ms[event],
                    params_.native_book_first_update_id[event],
                    params_.native_book_final_update_id[event],
                    params_.native_book_previous_final_update_id[event],
                    params_.native_book_last_update_id[event],
                    levels
                );
                ++summary.native_book_events_consumed;
                summary.native_book_events_accepted += result.accepted ? 1 : 0;
                summary.native_book_events_rejected += result.accepted ? 0 : 1;
                summary.native_book_snapshot_events +=
                    result.snapshot_reset ? 1 : 0;
                advance.snapshot_reset =
                    advance.snapshot_reset || result.snapshot_reset;
                advance.invalidated = advance.invalidated || result.invalidated;
                latest_batch_discontinuous_ =
                    latest_batch_discontinuous_ || result.invalidated;
                advance.changes.insert(
                    advance.changes.end(),
                    result.changes.begin(), result.changes.end()
                );
            }
            latest_batch_segment_id_ = book_.segment_id();
            latest_batch_initialized_ = book_.initialized();
            on_batch(advance);
        }
        boundary_ns_ = std::max(boundary_ns_, target_ns);
        const auto& stats = book_.stats();
        summary.native_book_sequence_gaps = stats.sequence_gaps;
    }

private:
    void validate() const {
        const auto events = params_.native_book_event_ts_ns.size();
        const auto require_events = [events](std::size_t size, const char* name) {
            if (size != events) {
                throw std::invalid_argument(
                    std::string("native ") + name + " length mismatch"
                );
            }
        };
        require_events(params_.native_book_event_type.size(), "event_type");
        require_events(params_.native_book_receive_ts_ns.size(), "receive_ts_ns");
        require_events(params_.native_book_event_time_ms.size(), "event_time_ms");
        require_events(
            params_.native_book_transaction_time_ms.size(),
            "transaction_time_ms"
        );
        require_events(params_.native_book_first_update_id.size(), "first_update_id");
        require_events(params_.native_book_final_update_id.size(), "final_update_id");
        require_events(
            params_.native_book_previous_final_update_id.size(),
            "previous_final_update_id"
        );
        require_events(params_.native_book_last_update_id.size(), "last_update_id");
        if (params_.native_book_level_offsets.size() != events + 1 ||
            params_.native_book_level_offsets.empty() ||
            params_.native_book_level_offsets.front() != 0) {
            throw std::invalid_argument("native level offsets are malformed");
        }
        const auto levels = params_.native_book_level_price_tick.size();
        if (params_.native_book_level_is_bid.size() != levels ||
            params_.native_book_level_quantity.size() != levels ||
            params_.native_book_level_offsets.back() !=
                static_cast<std::int64_t>(levels)) {
            throw std::invalid_argument("native level arrays are malformed");
        }
        for (std::size_t event = 0; event < events; ++event) {
            if (params_.native_book_event_ts_ns[event] <= 0 ||
                (event > 0 && params_.native_book_event_ts_ns[event] <
                    params_.native_book_event_ts_ns[event - 1]) ||
                params_.native_book_level_offsets[event] >
                    params_.native_book_level_offsets[event + 1]) {
                throw std::invalid_argument("native exchange-book tape is not sorted");
            }
            static_cast<void>(event_type(event));
        }
        for (const double quantity : params_.native_book_level_quantity) {
            if (!std::isfinite(quantity) || quantity < 0.0) {
                throw std::invalid_argument(
                    "native exchange-book quantity must be finite and non-negative"
                );
            }
        }
    }

    [[nodiscard]] NativeBookEventType event_type(std::size_t event) const {
        switch (params_.native_book_event_type[event]) {
            case 1: return NativeBookEventType::Snapshot;
            case 2: return NativeBookEventType::Delta;
            case 3: return NativeBookEventType::SourceGap;
            default:
                throw std::invalid_argument("unknown native exchange-book event type");
        }
    }

    [[nodiscard]] std::pair<std::size_t, std::size_t> level_bounds(
        std::size_t event
    ) const {
        const auto first = params_.native_book_level_offsets[event];
        const auto last = params_.native_book_level_offsets[event + 1];
        if (first < 0 || last < first) {
            throw std::invalid_argument("native level offsets regressed");
        }
        return {
            static_cast<std::size_t>(first),
            static_cast<std::size_t>(last),
        };
    }

    const TickReplayParams& params_;
    NativeExchangeBookSchedulerCpp book_;
    std::size_t cursor_ = 0;
    std::int64_t boundary_ns_ = 0;
    std::int64_t latest_batch_ts_ns_ = 0;
    std::int64_t latest_batch_prior_asof_ns_ = 0;
    std::int64_t latest_batch_prior_segment_id_ = 0;
    std::int64_t latest_batch_segment_id_ = 0;
    bool latest_batch_discontinuous_ = false;
    bool latest_batch_prior_initialized_ = false;
    bool latest_batch_initialized_ = false;
    std::set<std::pair<bool, std::int64_t>> latest_batch_touched_levels_;
};

void invalidate_native_queue_paths(
    ReplayOrders& orders,
    TickReplaySummary& summary
) {
    for (auto& order : orders) {
        if (order.native_queue_path_valid) {
            ++summary.native_queue_invalidated_order_count;
        }
        order.native_queue_path_valid = false;
        order.native_queue_invalidated = true;
    }
}

void mark_native_cancel_boundary_ambiguity(
    ReplayOrders& orders,
    const NativeTapePreview& preview,
    std::int64_t boundary_ms,
    TickReplaySummary& summary
) {
    if (preview.event_count <= 0) {
        return;
    }
    for (auto& order : orders) {
        if ((order.state != OrderState::Open &&
             order.state != OrderState::PendingCancel) ||
            order.cancel_effective_ts != boundary_ms) {
            continue;
        }
        const bool touched = preview.snapshot_or_gap ||
            preview.touched_levels.contains({
                order.side == Side::Buy,
                order.price_tick,
            });
        if (!touched) {
            continue;
        }
        if (!order.native_queue_ambiguous) {
            ++summary.native_queue_ambiguous_event_count;
        }
        if (order.native_queue_path_valid) {
            ++summary.native_queue_invalidated_order_count;
        }
        order.native_queue_path_valid = false;
        order.native_queue_ambiguous = true;
    }
}

void mark_native_cancel_trade_ambiguity(
    ReplayOrders& orders,
    std::int64_t boundary_ms,
    std::optional<std::int64_t> sell_min_tick,
    std::optional<std::int64_t> buy_max_tick,
    TickReplaySummary& summary
) {
    for (auto& order : orders) {
        if ((order.state != OrderState::Open &&
             order.state != OrderState::PendingCancel) ||
            order.cancel_effective_ts != boundary_ms) {
            continue;
        }
        const bool crossed = order.side == Side::Buy
            ? (sell_min_tick.has_value() &&
               *sell_min_tick <= order.price_tick)
            : (buy_max_tick.has_value() &&
               *buy_max_tick >= order.price_tick);
        if (!crossed) {
            continue;
        }
        if (!order.native_queue_ambiguous) {
            ++summary.native_queue_ambiguous_event_count;
        }
        if (order.native_queue_path_valid) {
            ++summary.native_queue_invalidated_order_count;
        }
        order.native_queue_path_valid = false;
        order.native_queue_ambiguous = true;
    }
}

void seed_native_order_queue(
    ReplayOrder& order,
    NativeBookTapeRuntime& native_book,
    TickReplaySummary& summary
) {
    ++summary.native_queue_lookup_count;
    const auto lookup = native_book.lookup_strictly_before(
        order.side == Side::Buy,
        order.price_tick,
        order.activate_ts * 1'000'000
    );
    order.native_queue_segment_id = lookup.segment_id;
    order.native_queue_trade_since_update = 0.0;
    order.native_queue_ambiguous = false;
    order.native_queue_invalidated = false;
    if (!lookup.strict_usable()) {
        ++summary.native_queue_missing_count;
        order.native_queue_path_valid = false;
        return;
    }
    if (lookup.status == "exact") {
        ++summary.native_queue_exact_count;
    } else {
        ++summary.native_queue_known_zero_count;
    }
    order.queue_left = std::max(0.0, lookup.quantity);
    order.queue_init = order.queue_left;
    order.queue_seed_source = 4;
    order.native_queue_path_valid = true;
    if (order.trace) {
        order.trace->queue_init = order.queue_init;
        order.trace->queue_left = order.queue_left;
    }
}

void apply_native_book_advance_to_orders(
    const NativeTapeAdvance& advance,
    ReplayOrders& bid_orders,
    ReplayOrders& ask_orders,
    std::optional<std::int64_t> same_ms_sell_min_tick,
    std::optional<std::int64_t> same_ms_buy_max_tick,
    const TickReplayParams& params,
    TickReplaySummary& summary
) {
    if (advance.snapshot_reset || advance.invalidated) {
        invalidate_native_queue_paths(bid_orders, summary);
        invalidate_native_queue_paths(ask_orders, summary);
    }
    for (const auto& change : advance.changes) {
        auto& orders = change.is_bid ? bid_orders : ask_orders;
        const std::int64_t change_ms = change.exchange_ts_ns / 1'000'000;
        for (auto& order : orders) {
            if ((order.state != OrderState::Open &&
                 order.state != OrderState::PendingCancel) ||
                order.price_tick != change.price_tick) {
                continue;
            }
            const bool same_ms_activation = order.activate_ts == change_ms;
            const bool trade_race = change.is_bid
                ? (same_ms_sell_min_tick.has_value() &&
                   *same_ms_sell_min_tick <= change.price_tick)
                : (same_ms_buy_max_tick.has_value() &&
                   *same_ms_buy_max_tick >= change.price_tick);
            if (same_ms_activation || trade_race) {
                if (!order.native_queue_ambiguous) {
                    ++summary.native_queue_ambiguous_event_count;
                }
                if (order.native_queue_path_valid) {
                    ++summary.native_queue_invalidated_order_count;
                }
                order.native_queue_path_valid = false;
                order.native_queue_ambiguous = true;
                continue;
            }
            const bool resumable_ambiguity =
                order.native_queue_ambiguous &&
                !order.native_queue_invalidated &&
                order.queue_seed_source == 4 &&
                order.native_queue_segment_id == change.segment_id;
            if (!order.native_queue_path_valid && !resumable_ambiguity) {
                continue;
            }
            const double explained_trade = std::min(
                std::max(0.0, change.quantity_before),
                std::max(0.0, order.native_queue_trade_since_update)
            );
            const double decrease = std::max(
                0.0,
                change.quantity_before - change.quantity_after
            );
            const double cancellation = std::max(0.0, decrease - explained_trade);
            const double ahead = std::max(0.0, order.queue_left);
            const double public_after_trade = std::max(
                0.0,
                change.quantity_before - explained_trade
            );
            const double behind = std::max(0.0, public_after_trade - ahead);
            const double denominator = ahead + behind;
            const double ahead_probability = denominator > 0.0
                ? ahead / denominator
                : 0.0;
            const double removed = std::min(
                ahead,
                cancellation * ahead_probability
            );
            const double queue_after = std::min(
                std::max(0.0, change.quantity_after),
                subtract_lot_quantity(ahead, removed, params.quote.lot_size)
            );
            const double effective_removed = ahead - queue_after;
            if (effective_removed > 0.0) {
                order.queue_left = queue_after;
                ++summary.native_queue_cancel_ahead_event_count;
                summary.native_queue_cancel_ahead_qty += effective_removed;
            }
            order.native_queue_trade_since_update = 0.0;
        }
    }
}

void record_native_same_price_trade(
    ReplayOrders& orders,
    std::int64_t trade_tick,
    double trade_qty
) {
    if (trade_qty <= 0.0) {
        return;
    }
    for (auto& order : orders) {
        if ((order.state == OrderState::Open ||
             order.state == OrderState::PendingCancel) &&
            order.price_tick == trade_tick) {
            order.native_queue_trade_since_update += trade_qty;
        }
    }
}

template <Side S>
ReplayOrder make_order(
    const TickReplayInput& input,
    const TickReplayParams& params,
    double price,
    double qty,
    std::int64_t ts,
    std::int64_t activate_latency_ms,
    std::int64_t ack_latency_ms,
    double mid,
    double queue_base,
    double queue_decay,
    double inventory_at_quote,
    std::size_t decision_trade_idx,
    double side_markout_ema,
    const SideQuoteContext& ctx,
    bool final_compressed,
    TraceOrderPtr trace
) {
    ReplayOrder order;
    order.side = S;
    order.price = price;
    order.price_tick = price_to_tick(price, params.quote.tick_size);
    order.quantity = qty;
    order.remaining = qty;
    order.quote_ts = ts;
    order.activate_ts = ts + std::max<std::int64_t>(0, activate_latency_ms);
    order.new_ack_ts = ts + std::max<std::int64_t>(
        std::max<std::int64_t>(0, activate_latency_ms),
        ack_latency_ms
    );
    order.cancel_effective_ts = 0;
    order.cancel_ack_ts = 0;
    order.exchange_accepted =
        activate_latency_ms <= 0 && !params.native_exchange_book_enabled;
    order.state = (ack_latency_ms > 0 || params.native_exchange_book_enabled)
        ? OrderState::PendingNew
        : OrderState::Open;
    order.mid_at_quote = mid;
    const auto [queue_mult, exposure_increasing] =
        queue_inventory_multiplier<S>(params, inventory_at_quote);
    (void)exposure_increasing;
    double activation_mid = mid;
    const auto activation_bbo_pos =
        index_at_or_before(input.bbo_ts_ms, order.activate_ts);
    const auto activation_l2_pos =
        index_at_or_before(input.l2_ts_ms, order.activate_ts);
    if (activation_bbo_pos >= 0 || activation_l2_pos >= 0) {
        const std::size_t activation_bbo_idx = activation_bbo_pos >= 0
            ? static_cast<std::size_t>(activation_bbo_pos)
            : input.bbo_ts_ms.size();
        const std::size_t activation_l2_idx = activation_l2_pos >= 0
            ? static_cast<std::size_t>(activation_l2_pos)
            : input.l2_ts_ms.size();
        const auto activation_book = book_snapshot_at(
            input,
            order.activate_ts,
            activation_bbo_idx,
            activation_l2_idx,
            mid - params.quote.tick_size * 0.5,
            mid + params.quote.tick_size * 0.5,
            params.quote.tick_size,
            params
        );
        if (activation_book.mid > 0.0) {
            activation_mid = activation_book.mid;
        }
    }
    // Python seeds a newly active order from the exchange-time book at ACK,
    // not from the decision-time mid. A book move during new-order latency can
    // cross a distance calibration edge or materially change fallback queue.
    const double dist_from_mid =
        side_distance_to_mid<S>(activation_mid, price, 0.0);
    const double activation_local_rank = causal_local_rank_cpp(
        input,
        decision_trade_idx,
        order.activate_ts,
        mid,
        120'000,
        params.quote.tick_size
    );
    const double regime_mult = queue_regime_multiplier_cpp<S>(
        params,
        dist_from_mid,
        activation_local_rank,
        ctx,
        ctx.near_depth_total
    );
    const double mo_mult = queue_mo_multiplier_cpp<S>(params, side_markout_ema);
    order.queue_left = estimate_queue_ahead<S>(
        input,
        params,
        price,
        dist_from_mid,
        order.activate_ts,
        queue_base,
        queue_decay,
        &order.queue_seed_source
    ) * queue_mult * regime_mult * mo_mult;
    order.queue_init = order.queue_left;
    if (params.queue_l2_cancel_ahead_enabled) {
        const auto l2_seen = index_at_or_before(input.l2_ts_ms, order.activate_ts);
        order.queue_l2_seen_idx = l2_seen;
        order.queue_visible_prev = l2_seen >= 0
            ? l2_visible_queue_ahead<S>(
                input,
                price,
                order.activate_ts,
                params.quote.tick_size
            )
            : -1.0;
        order.queue_trade_since_l2 = 0.0;
    }
    order.side_adverse = ctx.side_adverse;
    order.defense_guard = ctx.defense_guard;
    order.local_extreme_guard = ctx.local_extreme_guard;
    order.final_compressed = final_compressed;
    order.ttl_ms = static_cast<std::int64_t>(std::llround(std::max(0.0, ctx.order_ttl_ms)));
    if (trace) {
        trace->queue_init = order.queue_init;
        trace->queue_left = order.queue_left;
        trace->remaining = order.remaining;
    }
    order.trace = std::move(trace);
    return order;
}

template <Side S>
void apply_l2_cancel_ahead_to_order(
    ReplayOrder& order,
    const TickReplayInput& input,
    const TickReplayParams& params,
    std::size_t current_l2_idx,
    TickReplaySummary& summary
) {
    if (!params.queue_l2_cancel_ahead_enabled ||
        current_l2_idx >= input.l2_ts_ms.size() ||
        (order.state == OrderState::PendingNew && !order.exchange_accepted)) {
        return;
    }
    if (order.queue_l2_seen_idx < 0) {
        order.queue_l2_seen_idx = static_cast<std::int64_t>(current_l2_idx);
        order.queue_visible_prev = l2_visible_queue_ahead<S>(
            input,
            order.price,
            input.l2_ts_ms.data()[current_l2_idx],
            params.quote.tick_size
        );
        order.queue_trade_since_l2 = 0.0;
        return;
    }
    if (static_cast<std::size_t>(order.queue_l2_seen_idx) >= current_l2_idx) {
        return;
    }
    double trade_since = std::max(0.0, order.queue_trade_since_l2);
    for (std::size_t idx = static_cast<std::size_t>(order.queue_l2_seen_idx + 1);
         idx <= current_l2_idx;
         ++idx) {
        const double current_visible = l2_visible_queue_ahead<S>(
            input,
            order.price,
            input.l2_ts_ms.data()[idx],
            params.quote.tick_size
        );
        if (current_visible < 0.0) {
            order.queue_visible_prev = -1.0;
            trade_since = 0.0;
            continue;
        }
        if (order.queue_visible_prev >= 0.0) {
            const double explained_trade = std::min(order.queue_visible_prev, trade_since);
            const double cancellation = std::max(
                0.0,
                order.queue_visible_prev - current_visible - explained_trade
            );
            const double ahead = std::max(0.0, order.queue_left);
            const double public_after_trade = std::max(
                0.0,
                order.queue_visible_prev - explained_trade
            );
            const double behind = std::max(0.0, public_after_trade - ahead);
            const double denominator = ahead + behind;
            const double ahead_probability = denominator > 0.0 ? ahead / denominator : 0.0;
            const double removed = std::min(ahead, cancellation * ahead_probability);
            if (removed > 0.0) {
                order.queue_left = subtract_lot_quantity(
                    ahead, removed, params.quote.lot_size
                );
                ++summary.queue_l2_cancel_ahead_event_count;
                summary.queue_l2_cancel_ahead_qty += removed;
                if constexpr (is_buy_v<S>) {
                    ++summary.queue_l2_cancel_ahead_bid_event_count;
                } else {
                    ++summary.queue_l2_cancel_ahead_ask_event_count;
                }
            }
        }
        order.queue_visible_prev = current_visible;
        trade_since = 0.0;
    }
    order.queue_trade_since_l2 = trade_since;
    order.queue_l2_seen_idx = static_cast<std::int64_t>(current_l2_idx);
}

template <Side S>
void apply_l2_cancel_ahead(
    ReplayOrders& orders,
    const TickReplayInput& input,
    const TickReplayParams& params,
    std::size_t current_l2_idx,
    TickReplaySummary& summary
) {
    for (auto& order : orders) {
        apply_l2_cancel_ahead_to_order<S>(
            order,
            input,
            params,
            current_l2_idx,
            summary
        );
    }
}

struct PairedProbeOrderState {
    ReplayOrder order;
    std::size_t row_index = 0;
    bool exact_touched = false;
    bool through_touched = false;
    bool filled = false;
    bool fully_filled = false;
    bool first_fill_via_through = false;
    bool through_forced_fill = false;
    std::int64_t first_fill_ts = 0;
    double fill_qty = 0.0;
};

struct PairedProbeCohort {
    std::int64_t cohort_id = -1;
    Side side = Side::Buy;
    std::int64_t submit_ts = 0;
    std::int64_t activate_ts = 0;
    std::int64_t cancel_request_ts = 0;
    std::int64_t cancel_effective_ts = 0;
    std::int64_t ttl_expiry_ts = 0;
    bool activation_resolved = false;
    bool activation_rejected = false;
    bool monotonicity_violation_recorded = false;
    std::vector<PairedProbeOrderState> orders;
};

class PairedFixedSpreadProbe {
public:
    PairedFixedSpreadProbe(
        const TickReplayInput& input,
        const TickReplayParams& params,
        TickReplayResult& result,
        double tick_size,
        double lot_size,
        double order_size
    )
        : input_(input),
          params_(params),
          result_(result),
          tick_size_(tick_size),
          lot_size_(lot_size),
          order_size_(order_size),
          distances_(params.paired_fixed_spread_probe_ticks) {
        if (!params_.paired_fixed_spread_probe_enabled) {
            return;
        }
        if (distances_.empty()) {
            throw std::invalid_argument(
                "paired_fixed_spread_probe_ticks must not be empty"
            );
        }
        for (std::size_t idx = 0; idx < distances_.size(); ++idx) {
            const double distance = distances_[idx];
            if (!std::isfinite(distance) || distance < 0.0) {
                throw std::invalid_argument(
                    "paired fixed-spread distances must be finite and non-negative"
                );
            }
            if (idx > 0 && !(distance > distances_[idx - 1])) {
                throw std::invalid_argument(
                    "paired fixed-spread distances must be strictly increasing"
                );
            }
        }
        result_.paired_fixed_spread_rows.reserve(distances_.size() * 2);
        for (const Side side : {Side::Buy, Side::Sell}) {
            for (const double distance : distances_) {
                PairedFixedSpreadProbeRow row;
                row.side = side;
                row.distance_ticks = distance;
                result_.paired_fixed_spread_rows.push_back(row);
            }
        }
    }

    [[nodiscard]] bool enabled() const noexcept {
        return params_.paired_fixed_spread_probe_enabled;
    }

    void on_event(
        std::int64_t ts,
        double best_bid,
        double best_ask,
        std::size_t current_l2_idx,
        bool l2_advanced
    ) {
        if (!enabled()) {
            return;
        }
        for (auto& cohort : cohorts_) {
            if (cohort.cancel_effective_ts > 0 &&
                cohort.cancel_effective_ts <= cohort.activate_ts &&
                ts >= cohort.cancel_effective_ts &&
                !cohort.activation_resolved) {
                for (auto& probe : cohort.orders) {
                    ++row(probe).cancelled_before_activation;
                }
                cohort.activation_rejected = true;
                continue;
            }
            if (!cohort.activation_resolved && ts >= cohort.activate_ts) {
                resolve_activation(cohort, best_bid, best_ask);
            }
            if (!cohort.activation_resolved || cohort.activation_rejected) {
                continue;
            }
            maybe_request_ttl_cancel(cohort, ts);
            if (cohort.cancel_effective_ts > 0 &&
                ts >= cohort.cancel_effective_ts) {
                account_closed_cohort(cohort);
                cohort.activation_rejected = true;
                continue;
            }
            if (l2_advanced) {
                apply_l2_update(cohort, current_l2_idx);
            }
        }
        std::erase_if(
            cohorts_,
            [](const PairedProbeCohort& cohort) {
                return cohort.activation_rejected;
            }
        );
    }

    template <Side S>
    void on_trade(
        std::int64_t ts,
        double trade_price,
        double trade_qty,
        double queue_deplete_mult
    ) {
        if (!enabled()) {
            return;
        }
        const auto trade_price_tick = price_to_tick(trade_price, tick_size_);
        for (auto& cohort : cohorts_) {
            if (cohort.side != S ||
                !cohort.activation_resolved ||
                cohort.activation_rejected) {
                continue;
            }
            for (auto& probe : cohort.orders) {
                if (probe.fully_filled) {
                    continue;
                }
                const bool exact =
                    trade_price_tick == probe.order.price_tick;
                const bool through = is_buy_v<S>
                    ? trade_price_tick < probe.order.price_tick
                    : trade_price_tick > probe.order.price_tick;
                if (!exact && !through) {
                    continue;
                }
                probe.exact_touched = probe.exact_touched || exact;
                probe.through_touched = probe.through_touched || through;
                if (exact && params_.queue_l2_cancel_ahead_enabled) {
                    probe.order.queue_trade_since_l2 += trade_qty;
                }
                double fill_qty = 0.0;
                if (through) {
                    // A print strictly through the passive limit proves that
                    // every better price level was exhausted. Queue-ahead is
                    // therefore no longer a probabilistic quantity.
                    fill_qty = probe.order.remaining;
                    probe.order.queue_left = 0.0;
                    probe.through_forced_fill = true;
                } else {
                    double available =
                        trade_qty * nonnegative_or_one(queue_deplete_mult);
                    if (probe.order.queue_left > 0.0) {
                        const double eaten =
                            std::min(probe.order.queue_left, available);
                        probe.order.queue_left = subtract_lot_quantity(
                            probe.order.queue_left, eaten, lot_size_
                        );
                        available = subtract_lot_quantity(available, eaten, lot_size_);
                    }
                    fill_qty = std::min(probe.order.remaining, available);
                }
                fill_qty = floor_lot(fill_qty, lot_size_);
                if (fill_qty < lot_size_) {
                    continue;
                }
                const bool first_fill = !probe.filled;
                probe.order.remaining = subtract_lot_quantity(
                    probe.order.remaining, fill_qty, lot_size_
                );
                probe.fill_qty += fill_qty;
                probe.filled = true;
                if (first_fill) {
                    probe.first_fill_ts = ts;
                    probe.first_fill_via_through = through;
                }
                if (probe.order.remaining < lot_size_) {
                    probe.order.remaining = 0.0;
                    probe.fully_filled = true;
                }
            }
            check_monotonicity(cohort, ts);
        }
    }

    void on_decision_cancel(std::int64_t ts) {
        if (!enabled()) {
            return;
        }
        for (const Side side : {Side::Buy, Side::Sell}) {
            const auto deadlines = sample_cancel_deadlines(
                ts,
                params_.cancel_order_latency_ms,
                params_.latency_jitter_ms,
                &params_.cancel_order_latency_samples_ms,
                params_,
                side,
                LatencyOperation::Cancel,
                ts,
                true
            );
            for (auto& cohort : cohorts_) {
                if (cohort.side == side && cohort.cancel_effective_ts == 0) {
                    cohort.cancel_request_ts = ts;
                    cohort.cancel_effective_ts = deadlines.effective_ts;
                    for (auto& probe : cohort.orders) {
                        if (probe.order.state != OrderState::PendingNew) {
                            probe.order.state = OrderState::PendingCancel;
                        }
                    }
                }
            }
        }
    }

    template <Side S>
    void create_cohort(
        std::int64_t ts,
        std::size_t decision_trade_idx,
        double best_bid,
        double best_ask,
        double mid,
        double queue_base,
        double queue_decay,
        const SideQuoteContext& ctx
    ) {
        if (!enabled()) {
            return;
        }
        const auto deadlines = sample_new_order_deadlines(
            ts,
            params_.new_order_latency_ms,
            params_.latency_jitter_ms,
            &params_.new_order_latency_samples_ms,
            params_,
            S,
            LatencyOperation::NewOrder,
            ts
        );
        const std::int64_t latency_ms = deadlines.effective_ts - ts;
        const std::int64_t ack_latency_ms = deadlines.ack_ts - ts;
        PairedProbeCohort cohort;
        cohort.cohort_id = next_cohort_id_++;
        cohort.side = S;
        cohort.submit_ts = ts;
        cohort.activate_ts = ts + latency_ms;
        cohort.orders.reserve(distances_.size());
        for (std::size_t idx = 0; idx < distances_.size(); ++idx) {
            const double distance_price = distances_[idx] * tick_size_;
            const double price = is_buy_v<S>
                ? floor_tick(best_bid - distance_price, tick_size_)
                : ceil_tick(best_ask + distance_price, tick_size_);
            TraceOrderPtr trace{nullptr, PmrTraceDeleter{}};
            ReplayOrder order = make_order<S>(
                input_,
                params_,
                price,
                order_size_,
                ts,
                latency_ms,
                ack_latency_ms,
                mid,
                queue_base,
                queue_decay,
                0.0,
                decision_trade_idx,
                0.0,
                ctx,
                false,
                std::move(trace)
            );
            PairedProbeOrderState probe;
            probe.order = std::move(order);
            probe.row_index = row_index(S, idx);
            ++result_.paired_fixed_spread_rows[probe.row_index].submitted_orders;
            cohort.orders.push_back(std::move(probe));
        }
        cohorts_.push_back(std::move(cohort));
        if (latency_ms == 0) {
            resolve_activation(cohorts_.back(), best_bid, best_ask);
        }
    }

    void finish(std::int64_t end_ts) {
        if (!enabled()) {
            return;
        }
        for (auto& cohort : cohorts_) {
            if (!cohort.activation_resolved || cohort.activation_rejected) {
                continue;
            }
            const std::int64_t age_ms =
                std::max<std::int64_t>(0, end_ts - cohort.activate_ts);
            for (auto& probe : cohort.orders) {
                auto& out = row(probe);
                // Administrative day-end censoring is cohort-wide. Even if a
                // shallow member has filled, the deeper counterfactual has not
                // completed the shared lifecycle window, so the whole cohort
                // is excluded from lifecycle estimates. Fixed horizons enter
                // only when the common observation window reached the horizon.
                ++out.end_censored_orders;
                account_horizon(probe, out, 1'000, age_ms);
                account_horizon(probe, out, 5'000, age_ms);
                account_horizon(probe, out, 10'000, age_ms);
                out.end_censored_before_1s += age_ms < 1'000 ? 1 : 0;
                out.end_censored_before_5s += age_ms < 5'000 ? 1 : 0;
                out.end_censored_before_10s += age_ms < 10'000 ? 1 : 0;
            }
        }
        validate_aggregate_monotonicity(end_ts);
    }

private:
    [[nodiscard]] std::size_t row_index(Side side, std::size_t distance_idx) const {
        return (side == Side::Buy ? 0 : distances_.size()) + distance_idx;
    }

    PairedFixedSpreadProbeRow& row(PairedProbeOrderState& probe) {
        return result_.paired_fixed_spread_rows[probe.row_index];
    }

    void resolve_activation(
        PairedProbeCohort& cohort,
        double best_bid,
        double best_ask
    ) {
        cohort.activation_resolved = true;
        const auto bbo_pos =
            index_at_or_before(input_.bbo_ts_ms, cohort.activate_ts);
        const auto l2_pos =
            index_at_or_before(input_.l2_ts_ms, cohort.activate_ts);
        const auto activation_book = book_snapshot_at(
            input_,
            cohort.activate_ts,
            bbo_pos >= 0
                ? static_cast<std::size_t>(bbo_pos)
                : input_.bbo_ts_ms.size(),
            l2_pos >= 0
                ? static_cast<std::size_t>(l2_pos)
                : input_.l2_ts_ms.size(),
            best_bid,
            best_ask,
            tick_size_,
            params_,
            cohort.activate_ts
        );
        best_bid = activation_book.best_bid;
        best_ask = activation_book.best_ask;
        const bool invalid_book =
            best_bid <= 0.0 || best_ask <= best_bid;
        const bool would_cross = std::any_of(
            cohort.orders.begin(),
            cohort.orders.end(),
            [&](const PairedProbeOrderState& probe) {
                return cohort.side == Side::Buy
                    ? probe.order.price_tick >=
                        price_to_tick(best_ask, tick_size_)
                    : probe.order.price_tick <=
                        price_to_tick(best_bid, tick_size_);
            }
        );
        if (invalid_book || would_cross) {
            for (auto& probe : cohort.orders) {
                ++row(probe).activation_gtx_rejects;
            }
            cohort.activation_rejected = true;
            return;
        }
        const std::int64_t ttl_ms = cohort.orders.empty()
            ? 0
            : cohort.orders.front().order.ttl_ms;
        for (auto& probe : cohort.orders) {
            if (probe.order.ttl_ms != ttl_ms) {
                throw std::runtime_error(
                    "paired fixed-spread cohort has distance-dependent TTL"
                );
            }
            probe.order.quote_ts = cohort.activate_ts;
            if (activation_book.mid > 0.0) {
                probe.order.mid_at_quote = activation_book.mid;
            }
            probe.order.state = cohort.cancel_effective_ts > 0
                ? OrderState::PendingCancel
                : OrderState::Open;
            auto& out = row(probe);
            ++out.placed_orders;
            out.queue_visible_positive_orders +=
                probe.order.queue_seed_source == 1 ? 1 : 0;
            out.queue_known_zero_orders +=
                probe.order.queue_seed_source == 2 ? 1 : 0;
            out.queue_fallback_orders +=
                probe.order.queue_seed_source == 3 ? 1 : 0;
        }
        cohort.ttl_expiry_ts = ttl_ms > 0
            ? cohort.activate_ts + ttl_ms
            : 0;
    }

    void maybe_request_ttl_cancel(
        PairedProbeCohort& cohort,
        std::int64_t ts
    ) {
        if (cohort.ttl_expiry_ts <= 0 ||
            ts < cohort.ttl_expiry_ts ||
            cohort.cancel_effective_ts > 0) {
            return;
        }
        const std::int64_t latency_ms = sample_latency_ms(
            params_.cancel_order_latency_ms,
            params_.latency_jitter_ms,
            &params_.cancel_order_latency_samples_ms,
            params_,
            ts,
            cohort.side,
            LatencyOperation::FragileTtlCancel,
            cohort.activate_ts
        );
        cohort.cancel_request_ts = ts;
        cohort.cancel_effective_ts = ts + latency_ms;
        for (auto& probe : cohort.orders) {
            probe.order.state = OrderState::PendingCancel;
        }
        ++result_.summary.fragile_ttl_cancel_count;
    }

    void apply_l2_update(PairedProbeCohort& cohort, std::size_t current_l2_idx) {
        if (cohort.side == Side::Buy) {
            for (auto& probe : cohort.orders) {
                apply_l2_cancel_ahead_to_order<Side::Buy>(
                    probe.order,
                    input_,
                    params_,
                    current_l2_idx,
                    result_.summary
                );
            }
        } else {
            for (auto& probe : cohort.orders) {
                apply_l2_cancel_ahead_to_order<Side::Sell>(
                    probe.order,
                    input_,
                    params_,
                    current_l2_idx,
                    result_.summary
                );
            }
        }
    }

    static void account_horizon(
        const PairedProbeOrderState& probe,
        PairedFixedSpreadProbeRow& out,
        std::int64_t horizon_ms,
        std::int64_t observed_age_ms
    ) {
        if (observed_age_ms < horizon_ms) {
            return;
        }
        std::int64_t* observed = nullptr;
        std::int64_t* filled = nullptr;
        if (horizon_ms == 1'000) {
            observed = &out.observed_1s_orders;
            filled = &out.filled_within_1s;
        } else if (horizon_ms == 5'000) {
            observed = &out.observed_5s_orders;
            filled = &out.filled_within_5s;
        } else {
            observed = &out.observed_10s_orders;
            filled = &out.filled_within_10s;
        }
        ++(*observed);
        if (probe.filled &&
            probe.first_fill_ts - probe.order.activate_ts <= horizon_ms) {
            ++(*filled);
        }
    }

    void account_closed_cohort(PairedProbeCohort& cohort) {
        for (auto& probe : cohort.orders) {
            auto& out = row(probe);
            account_terminal_probe(
                cohort,
                probe,
                out
            );
        }
    }

    static void account_terminal_probe(
        const PairedProbeCohort& cohort,
        const PairedProbeOrderState& probe,
        PairedFixedSpreadProbeRow& out
    ) {
        ++out.observed_lifecycle_orders;
        // Cancel ACK and TTL ACK are terminal competing events, not missing
        // observations. Once the shared lifecycle has ended, every fixed
        // horizon has a known fill/non-fill outcome.
        const std::int64_t horizon_age =
            std::numeric_limits<std::int64_t>::max();
        account_horizon(probe, out, 1'000, horizon_age);
        account_horizon(probe, out, 5'000, horizon_age);
        account_horizon(probe, out, 10'000, horizon_age);
        out.exact_touched_orders += probe.exact_touched ? 1 : 0;
        out.through_touched_orders += probe.through_touched ? 1 : 0;
        out.any_touched_orders +=
            (probe.exact_touched || probe.through_touched) ? 1 : 0;
        if (!probe.filled) {
            ++out.cancelled_unfilled_orders;
            return;
        }
        ++out.filled_orders;
        out.fully_filled_orders += probe.fully_filled ? 1 : 0;
        out.filled_via_through_orders +=
            probe.first_fill_via_through ? 1 : 0;
        out.filled_via_exact_orders +=
            probe.first_fill_via_through ? 0 : 1;
        out.through_forced_fill_orders +=
            probe.through_forced_fill ? 1 : 0;
        out.first_fill_pending_cancel_orders +=
            cohort.cancel_request_ts > 0 &&
            probe.first_fill_ts >= cohort.cancel_request_ts &&
            probe.first_fill_ts < cohort.cancel_effective_ts
                ? 1
                : 0;
        out.fill_qty += probe.fill_qty;
    }

    void check_monotonicity(PairedProbeCohort& cohort, std::int64_t ts) {
        if (cohort.monotonicity_violation_recorded) {
            return;
        }
        for (std::size_t idx = 1; idx < cohort.orders.size(); ++idx) {
            const auto& shallow = cohort.orders[idx - 1];
            const auto& deep = cohort.orders[idx];
            const bool any_fill_violation =
                deep.filled && !shallow.filled;
            const bool full_fill_violation =
                deep.fully_filled && !shallow.fully_filled;
            const bool quantity_violation =
                deep.fill_qty > shallow.fill_qty +
                    std::max(1e-12, order_size_ * 1e-9);
            if (any_fill_violation ||
                full_fill_violation ||
                quantity_violation) {
                record_violation(cohort, idx - 1, idx, ts);
                return;
            }
        }
    }

    void validate_aggregate_monotonicity(std::int64_t ts) {
        for (const Side side : {Side::Buy, Side::Sell}) {
            for (std::size_t idx = 1; idx < distances_.size(); ++idx) {
                const auto& shallow =
                    result_.paired_fixed_spread_rows[row_index(side, idx - 1)];
                const auto& deep =
                    result_.paired_fixed_spread_rows[row_index(side, idx)];
                const bool same_denominators =
                    shallow.submitted_orders == deep.submitted_orders &&
                    shallow.placed_orders == deep.placed_orders &&
                    shallow.observed_lifecycle_orders ==
                        deep.observed_lifecycle_orders &&
                    shallow.observed_1s_orders == deep.observed_1s_orders &&
                    shallow.observed_5s_orders == deep.observed_5s_orders &&
                    shallow.observed_10s_orders == deep.observed_10s_orders;
                const bool monotone =
                    shallow.filled_orders >= deep.filled_orders &&
                    shallow.fully_filled_orders >= deep.fully_filled_orders &&
                    shallow.filled_within_1s >= deep.filled_within_1s &&
                    shallow.filled_within_5s >= deep.filled_within_5s &&
                    shallow.filled_within_10s >= deep.filled_within_10s &&
                    shallow.fill_qty +
                        std::max(1e-12, order_size_ * 1e-9) >=
                        deep.fill_qty;
                if (same_denominators && monotone) {
                    continue;
                }
                PairedProbeCohort aggregate;
                aggregate.cohort_id = -1;
                aggregate.side = side;
                aggregate.orders.resize(2);
                aggregate.orders[0].row_index = row_index(side, idx - 1);
                aggregate.orders[1].row_index = row_index(side, idx);
                record_violation(aggregate, 0, 1, ts);
            }
        }
    }

    void record_violation(
        PairedProbeCohort& cohort,
        std::size_t shallow_idx,
        std::size_t deep_idx,
        std::int64_t ts
    ) {
        cohort.monotonicity_violation_recorded = true;
        const auto& shallow =
            result_.paired_fixed_spread_rows[cohort.orders[shallow_idx].row_index];
        const auto& deep =
            result_.paired_fixed_spread_rows[cohort.orders[deep_idx].row_index];
        if (result_.paired_fixed_spread_violations.size() <
            static_cast<std::size_t>(std::max<std::int64_t>(
                0,
                params_.paired_fixed_spread_max_recorded_violations
            ))) {
            PairedFixedSpreadViolationRow violation;
            violation.cohort_id = cohort.cohort_id;
            violation.side = cohort.side;
            violation.event_ts_ms = ts;
            violation.shallower_distance_ticks = shallow.distance_ticks;
            violation.deeper_distance_ticks = deep.distance_ticks;
            result_.paired_fixed_spread_violations.push_back(violation);
        }
        if (params_.paired_fixed_spread_fail_on_violation) {
            throw std::runtime_error(
                "paired fixed-spread monotonicity violation: cohort=" +
                std::to_string(cohort.cohort_id) +
                " side=" + std::string(side_name(cohort.side)) +
                " shallow_ticks=" + std::to_string(shallow.distance_ticks) +
                " deep_ticks=" + std::to_string(deep.distance_ticks) +
                " ts_ms=" + std::to_string(ts)
            );
        }
    }

    const TickReplayInput& input_;
    const TickReplayParams& params_;
    TickReplayResult& result_;
    double tick_size_;
    double lot_size_;
    double order_size_;
    std::vector<double> distances_;
    std::vector<PairedProbeCohort> cohorts_;
    std::int64_t next_cohort_id_ = 1;
};

bool activate_fixed_spread_probe_order(
    ReplayOrder& order,
    double best_bid,
    double best_ask,
    double tick_size,
    TickReplaySummary& summary
) {
    if (!order.fixed_spread_probe) {
        return true;
    }
    const bool would_cross = order.side == Side::Buy
        ? (
            best_ask > 0.0 &&
            order.price_tick >= price_to_tick(best_ask, tick_size)
        )
        : (
            best_bid > 0.0 &&
            order.price_tick <= price_to_tick(best_bid, tick_size)
        );
    if (would_cross) {
        if (order.side == Side::Buy) {
            ++summary.fixed_spread_probe_bid_activation_gtx_rejects;
        } else {
            ++summary.fixed_spread_probe_ask_activation_gtx_rejects;
        }
        return false;
    }
    if (order.side == Side::Buy) {
        ++summary.fixed_spread_probe_bid_placed_orders;
        summary.fixed_spread_probe_bid_queue_visible_positive_orders +=
            order.queue_seed_source == 1;
        summary.fixed_spread_probe_bid_queue_known_zero_orders +=
            order.queue_seed_source == 2;
        summary.fixed_spread_probe_bid_queue_fallback_orders +=
            order.queue_seed_source == 3;
    } else {
        ++summary.fixed_spread_probe_ask_placed_orders;
        summary.fixed_spread_probe_ask_queue_visible_positive_orders +=
            order.queue_seed_source == 1;
        summary.fixed_spread_probe_ask_queue_known_zero_orders +=
            order.queue_seed_source == 2;
        summary.fixed_spread_probe_ask_queue_fallback_orders +=
            order.queue_seed_source == 3;
    }
    order.fixed_spread_probe_activated = true;
    return true;
}

bool activate_resting_order(
    ReplayOrder& order,
    double best_bid,
    double best_ask,
    double tick_size,
    bool historical_book_available,
    TickReplaySummary& summary,
    std::int64_t& circuit_breaker_close_gtx_reject_streak
) {
    if (order.immediate_or_cancel) {
        return true;
    }
    if (order.fixed_spread_probe) {
        return activate_fixed_spread_probe_order(
            order,
            best_bid,
            best_ask,
            tick_size,
            summary
        );
    }
    if (!historical_book_available) {
        return true;
    }
    const bool would_cross = order.side == Side::Buy
        ? (
            best_ask > 0.0 &&
            order.price_tick >= price_to_tick(best_ask, tick_size)
        )
        : (
            best_bid > 0.0 &&
            order.price_tick <= price_to_tick(best_bid, tick_size)
        );
    if (would_cross) {
        if (order.circuit_breaker_close) {
            ++summary.circuit_breaker_close_gtx_reject_count;
            ++circuit_breaker_close_gtx_reject_streak;
        }
        return false;
    }
    if (order.circuit_breaker_close) {
        circuit_breaker_close_gtx_reject_streak = 0;
    }
    return true;
}

void transition_orders(
    ReplayOrders& orders,
    const TickReplayInput& input,
    std::int64_t ts,
    double fallback_bid,
    double fallback_ask,
    double tick_size,
    std::int64_t cancel_latency_ms,
    std::int64_t latency_jitter_ms,
    const std::vector<double>* cancel_latency_samples_ms,
    const TickReplayParams& params,
    TickReplaySummary& summary,
    TickReplayResult& result,
    std::int64_t trace_quotes_max,
    std::int64_t& circuit_breaker_close_gtx_reject_streak,
    NativeBookTapeRuntime* native_book = nullptr,
    bool allow_ttl_initiation = true,
    bool defer_cancel_ack_at_ts = false
) {
    for (auto& order : orders) {
        const bool canceled_before_activation =
            !order.exchange_accepted &&
            order.cancel_effective_ts > 0 &&
            order.cancel_effective_ts <= order.activate_ts &&
            ts >= order.cancel_effective_ts;
        if (!canceled_before_activation &&
            !order.exchange_accepted &&
            ts >= order.activate_ts) {
            double activation_best_bid = fallback_bid;
            double activation_best_ask = fallback_ask;
            const auto bbo_pos =
                index_at_or_before(input.bbo_ts_ms, order.activate_ts);
            const auto l2_pos =
                index_at_or_before(input.l2_ts_ms, order.activate_ts);
            const auto activation_book = book_snapshot_at(
                input,
                order.activate_ts,
                bbo_pos >= 0
                    ? static_cast<std::size_t>(bbo_pos)
                    : input.bbo_ts_ms.size(),
                l2_pos >= 0
                    ? static_cast<std::size_t>(l2_pos)
                    : input.l2_ts_ms.size(),
                fallback_bid,
                fallback_ask,
                tick_size,
                params,
                order.activate_ts
            );
            activation_best_bid = activation_book.best_bid;
            activation_best_ask = activation_book.best_ask;
            if (activate_resting_order(
                    order,
                    activation_best_bid,
                    activation_best_ask,
                    tick_size,
                    !input.bbo_ts_ms.empty() || !input.l2_ts_ms.empty(),
                    summary,
                    circuit_breaker_close_gtx_reject_streak)) {
                order.exchange_accepted = true;
                if (!order.immediate_or_cancel) {
                    if (native_book != nullptr) {
                        seed_native_order_queue(order, *native_book, summary);
                    }
                    order.quote_ts = order.activate_ts;
                    if (activation_book.mid > 0.0) {
                        order.mid_at_quote = activation_book.mid;
                    }
                    if (order.trace) {
                        order.trace->quote_ts = order.quote_ts;
                    }
                }
                if (order.new_ack_ts <= ts) {
                    order.state = order.cancel_ack_ts > ts
                        ? OrderState::PendingCancel
                        : OrderState::Open;
                }
            } else {
                order.activation_rejected = true;
                continue;
            }
        }
        const bool split_new_ack_pending =
            !params.new_order_exchange_effective_latency_samples_ms.empty() &&
            order.state == OrderState::PendingNew;
        if (allow_ttl_initiation &&
            order.ttl_ms > 0 && !split_new_ack_pending &&
            order.state != OrderState::PendingCancel &&
            ts - order.quote_ts >= order.ttl_ms) {
            order.state = OrderState::PendingCancel;
            const auto deadlines = sample_cancel_deadlines(
                ts,
                cancel_latency_ms,
                latency_jitter_ms,
                cancel_latency_samples_ms,
                params,
                order.side,
                LatencyOperation::FragileTtlCancel,
                order.quote_ts
            );
            order.cancel_effective_ts = deadlines.effective_ts;
            order.cancel_ack_ts = deadlines.ack_ts;
            order.pending_cancel_reason = CancelReason::FragileTtl;
            ++summary.fragile_ttl_cancel_count;
        }
        if (order.exchange_accepted &&
            order.state == OrderState::PendingNew &&
            ts >= order.new_ack_ts) {
            order.state = order.cancel_ack_ts > ts
                ? OrderState::PendingCancel
                : OrderState::Open;
        }
    }
    for (auto it = orders.begin(); it != orders.end();) {
        if (it->activation_rejected) {
            it = orders.erase(it);
            continue;
        }
        const bool cancel_due =
            it->cancel_ack_ts > 0 &&
            ts >= it->cancel_ack_ts &&
            !(defer_cancel_ack_at_ts && it->cancel_ack_ts == ts);
        const bool cancel_acked = cancel_due && (
            it->state == OrderState::PendingCancel ||
            it->state == OrderState::Open ||
            (
                it->state == OrderState::PendingNew &&
                (
                    it->cancel_effective_ts <= it->activate_ts ||
                    it->exchange_accepted
                )
            )
        );
        if (!cancel_acked) {
            ++it;
            continue;
        }
        if (trace_quotes_max > 0 &&
            result.quote_trace.size() < static_cast<std::size_t>(trace_quotes_max)) {
            append_order_trace(
                result,
                *it,
                // The transition is consumed before this event's policy work;
                // record the known ACK boundary, not the later polling event.
                (!params.cancel_exchange_effective_latency_samples_ms.empty() ||
                 !params.cancel_ack_visibility_latency_samples_ms.empty() ||
                 decision_to_gateway_latency_enabled(params))
                    ? it->cancel_ack_ts : ts,
                TraceOutcome::Cancel,
                it->pending_cancel_reason,
                0.0
            );
        }
        it = orders.erase(it);
    }
}

void request_cancel_all(
    ReplayOrders& orders,
    std::int64_t ts,
    std::int64_t cancel_latency_ms,
    std::int64_t latency_jitter_ms,
    const std::vector<double>* cancel_latency_samples_ms,
    const TickReplayParams& params,
    TickReplayResult* result = nullptr,
    std::int64_t trace_quotes_max = 0,
    CancelReason reason = CancelReason::Requote
) {
    const bool apply_decision_compute =
        reason == CancelReason::Requote ||
        reason == CancelReason::RequoteReplace ||
        reason == CancelReason::SideDisabled;
    if ((cancel_latency_samples_ms == nullptr || cancel_latency_samples_ms->empty()) &&
        params.cancel_exchange_effective_latency_samples_ms.empty() &&
        params.cancel_ack_visibility_latency_samples_ms.empty() &&
        !(apply_decision_compute && decision_to_gateway_latency_enabled(params)) &&
        !(apply_decision_compute && std::any_of(orders.begin(), orders.end(),
            [](const auto& order) { return order.state == OrderState::PendingNew; })) &&
        cancel_latency_ms <= 0 && latency_jitter_ms <= 0) {
        if (result != nullptr && trace_quotes_max > 0) {
            for (const auto& order : orders) {
                if (result->quote_trace.size() >= static_cast<std::size_t>(trace_quotes_max)) {
                    break;
                }
                append_order_trace(*result, order, ts, TraceOutcome::Cancel, reason, 0.0);
            }
        }
        orders.clear();
        return;
    }
    for (auto& order : orders) {
        if (apply_decision_compute && order.state == OrderState::PendingNew) {
            continue;  // No replacement cancel before the local submit ACK.
        }
        const auto deadlines = sample_cancel_deadlines(
            ts,
            cancel_latency_ms,
            latency_jitter_ms,
            cancel_latency_samples_ms,
            params,
            order.side,
            LatencyOperation::Cancel,
            order.quote_ts,
            apply_decision_compute
        );
        if (order.state == OrderState::Open) {
            order.state = OrderState::PendingCancel;
            order.cancel_effective_ts = deadlines.effective_ts;
            order.cancel_ack_ts = deadlines.ack_ts;
            order.pending_cancel_reason = reason;
        } else if (order.state == OrderState::PendingNew) {
            order.cancel_effective_ts = deadlines.effective_ts;
            order.cancel_ack_ts = deadlines.ack_ts;
            order.pending_cancel_reason = reason;
        }
    }
    for (auto it = orders.begin(); it != orders.end();) {
        const bool cancel_due =
            it->cancel_ack_ts > 0 &&
            ts >= it->cancel_ack_ts;
        const bool cancel_acked = cancel_due && (
            it->state == OrderState::PendingCancel ||
            (
                it->state == OrderState::PendingNew &&
                (
                    it->cancel_effective_ts <= it->activate_ts ||
                    it->exchange_accepted
                )
            )
        );
        if (!cancel_acked) {
            ++it;
            continue;
        }
        if (result != nullptr && trace_quotes_max > 0 &&
            result->quote_trace.size() < static_cast<std::size_t>(trace_quotes_max)) {
            append_order_trace(
                *result,
                *it,
                (!params.cancel_exchange_effective_latency_samples_ms.empty() ||
                 !params.cancel_ack_visibility_latency_samples_ms.empty() ||
                 decision_to_gateway_latency_enabled(params))
                    ? it->cancel_ack_ts : ts,
                TraceOutcome::Cancel,
                it->pending_cancel_reason,
                0.0
            );
        }
        it = orders.erase(it);
    }
}

template <Side S>
const ReplayOrder* best_live_order(const ReplayOrders& orders, double lot_size) {
    const ReplayOrder* best = nullptr;
    for (const auto& order : orders) {
        if ((order.state != OrderState::Open && order.state != OrderState::PendingCancel) ||
            order.remaining + 1e-12 < lot_size) {
            continue;
        }
        if (best == nullptr) {
            best = &order;
            continue;
        }
        if constexpr (is_buy_v<S>) {
            if (order.price_tick > best->price_tick) {
                best = &order;
            }
        } else {
            if (order.price_tick < best->price_tick) {
                best = &order;
            }
        }
    }
    return best;
}

const ReplayOrder* pending_lifecycle_order(const ReplayOrders& orders, double lot_size) {
    const ReplayOrder* latest = nullptr;
    for (const auto& order : orders) {
        if ((order.state != OrderState::PendingNew && order.state != OrderState::PendingCancel) ||
            order.remaining + 1e-12 < lot_size) {
            continue;
        }
        if (latest == nullptr ||
            std::tie(order.quote_ts, order.activate_ts, order.price_tick) >
                std::tie(latest->quote_ts, latest->activate_ts, latest->price_tick)) {
            latest = &order;
        }
    }
    return latest;
}

std::pair<std::int64_t, std::int64_t> pending_order_counts(const ReplayOrders& bid_orders,
                                                           const ReplayOrders& ask_orders) {
    std::int64_t pending_new = 0;
    std::int64_t pending_cancel = 0;
    auto scan = [&](const ReplayOrders& orders) {
        for (const auto& order : orders) {
            if (order.state == OrderState::PendingNew) {
                ++pending_new;
            } else if (order.state == OrderState::PendingCancel) {
                ++pending_cancel;
            }
        }
    };
    scan(bid_orders);
    scan(ask_orders);
    return {pending_new, pending_cancel};
}

void count_decision_action(TickReplaySummary& summary, std::string_view action) {
    if (action == "place") {
        ++summary.decision_place_count;
    } else if (action == "replace") {
        ++summary.decision_replace_count;
    } else if (action == "keep") {
        ++summary.decision_keep_count;
    } else if (action == "pause") {
        ++summary.decision_pause_count;
    } else if (action == "pending_coalesce") {
        ++summary.decision_pending_coalesce_count;
    } else if (action == "cancel_first") {
        ++summary.decision_cancel_first_count;
    } else {
        ++summary.decision_none_count;
    }
}

double sigmoid_cpp(double x) {
    if (x >= 0.0) {
        const double z = std::exp(-x);
        return 1.0 / (1.0 + z);
    }
    const double z = std::exp(x);
    return z / (1.0 + z);
}

std::string numeric_bucket(double value, const std::vector<double>& cuts) {
    if (!std::isfinite(value)) {
        return "missing";
    }
    std::size_t idx = 0;
    while (idx < cuts.size() && value >= cuts[idx]) {
        ++idx;
    }
    char buf[8];
    std::snprintf(buf, sizeof(buf), "b%02zu", idx);
    return std::string(buf);
}

const double* contribution_for(const TickReplayParams::FillSelectionFoldModel& model,
                               const std::string& feature,
                               const std::string& bucket) {
    const auto fit = model.contributions.find(feature);
    if (fit == model.contributions.end()) {
        return nullptr;
    }
    const auto bit = fit->second.find(bucket);
    if (bit == fit->second.end()) {
        return nullptr;
    }
    return &bit->second;
}

bool buy_fill_dynamic_numeric_feature(const std::string& feature) {
    return feature == "inventory_ratio" ||
        feature == "l2_book_cancel_ratio" ||
        feature == "l2_book_refresh_ratio" ||
        feature == "l2_near_depth_total" ||
        feature == "markout_ema" ||
        feature == "microprice_shift_bps" ||
        feature == "near_depth_total" ||
        feature == "order_exposure_increasing" ||
        feature == "queue_local_rank" ||
        feature == "quote_distance_bps" ||
        feature == "toxicity";
}

bool buy_fill_dynamic_categorical_feature(const std::string& feature) {
    return feature == "side" ||
        feature == "quote_action" ||
        feature == "quote_allow_post" ||
        feature == "quote_allow_exposure_increase" ||
        feature == "order_exposure_increasing" ||
        feature == "fill_eligible";
}

bool buy_fill_model_requires_static_payload(
    const TickReplayParams::FillSelectionFoldModel& model
) {
    for (const auto& [feature, _cuts] : model.numeric_cuts) {
        if (!buy_fill_dynamic_numeric_feature(feature)) {
            return true;
        }
    }
    for (const auto& feature : model.categorical_features) {
        if (!buy_fill_dynamic_categorical_feature(feature)) {
            return true;
        }
    }
    return false;
}

double buy_fill_numeric_feature(const std::string& feature,
                                const QuoteCoreResult& quote,
                                const SideQuoteContext& ctx,
                                const QuotePrediction& pred,
                                double mid,
                                double inventory,
                                double max_inventory,
                                double mo_ema_bid,
                                double local_rank) {
    if (feature == "inventory_ratio") {
        return inventory / std::max(max_inventory, 1e-9);
    }
    if (feature == "markout_ema") {
        return mo_ema_bid;
    }
    if (feature == "microprice_shift_bps") {
        return quote.microprice_shift_bps;
    }
    if (feature == "near_depth_total") {
        return std::max(ctx.near_depth_total, ctx.l2_near_depth_total);
    }
    if (feature == "l2_book_refresh_ratio") {
        return ctx.l2_book_refresh_ratio;
    }
    if (feature == "l2_book_cancel_ratio") {
        return ctx.l2_book_cancel_ratio;
    }
    if (feature == "l2_near_depth_total") {
        return ctx.l2_near_depth_total;
    }
    if (feature == "order_exposure_increasing") {
        return inventory >= 0.0 ? 1.0 : 0.0;
    }
    if (feature == "queue_local_rank") {
        return local_rank;
    }
    if (feature == "quote_distance_bps") {
        return (mid > 0.0 && ctx.pre_guard_price > 0.0)
            ? std::abs(mid - ctx.pre_guard_price) / mid * 10000.0
            : 0.0;
    }
    if (feature == "toxicity") {
        return pred.tox_bid;
    }
    return std::numeric_limits<double>::quiet_NaN();
}

std::string buy_fill_categorical_feature(const std::string& feature,
                                         double inventory,
                                         const CommonSidePolicyCpp& policy) {
    if (feature == "side") {
        return "BUY";
    }
    if (feature == "quote_action") {
        return "place";
    }
    if (feature == "quote_allow_post") {
        return policy.allow_post ? "1" : "missing";
    }
    if (feature == "quote_allow_exposure_increase") {
        return policy.allow_exposure_increase ? "1" : "missing";
    }
    if (feature == "fill_eligible") {
        return policy.allow_post && policy.allow_exposure_increase ? "True" : "missing";
    }
    if (feature == "order_exposure_increasing") {
        return inventory >= 0.0 ? "1" : "missing";
    }
    return "missing";
}

struct FillSelectionScoreCpp {
    double score = 0.5;
    int missing = 0;
    int used = 0;
    int model_count = 0;
};

FillSelectionScoreCpp score_buy_fill_selection_cpp(const TickReplayInput& input,
                                                   const TickReplayParams& params,
                                                   const QuoteCoreResult& quote,
                                                   const SideQuoteContext& ctx,
                                                   const QuotePrediction& pred,
                                                   std::size_t static_row_idx,
                                                   double mid,
                                                   double inventory,
                                                   double mo_ema_bid,
                                                   double local_rank,
                                                   const CommonSidePolicyCpp& policy) {
    if (params.buy_fill_selection_models.empty()) {
        return {};
    }
    double score_sum = 0.0;
    int missing_sum = 0;
    int used_sum = 0;
    int model_count = 0;
    const bool has_static =
        !input.buy_fill_static_logit_delta.empty() &&
        static_row_idx < input.buy_fill_static_logit_delta.rows &&
        input.buy_fill_static_logit_delta.cols == params.buy_fill_selection_models.size();
    std::size_t fold_idx = 0;
    for (const auto& model : params.buy_fill_selection_models) {
        double total = model.base_logit +
            (has_static ? input.buy_fill_static_logit_delta(static_row_idx, fold_idx) : 0.0);
        int missing = has_static
            ? static_cast<int>(std::llround(input.buy_fill_static_missing(static_row_idx, fold_idx)))
            : 0;
        int used = has_static
            ? static_cast<int>(std::llround(input.buy_fill_static_used(static_row_idx, fold_idx)))
            : 0;
        for (const auto& [feature, cuts] : model.numeric_cuts) {
            if (!buy_fill_dynamic_numeric_feature(feature)) {
                if (!has_static) {
                    ++missing;
                }
                continue;
            }
            const double value = buy_fill_numeric_feature(
                feature,
                quote,
                ctx,
                pred,
                mid,
                inventory,
                params.max_inventory,
                mo_ema_bid,
                local_rank
            );
            if (!std::isfinite(value)) {
                ++missing;
            }
            const std::string bucket = numeric_bucket(value, cuts);
            if (const double* contrib = contribution_for(model, feature, bucket)) {
                total += model.contribution_scale * (*contrib);
                ++used;
            }
        }
        for (const auto& feature : model.categorical_features) {
            if (!buy_fill_dynamic_categorical_feature(feature)) {
                if (!has_static) {
                    ++missing;
                }
                continue;
            }
            const std::string bucket = buy_fill_categorical_feature(feature, inventory, policy);
            if (bucket == "missing") {
                ++missing;
            }
            if (const double* contrib = contribution_for(model, feature, bucket)) {
                total += model.contribution_scale * (*contrib);
                ++used;
            }
        }
        if (used > 0) {
            const double shrink = std::sqrt(static_cast<double>(used) / (static_cast<double>(used) + 4.0));
            total = model.base_logit + shrink * (total - model.base_logit);
        }
        score_sum += sigmoid_cpp(total);
        missing_sum += missing;
        used_sum += used;
        ++model_count;
        ++fold_idx;
    }
    const int n = std::max(model_count, 1);
    return FillSelectionScoreCpp{
        score_sum / static_cast<double>(n),
        static_cast<int>(std::llround(static_cast<double>(missing_sum) / static_cast<double>(n))),
        static_cast<int>(std::llround(static_cast<double>(used_sum) / static_cast<double>(n))),
        n,
    };
}

template <Side S>
std::tuple<double, double, bool> replace_throttle_params(const TickReplayParams& params,
                                                         double inventory) {
    const bool exposure_increasing = is_buy_v<S> ? inventory >= 0.0 : inventory <= 0.0;
    if (exposure_increasing) {
        return {
            std::max(0.0, params.replace_min_price_change_ticks),
            std::max(0.0, params.replace_min_interval_ms),
            true,
        };
    }
    const double reducing_ticks = params.replace_min_price_change_ticks_reducing > 0.0
        ? params.replace_min_price_change_ticks_reducing
        : params.replace_min_price_change_ticks;
    const double reducing_interval = params.replace_min_interval_ms_reducing > 0.0
        ? params.replace_min_interval_ms_reducing
        : params.replace_min_interval_ms;
    return {std::max(0.0, reducing_ticks), std::max(0.0, reducing_interval), false};
}

template <Side S>
bool apply_replace_throttle(const TickReplayParams& params,
                            TickReplaySummary& summary,
                            std::int64_t ts,
                            double inventory,
                            double target_price,
                            const ReplayOrder* order,
                            bool needs_update,
                            double tick_size) {
    if (!needs_update || order == nullptr ||
        (order->state != OrderState::Open && order->state != OrderState::PendingCancel) ||
        order->price <= 0.0) {
        return needs_update;
    }
    const auto [min_ticks, min_interval_ms, exposure_increasing] =
        replace_throttle_params<S>(params, inventory);
    (void)exposure_increasing;
    const double price_delta_ticks = std::abs(target_price - order->price) / std::max(tick_size, 1e-12);
    const double age_ms = static_cast<double>(std::max<std::int64_t>(0, ts - order->quote_ts));
    const bool throttle_by_price = min_ticks > 0.0 && price_delta_ticks + 1e-9 < min_ticks;
    const bool throttle_by_age = min_interval_ms > 0.0 && age_ms < min_interval_ms;
    if (!throttle_by_price && !throttle_by_age) {
        return needs_update;
    }
    ++summary.replace_throttle_count;
    if constexpr (is_buy_v<S>) {
        ++summary.replace_throttle_bid_count;
    } else {
        ++summary.replace_throttle_ask_count;
    }
    summary.replace_throttle_price_count += throttle_by_price ? 1 : 0;
    summary.replace_throttle_age_count += throttle_by_age ? 1 : 0;
    return false;
}

template <Side S>
bool should_cancel_first_replace(const TickReplayParams& params,
                                 double inventory,
                                 const ReplayOrder* order,
                                 bool needs_update,
                                 bool can_post) {
    if (!params.replace_cancel_first_exposure_increasing || !needs_update || !can_post ||
        order == nullptr ||
        (order->state != OrderState::Open && order->state != OrderState::PendingCancel)) {
        return false;
    }
    const auto [min_ticks, min_interval_ms, exposure_increasing] =
        replace_throttle_params<S>(params, inventory);
    (void)min_ticks;
    (void)min_interval_ms;
    return exposure_increasing;
}

template <Side S, typename FillObserver>
void process_side_fill(
    ReplayOrders& orders,
    TickReplayResult& result,
    const TickReplayInput& input,
    std::size_t trade_idx,
    std::int64_t ts,
    double trade_price,
    std::int64_t trade_price_tick,
    double trade_qty,
    double queue_deplete_mult,
    bool track_l2_cancel_ahead,
    double lot_size,
    double maker_fee,
    double order_size,
    double& cash,
    double& inventory,
    double& entry_price,
    ConsecutiveLossCooldownState& loss_cooldown,
    const std::function<void()>& observe_fill_risk,
    double& consecutive_side_fills,
    double& consecutive_other_fills,
    std::int64_t& last_side_fill_ts,
    PendingMarkouts& pending_markouts,
    bool markout_enabled,
    std::int64_t& fill_count,
    std::int64_t& pending_cancel_fills,
    std::int64_t& final_compressed_fills,
    std::int64_t trace_fills_max,
    std::int64_t trace_quotes_max,
    std::int64_t trace_window_ms,
    FillObserver&& fill_observer
) {
    double remaining_trade_qty = trade_qty * nonnegative_or_one(queue_deplete_mult);
    // Match Python replay fill selection:
    // BUY orders are consumed from the highest bid first, SELL orders from the
    // lowest ask first.  Vector insertion order can differ after replace /
    // pending-cancel lifecycles; using it here changes which queue gets
    // depleted first and can produce a few fill/PnL differences even when all
    // quote prices are identical.
    std::stable_sort(orders.begin(), orders.end(), [](const ReplayOrder& lhs, const ReplayOrder& rhs) {
        if constexpr (is_buy_v<S>) {
            return lhs.price_tick > rhs.price_tick;
        } else {
            return lhs.price_tick < rhs.price_tick;
        }
    });
    for (auto& order : orders) {
        if (order.state == OrderState::PendingNew && !order.exchange_accepted) {
            continue;
        }
        if (order.cancel_effective_ts > 0 && ts >= order.cancel_effective_ts) {
            continue;
        }
        const bool crosses = trade_crosses_order<S>(
            trade_price_tick,
            order.price_tick
        );
        const bool legacy_double_crosses = is_buy_v<S>
            ? trade_price <= order.price
            : trade_price >= order.price;
        const bool recovered_integer_tick_crossing =
            crosses && !legacy_double_crosses;
        if (order.fixed_spread_probe && crosses && !order.fixed_spread_probe_touched) {
            order.fixed_spread_probe_touched = true;
            if constexpr (is_buy_v<S>) {
                ++result.summary.fixed_spread_probe_bid_active_touched_orders;
            } else {
                ++result.summary.fixed_spread_probe_ask_active_touched_orders;
            }
        }
        if (track_l2_cancel_ahead && crosses &&
            trade_price_tick == order.price_tick) {
            order.queue_trade_since_l2 += trade_qty;
        }
        if (remaining_trade_qty < lot_size || order.remaining < lot_size) {
            continue;
        }
        if (!crosses) {
            continue;
        }
        if (recovered_integer_tick_crossing) {
            if constexpr (is_buy_v<S>) {
                ++result.summary.integer_tick_crossing_recovered_bid_candidates;
            } else {
                ++result.summary.integer_tick_crossing_recovered_ask_candidates;
            }
        }
        const double queue_before = order.queue_left;
        if (order.queue_left > 0.0) {
            const double eaten = std::min(order.queue_left, remaining_trade_qty);
            order.queue_left = subtract_lot_quantity(order.queue_left, eaten, lot_size);
            remaining_trade_qty = subtract_lot_quantity(remaining_trade_qty, eaten, lot_size);
        }
        if (remaining_trade_qty < lot_size || order.remaining < lot_size) {
            continue;
        }

        const double rem_before = order.remaining;
        double fill_qty = std::min(order.remaining, remaining_trade_qty);
        if (order.reduce_only) {
            const double reducible = is_buy_v<S>
                ? std::max(0.0, -inventory)
                : std::max(0.0, inventory);
            fill_qty = std::min(fill_qty, reducible);
        }
        fill_qty = floor_lot(fill_qty, lot_size);
        if (fill_qty < lot_size) {
            continue;
        }
        if (order.state == OrderState::PendingNew && order.exchange_accepted) {
            throw std::runtime_error(
                "pre-ACK exchange fill requires private-fill visibility scheduling"
            );
        }
        if (recovered_integer_tick_crossing) {
            if constexpr (is_buy_v<S>) {
                ++result.summary.integer_tick_crossing_recovered_bid_fills;
            } else {
                ++result.summary.integer_tick_crossing_recovered_ask_fills;
            }
        }

        const double q_before = inventory;
        cash += side_cash_delta<S>(order.price, fill_qty, maker_fee);
        inventory += inventory_delta_sign<S>() * fill_qty;
        loss_cooldown.template on_fill<S>(
            fill_qty,
            order.price,
            order.price * fill_qty * maker_fee
        );
        if (observe_fill_risk) observe_fill_risk();
        if (std::abs(loss_cooldown.inventory - inventory) >
            std::max(1e-10, lot_size * 1e-7)) {
            throw std::runtime_error(
                "consecutive-loss replay inventory diverged from execution state"
            );
        }
        if constexpr (is_buy_v<S>) {
            if (q_before <= 0.0 && inventory > 0.0) {
                entry_price = order.price;
            } else if (q_before > 0.0 && inventory > 0.0) {
                entry_price = (entry_price * q_before + order.price * fill_qty) / inventory;
            }
        } else {
            if (q_before >= 0.0 && inventory < 0.0) {
                entry_price = order.price;
            } else if (q_before < 0.0 && inventory < 0.0) {
                entry_price = (entry_price * (-q_before) + order.price * fill_qty) / (-inventory);
            }
        }
        if (std::abs(inventory) < 1e-10) {
            entry_price = 0.0;
        }
        order.remaining = subtract_lot_quantity(order.remaining, fill_qty, lot_size);
        remaining_trade_qty = subtract_lot_quantity(remaining_trade_qty, fill_qty, lot_size);
        if (order.fixed_spread_probe) {
            const std::int64_t fill_age_ms =
                std::max<std::int64_t>(0, ts - order.activate_ts);
            const bool first_probe_fill = !order.fixed_spread_probe_filled;
            const bool fully_filled = order.remaining < lot_size;
            if constexpr (is_buy_v<S>) {
                result.summary.fixed_spread_probe_bid_fill_qty += fill_qty;
                if (first_probe_fill) {
                    ++result.summary.fixed_spread_probe_bid_filled_orders;
                    result.summary
                        .fixed_spread_probe_bid_first_fill_pending_cancel_orders +=
                        order.state == OrderState::PendingCancel;
                    result.summary.fixed_spread_probe_bid_filled_within_1s +=
                        fill_age_ms <= 1'000 ? 1 : 0;
                    result.summary.fixed_spread_probe_bid_filled_within_5s +=
                        fill_age_ms <= 5'000 ? 1 : 0;
                    result.summary.fixed_spread_probe_bid_filled_within_10s +=
                        fill_age_ms <= 10'000 ? 1 : 0;
                }
                result.summary.fixed_spread_probe_bid_fully_filled_orders +=
                    fully_filled;
            } else {
                result.summary.fixed_spread_probe_ask_fill_qty += fill_qty;
                if (first_probe_fill) {
                    ++result.summary.fixed_spread_probe_ask_filled_orders;
                    result.summary
                        .fixed_spread_probe_ask_first_fill_pending_cancel_orders +=
                        order.state == OrderState::PendingCancel;
                    result.summary.fixed_spread_probe_ask_filled_within_1s +=
                        fill_age_ms <= 1'000 ? 1 : 0;
                    result.summary.fixed_spread_probe_ask_filled_within_5s +=
                        fill_age_ms <= 5'000 ? 1 : 0;
                    result.summary.fixed_spread_probe_ask_filled_within_10s +=
                        fill_age_ms <= 10'000 ? 1 : 0;
                }
                result.summary.fixed_spread_probe_ask_fully_filled_orders +=
                    fully_filled;
            }
            order.fixed_spread_probe_filled = true;
        }
        consecutive_side_fills += fill_qty / std::max(order_size, lot_size);
        consecutive_other_fills = 0.0;
        last_side_fill_ts = ts;
        if (markout_enabled) {
            pending_markouts.push_back(PendingMarkout{
                ts, order.price, is_buy_v<S>, order.final_compressed, fill_qty
            });
        }
        ++fill_count;
        fill_observer(
            S,
            order,
            trade_idx,
            ts,
            order.price,
            fill_qty,
            q_before,
            inventory,
            consecutive_side_fills,
            cash
        );
        if (order.circuit_breaker_close) {
            ++result.summary.circuit_breaker_close_fill_count;
        }
        if (order.state == OrderState::PendingCancel) {
            ++pending_cancel_fills;
        }
        if (order.final_compressed) {
            ++final_compressed_fills;
        }
        if (order.local_extreme_guard) {
            if constexpr (is_buy_v<S>) {
                ++result.summary.fills_bid_local_extreme_guard;
            } else {
                ++result.summary.fills_ask_local_extreme_guard;
            }
        }
        append_fill_trace(
            result,
            input,
            order,
            trade_idx,
            trade_price,
            queue_before,
            rem_before,
            fill_qty,
            q_before,
            inventory,
            maker_fee,
            trace_window_ms,
            trace_fills_max
        );
        if (trace_quotes_max > 0 &&
            result.quote_trace.size() < static_cast<std::size_t>(trace_quotes_max)) {
            append_order_trace(result, order, ts, TraceOutcome::Fill, CancelReason::Fill, fill_qty);
        }
    }

    std::erase_if(
        orders,
        [lot_size](const ReplayOrder& order) { return order.remaining < lot_size; });
}

template <Side S>
std::pair<double, double> match_ioc_order(
    const std::vector<std::pair<double, double>>& raw_levels,
    std::int64_t limit_tick, double quantity, double tick_size, double lot_size,
    bool market
) {
    std::vector<std::pair<double, double>> levels;
    std::set<std::int64_t> seen;
    double available = 0.0;
    for (const auto& [price, raw_qty] : raw_levels) {
        if (!std::isfinite(price) || price <= 0.0) continue;
        const auto tick = price_to_tick(price, tick_size);
        if (!market && (is_buy_v<S> ? tick > limit_tick : tick < limit_tick)) continue;
        if (!seen.insert(tick).second) continue;
        const double qty = std::isfinite(raw_qty) && raw_qty > 0.0 ? raw_qty : 0.0;
        levels.emplace_back(price, qty);
        available += qty;
    }
    const double filled = std::floor(
        std::min(std::max(0.0, quantity), available) / lot_size + 1e-12
    ) * lot_size;
    if (filled < lot_size) return {0.0, 0.0};
    std::sort(levels.begin(), levels.end(), [](const auto& a, const auto& b) {
        return is_buy_v<S> ? a.first < b.first : a.first > b.first;
    });
    double remaining = filled;
    double notional = 0.0;
    for (const auto& [price, qty] : levels) {
        const double take = std::min(remaining, qty);
        notional += price * take;
        remaining = subtract_lot_quantity(remaining, take, lot_size);
        if (remaining <= 1e-12) break;
    }
    return {filled, notional / filled};
}

template <Side S, typename FillObserver>
bool process_ioc_close_orders(
    ReplayOrders& orders,
    TickReplayResult& result,
    const TickReplayInput& input,
    std::size_t event_idx,
    std::int64_t ts,
    const BookSnapshot& book,
    const TickReplayParams& params,
    double lot_size,
    double taker_fee,
    double order_size,
    double& cash,
    double& inventory,
    double& entry_price,
    ConsecutiveLossCooldownState& loss_cooldown,
    const std::function<void()>& observe_fill_risk,
    double& consecutive_side_fills,
    double& consecutive_other_fills,
    std::int64_t& last_side_fill_ts,
    PendingMarkouts& pending_markouts,
    bool markout_enabled,
    std::int64_t& fill_count,
    std::int64_t trace_fills_max,
    std::int64_t trace_quotes_max,
    std::int64_t trace_window_ms,
    FillObserver&& fill_observer
) {
    bool filled_any = false;
    for (std::size_t idx = 0; idx < orders.size();) {
        auto& order = orders[idx];
        if (!order.immediate_or_cancel ||
            order.state == OrderState::PendingNew ||
            order.activate_ts > ts) {
            ++idx;
            continue;
        }

        const auto bbo_pos =
            index_at_or_before(input.bbo_ts_ms, order.activate_ts);
        const auto l2_pos =
            index_at_or_before(input.l2_ts_ms, order.activate_ts);
        const auto activation_book = book_snapshot_at(
            input,
            order.activate_ts,
            bbo_pos >= 0
                ? static_cast<std::size_t>(bbo_pos)
                : input.bbo_ts_ms.size(),
            l2_pos >= 0
                ? static_cast<std::size_t>(l2_pos)
                : input.l2_ts_ms.size(),
            book.best_bid,
            book.best_ask,
            params.quote.tick_size,
            params,
            order.activate_ts
        );
        const double reducible = is_buy_v<S>
            ? std::max(0.0, -inventory)
            : std::max(0.0, inventory);
        std::vector<std::pair<double, double>> levels;
        if (l2_pos >= 0 && !input.l2_bid_px.empty()) {
            const auto row = static_cast<std::size_t>(l2_pos);
            const auto& prices = is_buy_v<S> ? input.l2_ask_px : input.l2_bid_px;
            const auto& quantities = is_buy_v<S> ? input.l2_ask_qty : input.l2_bid_qty;
            for (std::size_t col = 0; col < prices.cols; ++col) {
                levels.emplace_back(prices(row, col), quantities(row, col));
            }
        } else {
            // BBO-only is a displayed top-size bound, never full-depth truth.
            levels.emplace_back(
                is_buy_v<S> ? activation_book.best_ask : activation_book.best_bid,
                is_buy_v<S> ? activation_book.ask_qty : activation_book.bid_qty
            );
        }
        const auto [fill_qty, touch] = match_ioc_order<S>(
            levels, order.price_tick, std::min(order.remaining, reducible),
            params.quote.tick_size, lot_size, order.emergency_market
        );
        if (fill_qty < lot_size) {
            ++result.summary.circuit_breaker_close_ioc_expire_count;
            if (trace_quotes_max > 0 &&
                result.quote_trace.size() <
                    static_cast<std::size_t>(trace_quotes_max)) {
                append_order_trace(
                    result,
                    order,
                    ts,
                    TraceOutcome::Cancel,
                    CancelReason::CircuitBreaker,
                    0.0
                );
            }
            orders.erase(orders.begin() + static_cast<std::ptrdiff_t>(idx));
            continue;
        }

        const double q_before = inventory;
        order.price = touch;
        // A multi-level execution VWAP need not be a legal submitted-price tick.
        // Preserve the original limit tick; do not round the realized cash price.
        cash += side_cash_delta<S>(touch, fill_qty, taker_fee);
        inventory += inventory_delta_sign<S>() * fill_qty;
        loss_cooldown.template on_fill<S>(
            fill_qty,
            touch,
            touch * fill_qty * taker_fee
        );
        if (observe_fill_risk) observe_fill_risk();
        if (std::abs(loss_cooldown.inventory - inventory) >
            std::max(1e-10, lot_size * 1e-7)) {
            throw std::runtime_error(
                "consecutive-loss replay inventory diverged from execution state"
            );
        }
        if (std::abs(inventory) < 1e-10) {
            entry_price = 0.0;
        }
        order.remaining = subtract_lot_quantity(order.remaining, fill_qty, lot_size);
        consecutive_side_fills += fill_qty / std::max(order_size, lot_size);
        consecutive_other_fills = 0.0;
        last_side_fill_ts = ts;
        if (markout_enabled) {
            pending_markouts.push_back(PendingMarkout{
                ts,
                touch,
                is_buy_v<S>,
                false,
                fill_qty,
            });
        }
        ++fill_count;
        fill_observer(
            S,
            order,
            event_idx,
            ts,
            touch,
            fill_qty,
            q_before,
            inventory,
            consecutive_side_fills,
            cash
        );
        ++result.summary.circuit_breaker_close_fill_count;
        ++result.summary.circuit_breaker_close_ioc_fill_count;
        append_fill_trace(
            result,
            input,
            order,
            event_idx,
            input.trade_price.data()[event_idx],
            0.0,
            order.quantity,
            fill_qty,
            q_before,
            inventory,
            taker_fee,
            trace_window_ms,
            trace_fills_max
        );
        if (trace_quotes_max > 0 &&
            result.quote_trace.size() <
                static_cast<std::size_t>(trace_quotes_max)) {
            append_order_trace(
                result,
                order,
                ts,
                TraceOutcome::Fill,
                CancelReason::IocFill,
                fill_qty
            );
        }
        filled_any = true;
        // IOC cancels any unfilled remainder.
        orders.erase(orders.begin() + static_cast<std::ptrdiff_t>(idx));
    }
    return filled_any;
}

}  // namespace

void validate_f05_cooldown_predicate_rows(
    const F05RepeatedBooleanCooldownConfig &config,
    const std::vector<F05CooldownPredicateRow> &rows) {
  const std::set<std::string_view> compiled_predicates{
      kF05ShortCrossPredicate,
      kF05LongCrossPredicate,
      kF05CampaignAgePredicate,
  };
  const bool all_predicates_compiled = std::all_of(
      config.policy.predicate_columns.begin(),
      config.policy.predicate_columns.end(),
      [&](const auto &column) { return compiled_predicates.contains(column); });
  if (rows.empty()) {
    if (!all_predicates_compiled) {
      throw std::invalid_argument(
          "F05 full replay requires exact predicate rows for noncompiled columns");
    }
    return;
  }
  std::int64_t previous_ordinal = 0;
  for (const auto &row : rows) {
    const bool predicate_width_valid =
        (row.predicate_values.empty() && all_predicates_compiled) ||
        row.predicate_values.size() == config.policy.predicate_columns.size();
    if (row.exposure_fill_ordinal <= previous_ordinal || row.fill_ts_ms <= 0 ||
        row.campaign_id <= 0 || row.snapshot_id.empty() ||
        !predicate_width_valid) {
      throw std::invalid_argument(
          "F05 full replay predicate-row identity is incomplete");
    }
    previous_ordinal = row.exposure_fill_ordinal;
  }
}

F05RepeatedBooleanCooldownRuntime::F05RepeatedBooleanCooldownRuntime(
    F05RepeatedBooleanCooldownConfig config)
    : config_(std::move(config)) {
  buy_lineage_.side = Side::Buy;
  sell_lineage_.side = Side::Sell;
  binding_error_ = validate_f05_policy(config_);
  if (!config_.buy_policy.policy_sha256.empty()) {
    const auto ema_count = config_.buy_policy.ema_half_lives_s.size();
    buy_ema_.assign(ema_count, 0.0);
    buy_velocity_.assign(ema_count, 0.0);
    buy_acceleration_.assign(ema_count, 0.0);
    buy_pairs_.assign(config_.buy_policy.predicate_pairs.size(), {});
  }
}

void F05RepeatedBooleanCooldownRuntime::update_window(
    const F05CooldownWindowObservation &observation) {
  const bool reset_only = observation.reset_feature_state;
  if ((!reset_only &&
       observation.right_ts_ns - observation.left_ts_ns !=
           kF05BooleanCooldownWindowWidthNs) ||
      (reset_only && observation.right_ts_ns != observation.left_ts_ns)) {
    throw std::invalid_argument("f05_window_width_drifted");
  }
  if (observation.left_ts_ns % kF05BooleanCooldownWindowWidthNs != 0 ||
      observation.right_ts_ns % kF05BooleanCooldownWindowWidthNs != 0) {
    throw std::invalid_argument("f05_window_grid_alignment_invalid");
  }
  if (observation.feature_ready_ts_ns < observation.right_ts_ns) {
    throw std::invalid_argument("f05_feature_ready_before_window_end");
  }
  if (last_right_ts_ns_.has_value()) {
    if (observation.right_ts_ns <= *last_right_ts_ns_) {
      throw std::invalid_argument("f05_window_clock_not_increasing");
    }
    if (!reset_only && observation.left_ts_ns != *last_right_ts_ns_) {
      throw std::invalid_argument("f05_missing_window_not_explicit");
    }
  }
  if (last_input_ready_ts_ns_.has_value() &&
      observation.feature_ready_ts_ns < *last_input_ready_ts_ns_) {
    throw std::invalid_argument("f05_feature_ready_clock_regressed");
  }
  if (last_market_generation_.has_value()) {
    if (observation.market_generation <= *last_market_generation_) {
      throw std::invalid_argument("f05_market_generation_not_increasing");
    }
    if (observation.depth_generation < *last_depth_generation_) {
      throw std::invalid_argument("f05_depth_generation_regressed");
    }
  }

  const auto reset_feature_state = [&]() {
    warmup_admitted_ = false;
    warmup_start_right_ts_ns_.reset();
    ema_initialized_ = false;
    last_observed_ts_ns_.reset();
    ema_ = {0.0, 0.0, 0.0};
    short_pair_ = {};
    long_pair_ = {};
    buy_ema_initialized_ = false;
    std::fill(buy_ema_.begin(), buy_ema_.end(), 0.0);
    std::fill(buy_velocity_.begin(), buy_velocity_.end(), 0.0);
    std::fill(buy_acceleration_.begin(), buy_acceleration_.end(), 0.0);
    std::fill(buy_pairs_.begin(), buy_pairs_.end(), F05CooldownPairState{});
    ++audit_.feature_state_reset_count;
  };
  if (reset_only) {
    if (observation.mid_usdc_per_btc.has_value() || observation.source_gap ||
        observation.source_stale || observation.warmup_admitted ||
        observation.channel_support_valid) {
      throw std::invalid_argument("f05_reset_marker_payload_invalid");
    }
    reset_feature_state();
    current_window_observed_ = false;
    current_channel_support_valid_ = false;
    last_right_ts_ns_ = observation.right_ts_ns;
    last_input_ready_ts_ns_ = observation.feature_ready_ts_ns;
    last_feature_ready_ts_ns_.reset();
    last_market_generation_ = observation.market_generation;
    last_depth_generation_ = observation.depth_generation;
    return;
  }

  const bool invalid_window =
      observation.source_gap || observation.source_stale;
  const bool historical_exchange_semantics =
      config_.feature_clock_semantics == "historical_exchange_m2_v1";
  const auto gap_since_observed_s =
      last_observed_ts_ns_.has_value()
          ? static_cast<double>(observation.right_ts_ns -
                                *last_observed_ts_ns_) /
                1'000'000'000.0
          : 0.0;
  if (!historical_exchange_semantics &&
      (observation.source_stale ||
       (observation.source_gap && last_observed_ts_ns_.has_value() &&
        gap_since_observed_s > config_.max_feature_age_s))) {
    reset_feature_state();
  }
  const bool observed =
      !invalid_window && observation.mid_usdc_per_btc.has_value();
  current_window_observed_ = observed;
  current_channel_support_valid_ =
      !invalid_window && observation.channel_support_valid;
  ++audit_.window_count;
  if (invalid_window) {
    ++audit_.gap_window_count;
  }
  if (observed) {
    const auto value = *observation.mid_usdc_per_btc;
    if (!std::isfinite(value) || value <= 0.0) {
      throw std::invalid_argument("f05_observed_mid_invalid");
    }
    if (!warmup_start_right_ts_ns_.has_value()) {
      warmup_start_right_ts_ns_ = observation.right_ts_ns;
    }
    if (!ema_initialized_) {
      ema_ = {value, value, value};
      ema_initialized_ = true;
    } else {
      if (!last_observed_ts_ns_.has_value() ||
          observation.right_ts_ns <= *last_observed_ts_ns_) {
        throw std::invalid_argument("f05_ema_clock_not_increasing");
      }
      const auto delta_s =
          static_cast<double>(observation.right_ts_ns - *last_observed_ts_ns_) /
          1'000'000'000.0;
      const auto previous = ema_;
      for (std::size_t index = 0; index < ema_.size(); ++index) {
        const auto decay =
            std::exp(-std::log(2.0) * delta_s / kF05SelectedHalfLivesS[index]);
        ema_[index] = decay * previous[index] + (1.0 - decay) * value;
      }
      update_f05_pair(short_pair_, ema_[0], ema_[1], observation.right_ts_ns);
      update_f05_pair(long_pair_, ema_[1], ema_[2], observation.right_ts_ns);
    }
    if (!config_.buy_policy.policy_sha256.empty()) {
      if (!buy_ema_initialized_) {
        std::fill(buy_ema_.begin(), buy_ema_.end(), value);
        std::fill(buy_velocity_.begin(), buy_velocity_.end(), 0.0);
        std::fill(buy_acceleration_.begin(), buy_acceleration_.end(), 0.0);
        buy_ema_initialized_ = true;
      } else {
        if (!last_observed_ts_ns_.has_value() ||
            observation.right_ts_ns <= *last_observed_ts_ns_) {
          throw std::invalid_argument("f05_buy_ema_clock_not_increasing");
        }
        const auto delta_s = static_cast<double>(
                                 observation.right_ts_ns -
                                 *last_observed_ts_ns_) /
                             1'000'000'000.0;
        const auto previous = buy_ema_;
        const auto previous_velocity = buy_velocity_;
        for (std::size_t index = 0; index < buy_ema_.size(); ++index) {
          const auto decay = std::exp(
              -std::log(2.0) * delta_s /
              config_.buy_policy.ema_half_lives_s[index]);
          const auto current =
              decay * previous[index] + (1.0 - decay) * value;
          const auto velocity = (current - previous[index]) / delta_s;
          buy_ema_[index] = current;
          buy_velocity_[index] = velocity;
          buy_acceleration_[index] =
              (velocity - previous_velocity[index]) / delta_s;
        }
        for (std::size_t index = 0; index < buy_pairs_.size(); ++index) {
          const auto &pair = config_.buy_policy.predicate_pairs[index];
          update_f05_pair(buy_pairs_[index],
                          buy_ema_[pair.fast_ema_index],
                          buy_ema_[pair.slow_ema_index],
                          observation.right_ts_ns);
        }
      }
    }
    last_observed_ts_ns_ = observation.right_ts_ns;
    const auto warmup_elapsed_s =
        static_cast<double>(observation.right_ts_ns -
                            *warmup_start_right_ts_ns_) /
        1'000'000'000.0;
    warmup_admitted_ = historical_exchange_semantics
                           ? observation.warmup_admitted
                           : warmup_elapsed_s >= config_.warmup_s;
  } else if (historical_exchange_semantics) {
    warmup_admitted_ = observation.warmup_admitted;
  }
  last_right_ts_ns_ = observation.right_ts_ns;
  last_input_ready_ts_ns_ = observation.feature_ready_ts_ns;
  last_feature_ready_ts_ns_ = observation.feature_ready_ts_ns;
  last_market_generation_ = observation.market_generation;
  last_depth_generation_ = observation.depth_generation;
}

F05CooldownDecision F05RepeatedBooleanCooldownRuntime::apply_fill(
    const F05CooldownFillInput &input) {
  constexpr double flat_tolerance = 1e-10;
  auto baseline = input.baseline_duration_ms;
  bool baseline_valid = baseline >= kF05BooleanCooldownControlUnitMs;
  if (!std::isfinite(input.consecutive_units_after) ||
      input.consecutive_units_after <= 0.0) {
    baseline_valid = false;
  } else {
    const auto expected = static_cast<std::int64_t>(
        std::llround(static_cast<double>(kF05BooleanCooldownControlUnitMs) *
                     std::max(1.0, input.consecutive_units_after)));
    baseline_valid = baseline_valid && baseline == expected;
  }
  if (!baseline_valid) {
    baseline = kF05BooleanCooldownControlUnitMs;
  }

  F05CooldownDecision decision;
  decision.snapshot_id = input.snapshot_id.empty() ? "cpp-runtime-predicate-row"
                                                   : input.snapshot_id;
  decision.side = input.side;
  decision.role = input.role;
  decision.fill_ts_ms = input.fill_ts_ms;
  decision.campaign_id = input.campaign_id;
  decision.consecutive_units_after = input.consecutive_units_after;
  decision.baseline_duration_ms = baseline;
  decision.duration_ms = baseline;
  decision.action_id = std::string(kF05BooleanCooldownControlAction);
  const bool buy_policy_enabled =
      input.side == Side::Buy && !config_.buy_policy.policy_sha256.empty();
  const auto &active_policy =
      buy_policy_enabled ? config_.buy_policy : config_.policy;
  decision.policy_sha256 = active_policy.policy_sha256;
  decision.predicate_bundle_sha256 = active_policy.predicate_bundle_sha256;
  decision.feature_ready_ts_ns = last_feature_ready_ts_ns_.value_or(0);
  decision.feature_age_ms =
      last_feature_ready_ts_ns_.has_value()
          ? static_cast<double>(input.decision_ts_ns -
                                *last_feature_ready_ts_ns_) /
                1'000'000.0
          : std::numeric_limits<double>::infinity();

  const auto clear_lineage = [&](F05CooldownLineageState &lineage) {
    if (lineage.active) {
      lineage.active = false;
      ++audit_.lineage_clear_count;
    }
  };
  auto &same_lineage = input.side == Side::Buy ? buy_lineage_ : sell_lineage_;
  auto &opposite_lineage =
      input.side == Side::Buy ? sell_lineage_ : buy_lineage_;

  const bool inventory_finite =
      std::isfinite(input.inventory_before_fill_btc) &&
      std::isfinite(input.inventory_after_fill_btc);
  const bool exposure_increasing =
      inventory_finite &&
      ((input.side == Side::Buy &&
        input.inventory_before_fill_btc >= 0.0 &&
        input.inventory_after_fill_btc > input.inventory_before_fill_btc) ||
       (input.side == Side::Sell &&
        input.inventory_before_fill_btc <= 0.0 &&
        input.inventory_after_fill_btc < input.inventory_before_fill_btc));
  const bool reducing =
      inventory_finite && !exposure_increasing &&
      ((input.side == Side::Buy &&
        input.inventory_after_fill_btc > input.inventory_before_fill_btc) ||
       (input.side == Side::Sell &&
        input.inventory_after_fill_btc < input.inventory_before_fill_btc));
  const bool before_flat =
      inventory_finite &&
      std::abs(input.inventory_before_fill_btc) <= flat_tolerance;
  const auto expected_role = exposure_increasing
                                 ? (before_flat ? F05CooldownFillRole::Opener
                                                : F05CooldownFillRole::Add)
                                 : F05CooldownFillRole::Reducing;

  clear_lineage(opposite_lineage);
  if (reducing && input.role == F05CooldownFillRole::Reducing) {
    ++audit_.reducing_bypass_count;
    decision.support_valid = true;
    decision.coverage_reason_code = "reducing_fill_baseline_bypass";
    return decision;
  }

  ++audit_.evaluation_count;
  auto fallback = [&](std::string reason, bool support_valid) {
    decision.coverage_reason_code = reason;
    decision.fallback_reason = std::move(reason);
    decision.support_valid = support_valid;
  };

  if (!exposure_increasing || input.role != expected_role) {
    fallback("fill_role_or_inventory_transition_invalid", false);
  } else if (!baseline_valid) {
    fallback("baseline_duration_ms_invalid", false);
  } else if (input.fill_ts_ms <= 0 || input.decision_ts_ns <= 0 ||
             input.campaign_id <= 0 || !std::isfinite(input.campaign_age_s) ||
             input.campaign_age_s < 0.0) {
    fallback("campaign_or_decision_context_invalid", false);
  } else if (input.side == Side::Buy && !buy_policy_enabled) {
    ++audit_.buy_control_count;
    fallback("buy_control_by_contract", true);
  } else if (!execution_admitted()) {
    fallback(binding_error_.empty()
                 ? "cpp_parity_not_qualified"
                 : "cpp_policy_binding_invalid:" + binding_error_,
             false);
  } else if (!input.policy_input_valid) {
    fallback(input.snapshot_fallback_reason.empty()
                 ? "snapshot_policy_input_invalid"
                 : "snapshot_invalid:" + input.snapshot_fallback_reason,
             false);
  } else if (!input.support_valid || !input.channel_support_valid ||
             (config_.feature_clock_semantics ==
                  "historical_exchange_m2_v1" &&
              !current_channel_support_valid_)) {
    fallback("snapshot_m2_support_invalid", false);
  } else if (!last_feature_ready_ts_ns_.has_value()) {
    fallback(config_.feature_clock_semantics ==
                     "receive_time_full_mid_ema_bank_v1"
                 ? "no_completed_receive_time_window"
                 : "no_completed_causal_window",
             false);
  } else if (input.decision_ts_ns < *last_feature_ready_ts_ns_) {
    fallback("feature_ready_state_crossed_decision_cutoff", false);
  } else if (!warmup_admitted_) {
    fallback(config_.feature_clock_semantics ==
                     "receive_time_full_mid_ema_bank_v1"
                 ? "receive_time_ema_warmup_incomplete"
                 : "ema_warmup_incomplete",
             false);
  } else if (config_.feature_clock_semantics == "historical_exchange_m2_v1" &&
             (!last_right_ts_ns_.has_value() ||
              input.decision_ts_ns - *last_right_ts_ns_ >=
                  kF05BooleanCooldownWindowWidthNs)) {
    fallback("completed_window_stream_stale_at_fill_visible_cutoff", false);
  } else if (config_.feature_clock_semantics != "historical_exchange_m2_v1" &&
             decision.feature_age_ms >
                 config_.max_feature_age_s * 1'000.0) {
    fallback(config_.feature_clock_semantics ==
                     "receive_time_full_mid_ema_bank_v1"
                 ? "receive_time_mid_state_stale"
                 : "feature_state_stale",
             false);
  } else if (!current_window_observed_) {
    // BUY E3 materializes its selected predicates from the feature row.  A
    // missing current mid therefore becomes an unobserved selected predicate,
    // while the legacy SELL policy reports the transport-level window reason.
    // Preserve those side-specific public reason-code semantics even though
    // both paths take the same fail-closed CONTROL action.
    fallback(buy_policy_enabled
                 ? "selected_predicate_state_unobserved"
                 : "latest_completed_mid_window_unobserved",
             false);
  } else {
    std::vector<F05TriState> predicates;
    if (!input.predicate_values.empty()) {
      if (input.predicate_values.size() !=
          active_policy.predicate_columns.size()) {
        fallback("runtime_predicate_columns_drifted", false);
      } else {
        predicates = input.predicate_values;
      }
    } else if (buy_policy_enabled) {
      if (!buy_ema_initialized_ || buy_ema_.size() !=
              config_.buy_policy.ema_half_lives_s.size() ||
          buy_pairs_.size() != config_.buy_policy.predicate_pairs.size()) {
        fallback("buy_feature_state_uninitialized", false);
      } else {
        predicates = materialize_f05_declarative_predicates(
            config_.buy_policy,
            buy_ema_,
            buy_velocity_,
            buy_acceleration_,
            buy_pairs_,
            input,
            baseline);
      }
    } else {
      predicates.assign(active_policy.predicate_columns.size(),
                        F05TriState::Unobserved);
      const auto set_predicate = [&](std::string_view name, F05TriState state) {
        const auto found =
            std::find(active_policy.predicate_columns.begin(),
                      active_policy.predicate_columns.end(), name);
        if (found != active_policy.predicate_columns.end()) {
          predicates[static_cast<std::size_t>(std::distance(
              active_policy.predicate_columns.begin(), found))] = state;
        }
      };
      const auto cross_state = [&](const F05CooldownPairState &pair) {
        if (!pair.last_cross_ts_ns.has_value()) {
          return F05TriState::Unobserved;
        }
        const auto age_s =
            static_cast<double>(input.decision_ts_ns - *pair.last_cross_ts_ns) /
            1'000'000'000.0;
        if (!std::isfinite(age_s) || age_s < 0.0) {
          return F05TriState::Unobserved;
        }
        return age_s <= kF05CrossAgeThresholdS ? F05TriState::True
                                               : F05TriState::False;
      };
      set_predicate(kF05ShortCrossPredicate, cross_state(short_pair_));
      set_predicate(kF05LongCrossPredicate, cross_state(long_pair_));
      set_predicate(kF05CampaignAgePredicate,
                    input.campaign_age_s * 1'000.0 >
                            static_cast<double>(baseline)
                        ? F05TriState::True
                        : F05TriState::False);
    }

    if (decision.coverage_reason_code.empty()) {
      bool resolved = false;
      for (std::size_t index = 0; index < active_policy.rules.size();
           ++index) {
        const auto state =
            f05_rule_state(active_policy.rules[index], predicates);
        if (state == F05TriState::Unobserved) {
          fallback("rule_unobserved:" + std::to_string(index), false);
          resolved = true;
          break;
        }
        if (state == F05TriState::True) {
          const auto &rule = active_policy.rules[index];
          decision.action_id = rule.action_id;
          decision.duration_ms = rule.duration_ms;
          decision.matched_rule_index = index;
          decision.support_valid = true;
          decision.coverage_reason_code = "policy_rule_matched";
          resolved = true;
          break;
        }
      }
      if (!resolved) {
        fallback("no_rule_matched", true);
      }
    }
  }

  if (decision.support_valid) {
    ++audit_.supported_count;
  }
  if (!decision.fallback_reason.empty()) {
    ++audit_.fallback_count;
  }
  if (decision.action_id != kF05BooleanCooldownControlAction) {
    ++audit_.nonbaseline_count;
  }

  if (exposure_increasing && input.fill_ts_ms > 0) {
    if (decision.duration_ms <= 0 ||
        input.fill_ts_ms >
            std::numeric_limits<std::int64_t>::max() - decision.duration_ms) {
      decision.action_id = std::string(kF05BooleanCooldownControlAction);
      decision.duration_ms = baseline;
      decision.matched_rule_index.reset();
      decision.support_valid = false;
      decision.coverage_reason_code = "duration_deadline_invalid";
      decision.fallback_reason = "duration_deadline_invalid";
    }
    same_lineage.active = true;
    same_lineage.side = input.side;
    ++same_lineage.revision;
    same_lineage.campaign_id = input.campaign_id;
    same_lineage.fill_ts_ms = input.fill_ts_ms;
    same_lineage.deadline_ts_ms = input.fill_ts_ms + decision.duration_ms;
    same_lineage.consecutive_units_after = input.consecutive_units_after;
    same_lineage.duration_ms = decision.duration_ms;
    same_lineage.action_id = decision.action_id;
    same_lineage.coverage_reason_code = decision.coverage_reason_code;
    decision.deadline_ts_ms = same_lineage.deadline_ts_ms;
    decision.lineage_revision = same_lineage.revision;
    decision.lineage_applied = true;
    ++audit_.lineage_count;
  }
  return decision;
}

void F05RepeatedBooleanCooldownRuntime::override_active_lineage_duration(
    Side side,
    std::int64_t fill_ts_ms,
    std::int64_t campaign_id,
    std::int64_t duration_ms,
    std::string action_id,
    std::string coverage_reason_code) {
  auto &lineage = side == Side::Buy ? buy_lineage_ : sell_lineage_;
  if (!lineage.active || lineage.side != side ||
      lineage.fill_ts_ms != fill_ts_ms ||
      lineage.campaign_id != campaign_id) {
    throw std::invalid_argument("f05_one_shot_lineage_identity_drifted");
  }
  if (duration_ms <= 0 ||
      fill_ts_ms > std::numeric_limits<std::int64_t>::max() - duration_ms) {
    throw std::invalid_argument("f05_one_shot_lineage_duration_invalid");
  }
  if (action_id.empty() || coverage_reason_code.empty()) {
    throw std::invalid_argument("f05_one_shot_lineage_metadata_invalid");
  }
  lineage.duration_ms = duration_ms;
  lineage.deadline_ts_ms = fill_ts_ms + duration_ms;
  lineage.action_id = std::move(action_id);
  lineage.coverage_reason_code = std::move(coverage_reason_code);
}

void F05RepeatedBooleanCooldownRuntime::advance_time(std::int64_t now_ms) {
  if (now_ms < 0) {
    throw std::invalid_argument("f05_runtime_clock_invalid");
  }
  for (auto *lineage : {&buy_lineage_, &sell_lineage_}) {
    if (lineage->active && now_ms >= lineage->deadline_ts_ms) {
      lineage->active = false;
      ++audit_.lineage_clear_count;
    }
  }
}

bool F05RepeatedBooleanCooldownRuntime::add_blocked(Side side,
                                                    std::int64_t now_ms) const {
  const auto &value = side == Side::Buy ? buy_lineage_ : sell_lineage_;
  return value.active && now_ms < value.deadline_ts_ms;
}

F05CooldownLineageState
F05RepeatedBooleanCooldownRuntime::lineage(Side side) const {
  return side == Side::Buy ? buy_lineage_ : sell_lineage_;
}

F05CooldownRuntimeAudit
F05RepeatedBooleanCooldownRuntime::audit() const noexcept {
  return audit_;
}

F05RepeatedBooleanCooldownCheckpoint
F05RepeatedBooleanCooldownRuntime::checkpoint() const {
  F05RepeatedBooleanCooldownCheckpoint output;
  output.abi_version = std::string(kF05RepeatedBooleanCooldownAbi);
  output.qualification_under_test = config_.qualification_under_test;
  output.parity_qualification_sha256 = config_.parity_qualification_sha256;
  output.qualification_scope = config_.qualification_scope;
  output.feature_clock_semantics = config_.feature_clock_semantics;
  output.policy_sha256 = config_.policy.policy_sha256;
  output.predicate_bundle_sha256 = config_.policy.predicate_bundle_sha256;
  output.buy_policy_sha256 = config_.buy_policy.policy_sha256;
  output.buy_predicate_bundle_sha256 =
      config_.buy_policy.predicate_bundle_sha256;
  output.warmup_s = config_.warmup_s;
  output.max_feature_age_s = config_.max_feature_age_s;
  output.warmup_admitted = warmup_admitted_;
  output.warmup_start_right_ts_ns = warmup_start_right_ts_ns_;
  output.last_right_ts_ns = last_right_ts_ns_;
  output.last_input_ready_ts_ns = last_input_ready_ts_ns_;
  output.last_feature_ready_ts_ns = last_feature_ready_ts_ns_;
  output.last_market_generation = last_market_generation_;
  output.last_depth_generation = last_depth_generation_;
  output.ema_initialized = ema_initialized_;
  output.buy_ema_initialized = buy_ema_initialized_;
  output.current_window_observed = current_window_observed_;
  output.current_channel_support_valid = current_channel_support_valid_;
  output.last_observed_ts_ns = last_observed_ts_ns_;
  output.ema = ema_;
  output.short_pair = short_pair_;
  output.long_pair = long_pair_;
  output.buy_ema = buy_ema_;
  output.buy_velocity = buy_velocity_;
  output.buy_acceleration = buy_acceleration_;
  output.buy_pairs = buy_pairs_;
  output.buy_lineage = buy_lineage_;
  output.sell_lineage = sell_lineage_;
  output.audit = audit_;
  output.canonical_payload = f05_checkpoint_payload(output);
  output.checkpoint_sha256 = f05_sha256(output.canonical_payload);
  return output;
}

void F05RepeatedBooleanCooldownRuntime::restore(
    const F05RepeatedBooleanCooldownCheckpoint &value) {
  if (value.abi_version != kF05RepeatedBooleanCooldownAbi ||
      value.qualification_under_test != config_.qualification_under_test ||
      value.parity_qualification_sha256 !=
          config_.parity_qualification_sha256 ||
      value.qualification_scope != config_.qualification_scope ||
      value.feature_clock_semantics != config_.feature_clock_semantics ||
      value.policy_sha256 != config_.policy.policy_sha256 ||
      value.predicate_bundle_sha256 != config_.policy.predicate_bundle_sha256 ||
      value.buy_policy_sha256 != config_.buy_policy.policy_sha256 ||
      value.buy_predicate_bundle_sha256 !=
          config_.buy_policy.predicate_bundle_sha256 ||
      f05_double_bits(value.warmup_s) != f05_double_bits(config_.warmup_s) ||
      f05_double_bits(value.max_feature_age_s) !=
          f05_double_bits(config_.max_feature_age_s)) {
    throw std::invalid_argument("f05_checkpoint_identity_drifted");
  }
  const auto canonical = f05_checkpoint_payload(value);
  if (value.canonical_payload != canonical ||
      value.checkpoint_sha256 != f05_sha256(canonical)) {
    throw std::invalid_argument("f05_checkpoint_hash_drifted");
  }
  const auto finite = [](double item) { return std::isfinite(item); };
  if (!std::all_of(value.ema.begin(), value.ema.end(), finite) ||
      !std::all_of(value.buy_ema.begin(), value.buy_ema.end(), finite) ||
      !std::all_of(value.buy_velocity.begin(), value.buy_velocity.end(), finite) ||
      !std::all_of(value.buy_acceleration.begin(), value.buy_acceleration.end(),
                   finite) ||
      value.buy_ema.size() != buy_ema_.size() ||
      value.buy_velocity.size() != buy_velocity_.size() ||
      value.buy_acceleration.size() != buy_acceleration_.size() ||
      value.buy_pairs.size() != buy_pairs_.size() ||
      value.buy_lineage.side != Side::Buy ||
      value.sell_lineage.side != Side::Sell) {
    throw std::invalid_argument("f05_checkpoint_state_invalid");
  }
  for (const auto *pair : {&value.short_pair, &value.long_pair}) {
    if (pair->effective_sign < -1 || pair->effective_sign > 1 ||
        pair->last_cross_direction < -1 || pair->last_cross_direction > 1 ||
        (pair->last_cross_ts_ns.has_value() &&
         pair->last_cross_direction == 0)) {
      throw std::invalid_argument("f05_checkpoint_pair_state_invalid");
    }
  }
  for (const auto &pair : value.buy_pairs) {
    if (pair.effective_sign < -1 || pair.effective_sign > 1 ||
        pair.last_cross_direction < -1 || pair.last_cross_direction > 1 ||
        (pair.last_cross_ts_ns.has_value() &&
         pair.last_cross_direction == 0)) {
      throw std::invalid_argument("f05_checkpoint_buy_pair_state_invalid");
    }
  }
  warmup_admitted_ = value.warmup_admitted;
  warmup_start_right_ts_ns_ = value.warmup_start_right_ts_ns;
  last_right_ts_ns_ = value.last_right_ts_ns;
  last_input_ready_ts_ns_ = value.last_input_ready_ts_ns;
  last_feature_ready_ts_ns_ = value.last_feature_ready_ts_ns;
  last_market_generation_ = value.last_market_generation;
  last_depth_generation_ = value.last_depth_generation;
  ema_initialized_ = value.ema_initialized;
  buy_ema_initialized_ = value.buy_ema_initialized;
  current_window_observed_ = value.current_window_observed;
  current_channel_support_valid_ = value.current_channel_support_valid;
  last_observed_ts_ns_ = value.last_observed_ts_ns;
  ema_ = value.ema;
  short_pair_ = value.short_pair;
  long_pair_ = value.long_pair;
  buy_ema_ = value.buy_ema;
  buy_velocity_ = value.buy_velocity;
  buy_acceleration_ = value.buy_acceleration;
  buy_pairs_ = value.buy_pairs;
  buy_lineage_ = value.buy_lineage;
  sell_lineage_ = value.sell_lineage;
  audit_ = value.audit;
}

bool F05RepeatedBooleanCooldownRuntime::parity_qualified() const noexcept {
  return config_.parity_qualified && binding_error_.empty() &&
         is_lower_sha256(config_.parity_qualification_sha256);
}

bool F05RepeatedBooleanCooldownRuntime::qualification_under_test() const noexcept {
  return config_.qualification_under_test && binding_error_.empty() &&
         !config_.parity_qualified &&
         config_.parity_qualification_sha256.empty();
}

bool F05RepeatedBooleanCooldownRuntime::execution_admitted() const noexcept {
  return parity_qualified() || qualification_under_test();
}

const std::string &
F05RepeatedBooleanCooldownRuntime::binding_error() const noexcept {
  return binding_error_;
}

const F05RepeatedBooleanCooldownConfig &
F05RepeatedBooleanCooldownRuntime::config() const noexcept {
  return config_;
}

std::int64_t sample_keyed_latency_ms(
    std::int64_t base_ms,
    std::int64_t jitter_ms,
    const std::vector<double>& samples,
    std::int64_t seed,
    std::int64_t event_ts_ms,
    bool is_buy,
    std::uint64_t operation,
    std::int64_t order_ts_ms,
    bool stress_enabled,
    double stress_spike_probability,
    double stress_spike_multiplier
) {
    if (operation < static_cast<std::uint64_t>(LatencyOperation::NewOrder) ||
        operation > static_cast<std::uint64_t>(LatencyOperation::QueueValueCancel)) {
        throw std::invalid_argument("latency operation must be within [1, 4]");
    }
    TickReplayParams params;
    params.latency_seed = seed;
    params.latency_stress_enabled = stress_enabled;
    params.latency_stress_spike_probability = stress_spike_probability;
    params.latency_stress_spike_multiplier = stress_spike_multiplier;
    return sample_latency_ms(
        base_ms,
        jitter_ms,
        &samples,
        params,
        event_ts_ms,
        is_buy ? Side::Buy : Side::Sell,
        static_cast<LatencyOperation>(operation),
        order_ts_ms
    );
}

double sample_keyed_random_passive_unit(
    std::int64_t seed,
    std::int64_t event_ts_ms,
    std::int64_t action_identity,
    std::uint64_t operation
) {
    if (operation < static_cast<std::uint64_t>(RandomPassiveOperation::TimingJitter) ||
        operation > static_cast<std::uint64_t>(RandomPassiveOperation::SideMirror)) {
        throw std::invalid_argument("random-passive operation must be within [1, 2]");
    }
    return keyed_random_passive_unit(
        seed,
        event_ts_ms,
        action_identity,
        static_cast<RandomPassiveOperation>(operation)
    );
}

void TickReplayInput::validate() const {
    if (trade_ts_ms.empty()) [[unlikely]] {
        throw std::invalid_argument("trade_ts_ms is empty");
    }
    require_same_size(trade_ts_ms.size(), trade_price.size(), "trade_price");
    require_same_size(trade_ts_ms.size(), trade_qty.size(), "trade_qty");
    require_same_size(trade_ts_ms.size(), is_buyer_maker.size(), "is_buyer_maker");
    if (var_ts_ms.size() != var_ssq.size()) {
        throw std::invalid_argument("var_ts_ms and var_ssq length mismatch");
    }
    if (var_ti.size() > 0) {
        require_same_size(var_ts_ms.size(), var_ti.size(), "var_ti");
    }
    if (var_retsq.size() > 0) {
        require_same_size(var_ts_ms.size(), var_retsq.size(), "var_retsq");
    }
    if (ml_ts_ms.size() > 0) {
        require_same_size(ml_ts_ms.size(), ml_dir_10s.size(), "ml_dir_10s");
        require_same_size(ml_ts_ms.size(), ml_vol_10s.size(), "ml_vol_10s");
        require_same_size(ml_ts_ms.size(), ml_ret_10s.size(), "ml_ret_10s");
        require_same_size(ml_ts_ms.size(), ml_tox_bid.size(), "ml_tox_bid");
        require_same_size(ml_ts_ms.size(), ml_tox_ask.size(), "ml_tox_ask");
    }
    const bool has_conditional_p3 =
        !p3_ts_ms.empty() || !p3_delta_star.empty() || !p3_kappa_eff.empty();
    if (has_conditional_p3) {
        if (p3_ts_ms.empty()) {
            throw std::invalid_argument(
                "conditional P3 values require non-empty p3_ts_ms"
            );
        }
        require_same_size(p3_ts_ms.size(), p3_delta_star.size(), "p3_delta_star");
        require_same_size(p3_ts_ms.size(), p3_kappa_eff.size(), "p3_kappa_eff");
        for (std::size_t i = 0; i < p3_ts_ms.size(); ++i) {
            if (i > 0 && p3_ts_ms.data()[i] <= p3_ts_ms.data()[i - 1]) {
                throw std::invalid_argument(
                    "conditional P3 timestamps must be strictly increasing"
                );
            }
            if (!std::isfinite(p3_delta_star.data()[i]) ||
                p3_delta_star.data()[i] <= 0.0 ||
                !std::isfinite(p3_kappa_eff.data()[i]) ||
                p3_kappa_eff.data()[i] <= 0.0) {
                throw std::invalid_argument(
                    "conditional P3 delta_star and kappa_eff must be finite and positive"
                );
            }
        }
    }
    const bool has_buy_fill_static =
        !buy_fill_static_logit_delta.empty() ||
        !buy_fill_static_missing.empty() ||
        !buy_fill_static_used.empty();
    if (has_buy_fill_static) {
        const std::size_t expected_rows = ml_ts_ms.size() + 1;
        if (buy_fill_static_logit_delta.rows != expected_rows ||
            buy_fill_static_missing.rows != expected_rows ||
            buy_fill_static_used.rows != expected_rows) {
            throw std::invalid_argument(
                "BUY fill-selection static matrix rows must contain cold-start "
                "plus one row per ml_ts_ms");
        }
        if (buy_fill_static_logit_delta.cols != buy_fill_static_missing.cols ||
            buy_fill_static_logit_delta.cols != buy_fill_static_used.cols) {
            throw std::invalid_argument(
                "BUY fill-selection static matrix fold count mismatch");
        }
    }
    const bool has_p3_reach_gate =
        !p3_reach_gate_ts_ms.empty() || !p3_reach_gate_status.empty();
    if (has_p3_reach_gate) {
        if (p3_reach_gate_ts_ms.empty() || p3_reach_gate_status.empty()) {
            throw std::invalid_argument(
                "conditional P3 reach gate requires timestamps and status matrix"
            );
        }
        if (p3_reach_gate_status.rows != p3_reach_gate_ts_ms.size()) {
            throw std::invalid_argument(
                "conditional P3 reach gate matrix rows must match timestamps"
            );
        }
        if (p3_reach_gate_status.cols == 0 || p3_reach_gate_status.cols % 4 != 0) {
            throw std::invalid_argument(
                "conditional P3 reach gate columns must contain four equal blocks"
            );
        }
        if (p3_reach_gate_ts_ms.size() != ml_ts_ms.size()) {
            throw std::invalid_argument(
                "conditional P3 reach gate timestamps must align to ML timestamps"
            );
        }
        for (std::size_t row = 0; row < p3_reach_gate_ts_ms.size(); ++row) {
            if (p3_reach_gate_ts_ms.data()[row] != ml_ts_ms.data()[row]) {
                throw std::invalid_argument(
                    "conditional P3 reach gate timestamp differs from ML timestamp"
                );
            }
            for (std::size_t column = 0; column < p3_reach_gate_status.cols; ++column) {
                if (p3_reach_gate_status(row, column) > 2) {
                    throw std::invalid_argument(
                        "conditional P3 reach gate status must lie in {0,1,2}"
                    );
                }
            }
        }
    }
    const bool has_p3_reach_budget =
        !p3_reach_budget_ts_ms.empty() || !p3_reach_budget_selected_k.empty();
    if (has_p3_reach_budget) {
        if (p3_reach_budget_ts_ms.empty() || p3_reach_budget_selected_k.empty()) {
            throw std::invalid_argument(
                "conditional P3 reach budget requires timestamps and selected-k matrix"
            );
        }
        if (p3_reach_budget_selected_k.rows != p3_reach_budget_ts_ms.size()) {
            throw std::invalid_argument(
                "conditional P3 reach budget matrix rows must match timestamps"
            );
        }
        if (p3_reach_budget_selected_k.cols != 4 * kP3ReachBudgetGridSize) {
            throw std::invalid_argument(
                "conditional P3 reach budget matrix must contain four 1180-column distance blocks"
            );
        }
        if (p3_reach_budget_ts_ms.size() != ml_ts_ms.size()) {
            throw std::invalid_argument(
                "conditional P3 reach budget timestamps must align to ML timestamps"
            );
        }
        for (std::size_t row = 0; row < p3_reach_budget_ts_ms.size(); ++row) {
            const auto timestamp = p3_reach_budget_ts_ms.data()[row];
            if (timestamp != ml_ts_ms.data()[row]) {
                throw std::invalid_argument(
                    "conditional P3 reach budget timestamp differs from ML timestamp"
                );
            }
            if (timestamp % kP3ReachBudgetBucketMs != 0) {
                throw std::invalid_argument(
                    "conditional P3 reach budget timestamp is not a canonical 10s bucket"
                );
            }
            if (row > 0 && timestamp <= p3_reach_budget_ts_ms.data()[row - 1]) {
                throw std::invalid_argument(
                    "conditional P3 reach budget timestamps must be strictly increasing"
                );
            }
            for (std::size_t column = 0;
                 column < p3_reach_budget_selected_k.cols;
                 ++column) {
                const auto selected_k = p3_reach_budget_selected_k(row, column);
                if (selected_k > 16 && selected_k != kP3ReachBudgetUnsupported) {
                    throw std::invalid_argument(
                        "conditional P3 reach budget selected-k must be 0..16 or 255"
                    );
                }
            }
        }
    }
    const bool has_fair_center =
        !fair_center_ts_ms.empty() || !fair_center_price.empty() ||
        !fair_center_gain.empty() || !fair_center_valid.empty();
    if (has_fair_center) {
        if (fair_center_ts_ms.empty()) {
            throw std::invalid_argument(
                "cross-venue fair center requires non-empty timestamps"
            );
        }
        require_same_size(
            fair_center_ts_ms.size(), fair_center_price.size(),
            "fair_center_price"
        );
        require_same_size(
            fair_center_ts_ms.size(), fair_center_gain.size(),
            "fair_center_gain"
        );
        require_same_size(
            fair_center_ts_ms.size(), fair_center_valid.size(),
            "fair_center_valid"
        );
        for (std::size_t row = 0; row < fair_center_ts_ms.size(); ++row) {
            if (row > 0 &&
                fair_center_ts_ms.data()[row] <=
                    fair_center_ts_ms.data()[row - 1]) {
                throw std::invalid_argument(
                    "cross-venue fair-center timestamps must be strictly increasing"
                );
            }
            const auto valid = fair_center_valid.data()[row];
            if (valid > 1) {
                throw std::invalid_argument(
                    "cross-venue fair-center valid flag must lie in {0,1}"
                );
            }
            if (valid == 1 &&
                (!std::isfinite(fair_center_price.data()[row]) ||
                 fair_center_price.data()[row] <= 0.0 ||
                 !std::isfinite(fair_center_gain.data()[row]) ||
                 fair_center_gain.data()[row] < 0.0 ||
                 fair_center_gain.data()[row] > 1.0)) {
                throw std::invalid_argument(
                    "valid cross-venue fair-center rows require positive price and gain in [0,1]"
                );
            }
        }
    }
    if (bbo_ts_ms.size() > 0) {
        require_same_size(bbo_ts_ms.size(), bbo_best_bid.size(), "bbo_best_bid");
        require_same_size(bbo_ts_ms.size(), bbo_best_ask.size(), "bbo_best_ask");
        if (bbo_bid_qty.size() > 0) {
            require_same_size(bbo_ts_ms.size(), bbo_bid_qty.size(), "bbo_bid_qty");
        }
        if (bbo_ask_qty.size() > 0) {
            require_same_size(bbo_ts_ms.size(), bbo_ask_qty.size(), "bbo_ask_qty");
        }
    }
    if (l2_ts_ms.size() > 0) {
        if (l2_bid_px.rows != l2_ts_ms.size() || l2_bid_qty.rows != l2_ts_ms.size() ||
            l2_ask_px.rows != l2_ts_ms.size() || l2_ask_qty.rows != l2_ts_ms.size()) {
            throw std::invalid_argument("L2 matrix row count must match l2_ts_ms");
        }
        if (l2_bid_px.cols != l2_bid_qty.cols || l2_ask_px.cols != l2_ask_qty.cols ||
            l2_bid_px.cols != l2_ask_px.cols) {
            throw std::invalid_argument("L2 matrix level count mismatch");
        }
    }
    if (queue_base_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), queue_base_by_trade.size(), "queue_base_by_trade");
    }
    if (queue_decay_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), queue_decay_by_trade.size(), "queue_decay_by_trade");
    }
    if (buy_fill_prob_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), buy_fill_prob_by_trade.size(), "buy_fill_prob_by_trade");
    }
    if (sell_fill_prob_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), sell_fill_prob_by_trade.size(), "sell_fill_prob_by_trade");
    }
    if (buy_queue_deplete_mult_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), buy_queue_deplete_mult_by_trade.size(), "buy_queue_deplete_mult_by_trade");
    }
    if (sell_queue_deplete_mult_by_trade.size() > 0) {
        require_same_size(trade_ts_ms.size(), sell_queue_deplete_mult_by_trade.size(), "sell_queue_deplete_mult_by_trade");
    }
}

TickReplayResult simulate_tick_arrays(
    const TickReplayInput& input,
    const TickReplayParams& params
) {
    input.validate();
    for (const double limit : {params.max_daily_loss, params.max_position_value,
                               params.emergency_close_dd}) {
        // +infinity is the native ABI's explicit disabled sentinel.
        if (std::isnan(limit) || limit <= 0.0) {
            throw std::invalid_argument("hard-risk limits must be positive or disabled");
        }
    }
    if (params.initial_risk_state_enabled) {
        for (const double value : {params.initial_risk_day_start_total_pnl,
                params.initial_risk_session_peak_pnl, params.initial_risk_last_total_pnl,
                params.initial_risk_total_pnl_offset}) {
            if (!std::isfinite(value)) {
                throw std::invalid_argument("initial risk state must be finite");
            }
        }
        if (!input.trade_ts_ms.empty() && params.initial_risk_utc_day >
                input.trade_ts_ms.data()[0] / 86'400'000) {
            throw std::invalid_argument("initial risk state is from a future UTC day");
        }
    }
    for (const auto sample : params.decision_to_gateway_latency_samples_ms) {
        if (!std::isfinite(sample) || sample < 0.0) {
            throw std::invalid_argument(
                "decision-to-gateway latency samples must be finite and non-negative"
            );
        }
    }
    if (!params.new_order_exchange_effective_latency_samples_ms.empty() &&
        !params.new_order_latency_samples_ms.empty() &&
        params.new_order_exchange_effective_latency_samples_ms.size() !=
            params.new_order_latency_samples_ms.size()) {
        throw std::invalid_argument(
            "paired new-order effective/ACK latency samples must have equal length"
        );
    }
    if (!params.cancel_exchange_effective_latency_samples_ms.empty() &&
        !params.cancel_ack_visibility_latency_samples_ms.empty() &&
        params.cancel_exchange_effective_latency_samples_ms.size() !=
            params.cancel_ack_visibility_latency_samples_ms.size()) {
        throw std::invalid_argument(
            "paired cancel effective/ACK latency samples must have equal length"
        );
    }
    const auto& f05_cooldown_window_tape =
        params.f05_cooldown_window_tape_shared
            ? params.f05_cooldown_window_tape_shared->observations
            : params.f05_cooldown_window_tape;
    if (params.trace_cooldown_duration_opportunities_max < 0) {
        throw std::invalid_argument(
            "trace_cooldown_duration_opportunities_max must be non-negative"
        );
    }
    if (params.cooldown_duration_fork_enabled) {
        if (params.cooldown_duration_fork_action != "CONTROL_85N" &&
            params.cooldown_duration_fork_action != "FIXED_DURATION_MS") {
            throw std::invalid_argument(
                "cooldown duration fork action must be CONTROL_85N or "
                "FIXED_DURATION_MS"
            );
        }
        if (params.cooldown_duration_fork_target_ordinal <= 0 ||
            params.cooldown_duration_fork_target_ts_ms <= 0 ||
            (params.cooldown_duration_fork_target_side != "BUY" &&
             params.cooldown_duration_fork_target_side != "SELL") ||
            params.cooldown_duration_fork_target_order_id < 0 ||
            params.cooldown_duration_fork_target_campaign_id <= 0 ||
            !std::isfinite(
                params.cooldown_duration_fork_expected_baseline_ms) ||
            params.cooldown_duration_fork_expected_baseline_ms <= 0.0) {
            throw std::invalid_argument(
                "cooldown duration fork target identity is incomplete"
            );
        }
        if (params.cooldown_duration_fork_action == "FIXED_DURATION_MS" &&
            (!std::isfinite(params.cooldown_duration_fork_fixed_ms) ||
             params.cooldown_duration_fork_fixed_ms <= 0.0)) {
            throw std::invalid_argument(
                "fixed cooldown duration must be positive and finite"
            );
        }
        if (params.buy_soft_widen_release_probe_enabled ||
            params.conditional_p3_reach_gate_enabled ||
            params.conditional_p3_reach_budget_policy_enabled ||
            params.buy_fill_selection_live_enabled) {
            throw std::invalid_argument(
                "cooldown duration fork cannot share a replay with another "
                "research action"
            );
        }
    }
    if (params.f05_repeated_cooldown_runtime) {
        const auto& runtime = *params.f05_repeated_cooldown_runtime;
        const auto& config = runtime.config();
        if (params.cooldown_duration_fork_enabled) {
            if (!params.cooldown_duration_fork_baseline_policy_enabled ||
                params.cooldown_duration_fork_expected_owner_action.empty() ||
                !is_lower_sha256(
                    params.cooldown_duration_fork_expected_owner_policy_sha256) ||
                params.cooldown_duration_fork_expected_owner_policy_sha256 !=
                    config.policy.policy_sha256) {
                throw std::invalid_argument(
                    "F05 one-shot plus repeated cooldown requires an exact-owner "
                    "baseline binding"
                );
            }
        }
        if (!runtime.execution_admitted() ||
            (config.qualification_scope != "synthetic_full_replay_smoke" &&
             config.qualification_scope !=
                 "real_day_all_arm_full_replay_v21" &&
             config.qualification_scope !=
                 "real_day_all_arm_full_replay_v22" &&
             config.qualification_scope !=
                 "real_day_all_arm_full_replay_v23" &&
             config.qualification_scope !=
                 "current_receive_time_full_replay_v1")) {
            throw std::invalid_argument(
                "F05 repeated cooldown requires a full-replay-admitted runtime"
            );
        }
        if (std::abs(params.fill_cooldown_s - 85.0) > 1e-12 ||
            params.adaptive_add_cooldown_enabled ||
            params.fill_cooldown_apply_reducing ||
            std::abs(params.fill_cooldown_reducing_s) > 1e-12 ||
            params.fill_cooldown_consecutive_reset_policy !=
                "opposite_fill_only" ||
            params.fill_cooldown_reset_consec_on_expiry) {
            throw std::invalid_argument(
                "F05 repeated cooldown requires the exact CONTROL_85N baseline"
            );
        }
        if (params.buy_soft_widen_release_probe_enabled ||
            params.conditional_p3_reach_gate_enabled ||
            params.conditional_p3_reach_budget_policy_enabled ||
            params.buy_fill_selection_live_enabled) {
            throw std::invalid_argument(
                "F05 repeated cooldown cannot share a replay with another research action"
            );
        }
        if (f05_cooldown_window_tape.empty()) {
            throw std::invalid_argument(
                "F05 repeated cooldown full replay requires a causal window tape"
            );
        }
        validate_f05_cooldown_predicate_rows(
            config,
            params.f05_cooldown_predicate_rows);
    } else if (!f05_cooldown_window_tape.empty() ||
               !params.f05_cooldown_predicate_rows.empty()) {
        throw std::invalid_argument(
            "F05 cooldown tapes were supplied without a bound runtime"
        );
    }
    if (params.conditional_p3_reach_gate_enabled &&
        params.conditional_p3_reach_budget_policy_enabled) {
        throw std::invalid_argument(
            "fixed conditional-P3 reach gate and adaptive reach budget are mutually exclusive"
        );
    }
    if (params.conditional_p3_reach_gate_enabled) {
        if (input.p3_reach_gate_status.empty()) {
            throw std::invalid_argument(
                "conditional P3 reach gate enabled without a frozen status matrix"
            );
        }
        if (params.conditional_p3_reach_gate_outward_ticks <= 0 ||
            params.conditional_p3_reach_gate_grid_min_ticks <= 0) {
            throw std::invalid_argument(
                "conditional P3 reach gate tick contract is invalid"
            );
        }
        const auto valid_threshold = [](double value) {
            return std::isfinite(value) && value >= 0.0 && value <= 1.0;
        };
        if (!valid_threshold(params.conditional_p3_reach_gate_buy_toxicity_threshold) ||
            !valid_threshold(params.conditional_p3_reach_gate_sell_toxicity_threshold)) {
            throw std::invalid_argument(
                "conditional P3 reach gate toxicity thresholds must lie in [0,1]"
            );
        }
    }
    if (params.conditional_p3_reach_budget_policy_enabled) {
        if (input.p3_reach_budget_selected_k.empty()) {
            throw std::invalid_argument(
                "conditional P3 reach budget enabled without a frozen selected-k matrix"
            );
        }
        if (params.conditional_p3_reach_budget_grid_min_ticks !=
            kP3ReachBudgetGridMinTicks) {
            throw std::invalid_argument(
                "conditional P3 reach budget grid_min_ticks must equal 5"
            );
        }
        const auto valid_threshold = [](double value) {
            return std::isfinite(value) && value >= 0.0 && value <= 1.0;
        };
        if (!valid_threshold(
                params.conditional_p3_reach_budget_buy_toxicity_threshold) ||
            !valid_threshold(
                params.conditional_p3_reach_budget_sell_toxicity_threshold)) {
            throw std::invalid_argument(
                "conditional P3 reach budget toxicity thresholds must lie in [0,1]"
            );
        }
    }
    if (params.cross_venue_fair_center_shift_enabled) {
        if (input.fair_center_ts_ms.empty()) {
            throw std::invalid_argument(
                "cross-venue fair-center action enabled without a causal tape"
            );
        }
        if (!std::isfinite(params.cross_venue_fair_center_max_state_age_ms) ||
            params.cross_venue_fair_center_max_state_age_ms <= 0.0) {
            throw std::invalid_argument(
                "cross-venue fair-center max state age must be positive and finite"
            );
        }
    }
    if (params.latency_sampler_version != "keyed_splitmix64_v1") {
        throw std::invalid_argument(
            "C++ replay requires latency_sampler_version=keyed_splitmix64_v1"
        );
    }
    if (params.max_consecutive_losses < 0 ||
        !std::isfinite(params.cooldown_after_loss_s) ||
        params.cooldown_after_loss_s < 0.0) {
        throw std::invalid_argument(
            "C++ replay loss-cooldown config must be finite and non-negative"
        );
    }
    if ((params.max_consecutive_losses > 0 ||
         params.cooldown_after_loss_s > 0.0) &&
        params.consecutive_loss_cooldown_semantics != kLossCooldownSemantics) {
        throw std::invalid_argument(
            "C++ replay requires the frozen consecutive-loss semantics"
        );
    }
    if (params.consecutive_loss_snapshot_enabled) {
        if (params.consecutive_loss_cooldown_semantics !=
            kLossCooldownSemantics) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown snapshot semantics are stale"
            );
        }
        if (params.consecutive_loss_snapshot_schema !=
            kLossCooldownSnapshotSchema) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown snapshot schema is stale"
            );
        }
        if (!std::isfinite(params.initial_inventory) ||
            !std::isfinite(params.initial_entry_price) ||
            !std::isfinite(params.initial_loss_open_commission) ||
            !std::isfinite(params.initial_loss_round_trip_pnl) ||
            params.initial_loss_consecutive_losses < 0 ||
            params.initial_loss_cooldown_until_ms < 0 ||
            params.initial_loss_last_cancel_ts_ms < -1 ||
            params.initial_loss_trigger_count < 0 ||
            params.initial_loss_expiry_count < 0 ||
            params.initial_loss_losing_round_trips < 0 ||
            params.initial_loss_winning_or_flat_round_trips < 0 ||
            params.initial_loss_max_observed_consecutive_losses <
                params.initial_loss_consecutive_losses) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown snapshot fields are invalid"
            );
        }
        if ((std::abs(params.initial_inventory) <= 1e-10 &&
             std::abs(params.initial_entry_price) > 1e-10) ||
            (std::abs(params.initial_inventory) > 1e-10 &&
             params.initial_entry_price <= 0.0)) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown inventory/entry is inconsistent"
            );
        }
        if (std::abs(params.initial_inventory) <= 1e-10 &&
            (std::abs(params.initial_loss_open_commission) > 1e-10 ||
             std::abs(params.initial_loss_round_trip_pnl) > 1e-10)) {
            throw std::invalid_argument(
                "flat C++ loss-cooldown snapshot retained open economics"
            );
        }
        const bool snapshot_enabled = params.max_consecutive_losses > 0 &&
            params.cooldown_after_loss_s > 0.0;
        const bool threshold_reached = snapshot_enabled &&
            params.initial_loss_consecutive_losses >=
                params.max_consecutive_losses;
        const bool active_clock =
            params.initial_loss_cooldown_until_ms > 0;
        if ((active_clock &&
             (params.initial_loss_trigger_count <= 0 ||
              params.initial_loss_trigger_count - 1 !=
                  params.initial_loss_expiry_count)) ||
            (!active_clock &&
             params.initial_loss_trigger_count !=
                 params.initial_loss_expiry_count) ||
            params.initial_loss_trigger_count >
                params.initial_loss_losing_round_trips ||
            params.initial_loss_max_observed_consecutive_losses >
                params.initial_loss_losing_round_trips) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown trigger/expiry history is inconsistent"
            );
        }
        if (!snapshot_enabled &&
            (params.initial_loss_consecutive_losses != 0 ||
             active_clock ||
             params.initial_loss_last_cancel_ts_ms != -1 ||
             params.initial_loss_threshold_pending ||
             params.initial_loss_max_observed_consecutive_losses != 0 ||
             params.initial_loss_trigger_count != 0 ||
             params.initial_loss_expiry_count != 0)) {
            throw std::invalid_argument(
                "disabled C++ loss-cooldown snapshot retained policy state"
            );
        }
        if (!active_clock &&
            params.initial_loss_threshold_pending != threshold_reached) {
            throw std::invalid_argument(
                "C++ replay loss-cooldown pending threshold is inconsistent"
            );
        }
    } else if (
        !params.consecutive_loss_snapshot_schema.empty() ||
        std::abs(params.initial_loss_open_commission) > 1e-10 ||
        std::abs(params.initial_loss_round_trip_pnl) > 1e-10 ||
        params.initial_loss_consecutive_losses != 0 ||
        params.initial_loss_cooldown_until_ms != 0 ||
        params.initial_loss_last_cancel_ts_ms != -1 ||
        params.initial_loss_threshold_pending ||
        params.initial_loss_trigger_count != 0 ||
        params.initial_loss_expiry_count != 0 ||
        params.initial_loss_losing_round_trips != 0 ||
        params.initial_loss_winning_or_flat_round_trips != 0 ||
        params.initial_loss_max_observed_consecutive_losses != 0) {
        throw std::invalid_argument(
            "C++ replay loss-cooldown snapshot state supplied while disabled"
        );
    }
    if (params.sync_adjust_replay_mode != "disabled" &&
        params.sync_adjust_replay_mode != "frozen_tape" &&
        params.sync_adjust_replay_mode != "censor" &&
        params.sync_adjust_replay_mode != "stress") {
        throw std::invalid_argument("invalid sync_adjust_replay_mode");
    }
    if (params.sync_adjust_replay_mode != "disabled" &&
        params.sync_adjust_semantics != kSyncAdjustSemantics) {
        throw std::invalid_argument(
            "C++ replay requires the frozen sync-adjust semantics"
        );
    }
    if (params.fixed_spread_probe_enabled) {
        throw std::invalid_argument(
            "scalar fixed-spread probe was retired after it violated paired "
            "distance monotonicity; use paired_fixed_spread_probe_enabled"
        );
    }
    if (params.paired_fixed_spread_probe_enabled) {
        if (params.fixed_spread_probe_enabled) {
            throw std::invalid_argument(
                "paired and scalar fixed-spread probes are mutually exclusive"
            );
        }
        if (input.bbo_ts_ms.empty() && input.l2_ts_ms.empty()) {
            throw std::invalid_argument(
                "paired fixed-spread probe requires historical BBO or L2"
            );
        }
    }
    if (params.buy_fill_selection_live_enabled &&
        !params.buy_fill_selection_models.empty()) {
        const bool requires_static = std::any_of(
            params.buy_fill_selection_models.begin(),
            params.buy_fill_selection_models.end(),
            buy_fill_model_requires_static_payload
        );
        const bool has_static =
            !input.buy_fill_static_logit_delta.empty() &&
            !input.buy_fill_static_missing.empty() &&
            !input.buy_fill_static_used.empty();
        if (requires_static && !has_static) {
            throw std::invalid_argument(
                "BUY fill-selection model contains Prediction.feature_dict features; "
                "simulate_tick_arrays_ext_policy_v3 static payload is required"
            );
        }
        if (has_static &&
            input.buy_fill_static_logit_delta.cols !=
                params.buy_fill_selection_models.size()) {
            throw std::invalid_argument(
                "BUY fill-selection static matrix fold count must match model count"
            );
        }
    }
    if (params.buy_soft_widen_release_probe_enabled) {
        if (params.buy_soft_widen_release_target_ts_ms <= 0) {
            throw std::invalid_argument(
                "BUY soft-widen release probe requires a positive target timestamp"
            );
        }
        if (params.buy_soft_widen_release_target_role != "opener" &&
            params.buy_soft_widen_release_target_role != "add") {
            throw std::invalid_argument(
                "BUY soft-widen release target role must be opener or add"
            );
        }
        if (std::abs(params.buy_soft_widen_release_spread_mult_cap - 1.0) > 1e-12) {
            throw std::invalid_argument(
                "BUY soft-widen release v1 freezes spread_mult_cap=1.0"
            );
        }
        if (params.buy_fill_selection_live_enabled) {
            throw std::invalid_argument(
                "BUY soft-widen release probe and legacy BUY fill selector are mutually exclusive"
            );
        }
        if (params.conditional_p3_reach_gate_enabled) {
            throw std::invalid_argument(
                "BUY soft-widen release probe and conditional-P3 action are mutually exclusive"
            );
        }
        if (params.conditional_p3_reach_budget_policy_enabled) {
            throw std::invalid_argument(
                "BUY soft-widen release probe and conditional-P3 reach budget are mutually exclusive"
            );
        }
    }
    if (params.queue_l2_cancel_ahead_enabled) {
        if (input.l2_ts_ms.empty() || input.l2_bid_px.empty() || input.l2_ask_px.empty()) {
            throw std::invalid_argument(
                "queue_l2_cancel_ahead_enabled requires historical L2"
            );
        }
    }

    TickReplayResult result;
    std::optional<F05RepeatedBooleanCooldownRuntime> f05_cooldown_runtime;
    if (params.f05_repeated_cooldown_runtime) {
        f05_cooldown_runtime.emplace(*params.f05_repeated_cooldown_runtime);
        result.f05_repeated_cooldown_decisions.reserve(
            params.f05_cooldown_predicate_rows.empty()
                ? std::min<std::size_t>(input.trade_ts_ms.size(), 1'024)
                : params.f05_cooldown_predicate_rows.size()
        );
    }
    std::size_t f05_cooldown_window_cursor = 0;
    std::size_t f05_cooldown_predicate_cursor = 0;
    if (params.trace_quotes_max > 0) {
        result.quote_trace.reserve(std::min<std::size_t>(
            static_cast<std::size_t>(params.trace_quotes_max), input.trade_ts_ms.size() * 2));
    }
    if (params.trace_fills_max > 0) {
        result.fill_trace.reserve(std::min<std::size_t>(
            static_cast<std::size_t>(params.trace_fills_max), input.trade_ts_ms.size()));
    }
    if (params.trace_cooldown_duration_opportunities_max > 0) {
        result.cooldown_duration_opportunity_trace.reserve(
            std::min<std::size_t>(
                static_cast<std::size_t>(
                    params.trace_cooldown_duration_opportunities_max),
                input.trade_ts_ms.size()
            )
        );
    }
    std::array<std::byte, 64 * 1024> replay_scratch{};
    // replay_resource 管理单次 replay 内的短生命周期对象；不要从 result 暴露指向该资源的指针。
    std::pmr::monotonic_buffer_resource replay_resource(
        replay_scratch.data(), replay_scratch.size());
    std::pmr::unsynchronized_pool_resource trace_resource{&replay_resource};
    std::pmr::polymorphic_allocator<TraceOrderRow> trace_allocator{&trace_resource};
    const bool trace_enabled =
        params.trace_quotes_max > 0 ||
        params.trace_fills_max > 0 ||
        params.trace_cooldown_duration_opportunities_max > 0 ||
        params.cooldown_duration_fork_enabled;
    auto& summary = result.summary;
    std::optional<NativeBookTapeRuntime> native_book;
    if (params.native_exchange_book_enabled) {
        native_book.emplace(params);
    }
    const bool paired_fixed_spread_probe =
        params.paired_fixed_spread_probe_enabled;
    const bool fixed_spread_probe =
        params.fixed_spread_probe_enabled || paired_fixed_spread_probe;
    const double lot_size = std::max(std::abs(params.quote.lot_size), 1e-12);
    const double tick_size = std::max(std::abs(params.quote.tick_size), 1e-12);
    const double order_size = std::max(floor_lot(params.order_size, lot_size), lot_size);
    PairedFixedSpreadProbe paired_probe(
        input,
        params,
        result,
        tick_size,
        lot_size,
        order_size
    );
    const std::int64_t requote_ms = std::max<std::int64_t>(
        1,
        static_cast<std::int64_t>(std::llround(params.requote_interval_s * 1000.0))
    );
    const std::int64_t rq_min_ms = params.rq_min_s > 0.0
        ? std::max<std::int64_t>(1, static_cast<std::int64_t>(std::llround(params.rq_min_s * 1000.0)))
        : requote_ms;
    const std::int64_t rq_max_ms = params.rq_max_s > 0.0
        ? std::max<std::int64_t>(rq_min_ms, static_cast<std::int64_t>(std::llround(params.rq_max_s * 1000.0)))
        : requote_ms;
    const bool dynamic_rq = rq_min_ms < rq_max_ms && input.var_retsq.size() == input.var_ts_ms.size();
    if (params.collect_curves) {
        const std::int64_t elapsed_ms = std::max<std::int64_t>(
            0, input.trade_ts_ms.back() - input.trade_ts_ms.front());
        const std::int64_t sample_interval_ms = dynamic_rq ? rq_min_ms : requote_ms;
        const std::size_t expected_samples = std::min<std::size_t>(
            input.trade_ts_ms.size(),
            static_cast<std::size_t>(elapsed_ms / std::max<std::int64_t>(1, sample_interval_ms) + 2));
        result.pnl_ts_ms.reserve(expected_samples);
        result.pnl.reserve(expected_samples);
        result.inventory.reserve(expected_samples);
    }
    const std::int64_t fill_cooldown_ms = std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(params.fill_cooldown_s * 1000.0))
    );
    bool fill_cooldown_reset_consec_on_expiry =
        params.fill_cooldown_reset_consec_on_expiry;
    if (!params.fill_cooldown_consecutive_reset_policy.empty()) {
        if (params.fill_cooldown_consecutive_reset_policy == "opposite_fill_only") {
            fill_cooldown_reset_consec_on_expiry = false;
        } else if (
            params.fill_cooldown_consecutive_reset_policy ==
            "opposite_fill_or_expiry"
        ) {
            fill_cooldown_reset_consec_on_expiry = true;
        } else {
            throw std::invalid_argument(
                "invalid fill_cooldown_consecutive_reset_policy"
            );
        }
    }
    const std::int64_t fill_cooldown_reducing_ms = std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(params.fill_cooldown_reducing_s * 1000.0))
    );
    const std::int64_t flat_unilateral_max_ms = std::max<std::int64_t>(
        0,
        static_cast<std::int64_t>(std::llround(params.flat_unilateral_max_s * 1000.0))
    );
    const bool markout_enabled = params.markout_ema_span_fills > 0;
    const double mo_alpha = markout_enabled
        ? 2.0 / (static_cast<double>(params.markout_ema_span_fills) + 1.0)
        : 0.0;
    const std::int64_t markout_horizon_ms = std::max<std::int64_t>(
        1,
        static_cast<std::int64_t>(std::llround(std::max(1e-6, params.markout_horizon_s) * 1000.0))
    );
    constexpr double mo_ref = 50.0;
    double adverse_markout_pause_threshold = std::abs(params.quote.adverse_markout_pause_threshold);
    if (adverse_markout_pause_threshold <= 0.0) {
        adverse_markout_pause_threshold = std::abs(params.quote.adverse_markout_threshold);
    }
    adverse_markout_pause_threshold = std::max(adverse_markout_pause_threshold, 1e-9);
    const std::int64_t adverse_markout_max_resolve_gap_ms = std::max<std::int64_t>(
        markout_horizon_ms,
        static_cast<std::int64_t>(std::llround(
            std::max(params.markout_horizon_s, params.adverse_markout_max_resolve_gap_s) * 1000.0
        ))
    );

    std::mt19937_64 rng(static_cast<std::uint64_t>(params.rng_seed));
    ReplayOrders bid_orders{&replay_resource};
    ReplayOrders ask_orders{&replay_resource};

    double inventory = params.initial_inventory;
    double entry_price = std::abs(params.initial_inventory) > 1e-10 ? params.initial_entry_price : 0.0;
    if (std::abs(inventory) > 1e-10 && entry_price <= 0.0 &&
        params.max_consecutive_losses > 0 &&
        params.cooldown_after_loss_s > 0.0) {
        throw std::invalid_argument(
            "non-flat consecutive-loss state requires initial_entry_price"
        );
    }
    ConsecutiveLossCooldownState loss_cooldown{
        params.max_consecutive_losses,
        static_cast<std::int64_t>(
            std::llround(params.cooldown_after_loss_s * 1000.0)
        ),
        inventory,
        entry_price,
    };
    if (params.consecutive_loss_snapshot_enabled) {
        loss_cooldown.open_commission =
            params.initial_loss_open_commission;
        loss_cooldown.round_trip_pnl =
            params.initial_loss_round_trip_pnl;
        loss_cooldown.consecutive_losses =
            params.initial_loss_consecutive_losses;
        loss_cooldown.cooldown_until_ms =
            params.initial_loss_cooldown_until_ms;
        loss_cooldown.last_cancel_ts_ms =
            params.initial_loss_last_cancel_ts_ms;
        loss_cooldown.threshold_pending =
            params.initial_loss_threshold_pending;
        loss_cooldown.trigger_count =
            params.initial_loss_trigger_count;
        loss_cooldown.expiry_count =
            params.initial_loss_expiry_count;
        loss_cooldown.losing_round_trips =
            params.initial_loss_losing_round_trips;
        loss_cooldown.winning_or_flat_round_trips =
            params.initial_loss_winning_or_flat_round_trips;
        loss_cooldown.max_observed_consecutive_losses =
            params.initial_loss_max_observed_consecutive_losses;
    }
    std::int64_t sync_adjust_degrade_until_ms = 0;
    const auto record_loss_fill = [&](bool buy,
                                      double quantity,
                                      double fill_price,
                                      double fee_rate,
                                      double expected_inventory) {
        const double commission = fill_price * quantity * fee_rate;
        if (buy) {
            loss_cooldown.on_fill<Side::Buy>(
                quantity, fill_price, commission
            );
        } else {
            loss_cooldown.on_fill<Side::Sell>(
                quantity, fill_price, commission
            );
        }
        if (std::abs(loss_cooldown.inventory - expected_inventory) >
            std::max(1e-10, lot_size * 1e-7)) {
            throw std::runtime_error(
                "consecutive-loss replay inventory diverged from execution state"
            );
        }
    };
    std::int64_t pos_open_ts = input.trade_ts_ms.data()[0];
    bool circuit_breaker_closing = false;
    std::int64_t circuit_breaker_close_start_ts = 0;
    std::int64_t circuit_breaker_close_gtx_reject_streak = 0;
    double cash = params.initial_entry_price > 0.0
        ? -params.initial_inventory * params.initial_entry_price
        : 0.0;
    bool risk_emergency_latched = false;
    bool risk_emergency_submit_sent = false;
    const bool hard_risk_enabled = std::isfinite(params.max_daily_loss)
        || std::isfinite(params.max_position_value) || std::isfinite(params.emergency_close_dd);
    std::int64_t risk_utc_day = params.initial_risk_state_enabled
        ? params.initial_risk_utc_day : input.trade_ts_ms.data()[0] / 86'400'000;
    double risk_day_start = params.initial_risk_day_start_total_pnl;
    double risk_peak = params.initial_risk_session_peak_pnl;
    double risk_last = params.initial_risk_last_total_pnl;
    const double risk_offset = params.initial_risk_total_pnl_offset;
    const auto observe_risk = [&](std::int64_t now, double mark) {
        const auto day = now / 86'400'000;
        if (day > risk_utc_day) {
            risk_utc_day = day;
            risk_day_start = risk_last;
        }
        risk_last = cash + inventory * mark + risk_offset;
        risk_peak = std::max(risk_peak, risk_last);
    };
    std::int64_t hard_risk_clock_ts = input.trade_ts_ms.data()[0];
    double hard_risk_last_mark = input.trade_price.data()[0];
    std::function<void()> observe_fill_risk;
    if (hard_risk_enabled) {
        observe_fill_risk = [&]() {
            observe_risk(hard_risk_clock_ts, hard_risk_last_mark);
        };
    }
    const auto cap_notional_qty = [&](bool buy, double quantity, double mark) {
        if (!std::isfinite(params.max_position_value)) return quantity;
        if (mark <= 0.0 || quantity <= 0.0) return 0.0;
        const double room = std::max(0.0, params.max_position_value / mark
            + (buy ? -inventory : inventory));
        return std::min(quantity, std::floor(room / lot_size + 1e-12) * lot_size);
    };
    bool planned_shutdown_requested = false;
    double quote_mid_state = input.trade_price.data()[0];
    double inferred_best_bid = input.trade_price.data()[0] - tick_size;
    double inferred_best_ask = input.trade_price.data()[0] + tick_size;
    double sigma_sq = std::max(params.initial_sigma_sq, 1e-6);
    QuotePrediction pred;

    std::ptrdiff_t var_idx = -1;
    std::size_t ml_idx = 0;
    bool ml_ready = false;
    std::size_t last_p3_trace_ml_idx_buy = std::numeric_limits<std::size_t>::max();
    std::size_t last_p3_trace_ml_idx_sell = std::numeric_limits<std::size_t>::max();
    P3ReachBudgetEpisode p3_reach_budget_buy_episode;
    P3ReachBudgetEpisode p3_reach_budget_sell_episode;
    const auto update_p3_reach_budget_lifecycle = [&](std::int64_t now_ms) {
        if (!params.conditional_p3_reach_budget_policy_enabled) {
            return;
        }
        const bool inventory_flat = std::abs(inventory) <= 1e-10;
        const auto update_one = [&](P3ReachBudgetEpisode& episode) {
            if (!episode.active) {
                return;
            }
            if (now_ms >= episode.active_bucket_ms + kP3ReachBudgetBucketMs) {
                episode.deactivate();
                ++summary.p3_reach_budget_bucket_expiry_count;
                return;
            }
            if (!inventory_flat) {
                episode.saw_nonflat_inventory = true;
                return;
            }
            if (episode.saw_nonflat_inventory) {
                episode.deactivate();
                ++summary.p3_reach_budget_flat_reset_count;
            }
        };
        update_one(p3_reach_budget_buy_episode);
        update_one(p3_reach_budget_sell_episode);
    };
    std::size_t p3_idx = 0;
    bool p3_ready = false;
    std::size_t fair_center_idx = 0;
    std::size_t bbo_idx = 0;
    std::size_t l2_idx = 0;
    std::int64_t current_rq_ms = requote_ms;
    std::int64_t last_requote_ts = input.trade_ts_ms.data()[0] - current_rq_ms;
    const bool exec_book_visibility_delay_enabled =
        !params.use_bar_pricing &&
        (
            !params.exec_book_visibility_delay_samples_ms.empty() ||
            params.exec_book_visibility_delay_mean_ms > 0.0 ||
            params.exec_book_visibility_delay_jitter_ms > 0.0
        );
    // Python tracks receive-time visibility independently by feed.  Markout
    // observations consume partial depth without advancing bookTicker, while a
    // later quote decision advances both monotonic cutoffs.  A shared cutoff
    // lets a markout reveal a BBO message that was never visible to live.
    std::int64_t last_visible_bbo_ts = std::numeric_limits<std::int64_t>::min();
    std::int64_t last_visible_l2_ts = std::numeric_limits<std::int64_t>::min();
    std::int64_t next_trace_order_id = 0;
    double rq_sum = 0.0;
    double ema_var_fast = 0.0;
    double ema_var_slow = 0.0;
    bool dyn_rq_inited = false;
    double ema_ti_fast = 0.0;
    double ema_ti_slow = 0.0;
    bool ber_inited = false;
    bool ber_active = false;
    double ber_held_ti = 50.0;
    double ber_pending_ti = 50.0;
    std::int64_t ber_pending_feature_ts_ms = -1;
    std::int64_t ber_published_feature_ts_ms = -1;
    double cur_ti = params.quote.liq_baseline;
    // Input var_ti is mean aggregate count per 1s bar. The live quote
    // consumer holds mean count per completed 10s bar between publications.
    std::vector<std::size_t> quote_ti_source_indices;
    if (input.var_ti.size() == input.var_ts_ms.size()) {
        for (std::size_t idx = 0; idx < input.var_ts_ms.size(); ++idx) {
            if ((input.var_ts_ms.data()[idx] + 1000) % 10'000 == 0) {
                quote_ti_source_indices.push_back(idx);
            }
        }
    }
    std::size_t quote_ti_cursor = 0;
    double consecutive_buy_fills = 0.0;
    double consecutive_sell_fills = 0.0;
    std::int64_t last_buy_fill_ts = input.trade_ts_ms.data()[0] - 999999999;
    std::int64_t last_sell_fill_ts = input.trade_ts_ms.data()[0] - 999999999;
    double last_buy_fill_cooldown_ms = 0.0;
    double last_sell_fill_cooldown_ms = 0.0;
    std::int64_t flat_unilateral_bid_started_ms = 0;
    std::int64_t flat_unilateral_ask_started_ms = 0;
    PendingMarkouts pending_markouts{&replay_resource};
    double mo_ema_bid = 0.0;
    double mo_ema_ask = 0.0;
    double mo_ema_all = 0.0;
    std::int64_t mo_last_decay_ts = input.trade_ts_ms.data()[0];
    std::int64_t mo_pause_until_bid_ms = 0;
    std::int64_t mo_pause_until_ask_ms = 0;
    double mo_sum_bid = 0.0;
    double mo_sum_ask = 0.0;
    double mo_sum_all = 0.0;
    double mo_sum_final_compressed = 0.0;
    double mo_sum_not_final_compressed = 0.0;
    std::int64_t mo_count_all = 0;
    double mo_qty_bid = 0.0;
    double mo_qty_ask = 0.0;
    double mo_qty_all = 0.0;
    double mo_qty_final_compressed = 0.0;
    double mo_qty_not_final_compressed = 0.0;
    const double ret_demean_alpha = params.ret_demean_halflife > 0
        ? 2.0 / (static_cast<double>(params.ret_demean_halflife) + 1.0)
        : 0.0;
    double pred_ret_ema = 0.0;
    double spread_sum = 0.0;
    double final_spread_sum = 0.0;
    std::int64_t final_spread_count = 0;
    double max_abs_inventory = std::abs(inventory);
    std::int64_t fills_bid_final_compressed = 0;
    std::int64_t fills_ask_final_compressed = 0;
    bool pnl_stats_initialized = false;
    std::int64_t previous_pnl_ts = 0;
    double previous_pnl = 0.0;
    double peak_pnl = -std::numeric_limits<double>::infinity();
    double max_drawdown = 0.0;
    double pnl_delta_sum = 0.0;
    double pnl_dt_sum = 0.0;
    double normalized_delta_sum = 0.0;
    double normalized_delta_sq_sum = 0.0;
    std::size_t normalized_delta_count = 0;

    auto decay_markout_ema = [&](std::int64_t ts) {
        if (params.adverse_markout_decay_tau_s <= 0.0 || mo_last_decay_ts <= 0 || ts <= mo_last_decay_ts) {
            mo_last_decay_ts = ts;
            return;
        }
        const double dt_s = std::max(0.0, static_cast<double>(ts - mo_last_decay_ts) / 1000.0);
        const double decay = std::exp(-dt_s / params.adverse_markout_decay_tau_s);
        mo_ema_bid *= decay;
        mo_ema_ask *= decay;
        mo_ema_all *= decay;
        mo_last_decay_ts = ts;
    };

    auto extend_markout_pause_until = [&](bool is_bid, std::int64_t ts) {
        if (!params.adverse_markout_pause_hybrid || !params.quote.adverse_pause) {
            return;
        }
        const double ema = is_bid ? mo_ema_bid : mo_ema_ask;
        if (ema >= -adverse_markout_pause_threshold) {
            return;
        }
        const double min_s = std::max(0.0, params.adverse_markout_pause_min_s);
        const double max_s = std::max(min_s, params.adverse_markout_pause_max_s);
        double ttl_s = std::max(0.0, params.adverse_markout_pause_base_s)
            * std::abs(ema) / adverse_markout_pause_threshold;
        ttl_s = std::max(min_s, std::min(max_s, ttl_s));
        const std::int64_t until_ms = ts + static_cast<std::int64_t>(std::llround(ttl_s * 1000.0));
        if (is_bid) {
            if (until_ms > mo_pause_until_bid_ms) {
                mo_pause_until_bid_ms = until_ms;
                ++summary.bid_adverse_markout_pause_extend_count;
            }
        } else {
            if (until_ms > mo_pause_until_ask_ms) {
                mo_pause_until_ask_ms = until_ms;
                ++summary.ask_adverse_markout_pause_extend_count;
            }
        }
    };

    auto markout_pause_latch_active = [&](bool is_bid, std::int64_t ts) {
        if (!params.adverse_markout_pause_hybrid) {
            return false;
        }
        const double ema = is_bid ? mo_ema_bid : mo_ema_ask;
        const std::int64_t until_ms = is_bid ? mo_pause_until_bid_ms : mo_pause_until_ask_ms;
        return ema < -adverse_markout_pause_threshold && ts < until_ms;
    };

    auto reducing_cooldown_campaign_gate_active = [&](double prev_inventory, std::int64_t ts) {
        if (!params.fill_cooldown_reducing_campaign_only) {
            return true;
        }
        const double abs_q = std::abs(prev_inventory);
        const bool inv_hit =
            params.fill_cooldown_reducing_inv_threshold > 0.0 &&
            abs_q >= params.fill_cooldown_reducing_inv_threshold;
        const bool ratio_hit =
            params.fill_cooldown_reducing_inv_ratio > 0.0 &&
            abs_q / std::max(order_size, lot_size) >= params.fill_cooldown_reducing_inv_ratio;
        const bool age_hit =
            params.fill_cooldown_reducing_age_s > 0.0 &&
            campaign_age_s(ts, pos_open_ts, prev_inventory, lot_size) >= params.fill_cooldown_reducing_age_s;
        return inv_hit || ratio_hit || age_hit;
    };

    auto reducing_cooldown_vol_mult = [&]() {
        if (params.fill_cooldown_reducing_vol_ref <= 0.0) {
            return 1.0;
        }
        const double vol = pred.vol_10s;
        if (!std::isfinite(vol) || vol <= 0.0) {
            return 1.0;
        }
        return clamp(
            vol / std::max(params.fill_cooldown_reducing_vol_ref, 1e-12),
            params.fill_cooldown_reducing_vol_min_mult,
            params.fill_cooldown_reducing_vol_max_mult
        );
    };

    auto cooldown_ms_for_fill = [&]<Side S>(
        double prev_inventory,
        double consecutive_units,
        std::int64_t ts,
        bool account_adaptive_hit
    ) {
        const bool exposure_increasing_fill = exposure_increasing_for_side<S>(prev_inventory);
        double base_ms = 0.0;
        if (exposure_increasing_fill || params.fill_cooldown_apply_reducing) {
            base_ms = static_cast<double>(fill_cooldown_ms);
            if (exposure_increasing_fill && params.adaptive_add_cooldown_enabled) {
                const double ema = is_buy_v<S> ? mo_ema_bid : mo_ema_ask;
                // C++ replay hot path keeps only quote-time state that already
                // exists in the replay arrays.  Exact L2 refill/reversion
                // diagnostics are Python richer fields, so the C++ parity path
                // uses neutral refill/reversion unless those arrays are later
                // promoted into TickReplayInput.
                constexpr double refill_edge = 0.0;
                constexpr double micro_reversion_score = 0.0;
                const double mult = adaptive_add_cooldown_multiplier_cpp<S>(
                    params,
                    ema,
                    consecutive_units,
                    prev_inventory,
                    params.max_inventory,
                    ts,
                    pos_open_ts,
                    pred.ret_10s,
                    refill_edge,
                    micro_reversion_score
                );
                if (account_adaptive_hit && std::abs(mult - 1.0) > 1e-12) {
                    ++summary.adaptive_add_cooldown_hit_count;
                    if constexpr (is_buy_v<S>) {
                        ++summary.adaptive_add_cooldown_bid_hit_count;
                    } else {
                        ++summary.adaptive_add_cooldown_ask_hit_count;
                    }
                }
                base_ms *= mult;
            }
        } else {
            base_ms = static_cast<double>(fill_cooldown_reducing_ms);
            if (base_ms > 0.0) {
                if (reducing_cooldown_campaign_gate_active(prev_inventory, ts)) {
                    base_ms *= reducing_cooldown_vol_mult();
                } else {
                    base_ms = 0.0;
                }
            }
        }
        if (base_ms <= 0.0) {
            return 0.0;
        }
        return base_ms * std::max(1.0, consecutive_units);
    };

    auto& cooldown_fork_trace = result.cooldown_duration_fork_trace;
    cooldown_fork_trace.enabled = params.cooldown_duration_fork_enabled;
    cooldown_fork_trace.action = params.cooldown_duration_fork_action;
    cooldown_fork_trace.target_exposure_fill_ordinal =
        params.cooldown_duration_fork_target_ordinal;
    cooldown_fork_trace.target_order_id =
        params.cooldown_duration_fork_target_order_id;
    cooldown_fork_trace.exact_owner_baseline_policy_enabled =
        params.cooldown_duration_fork_baseline_policy_enabled;
    cooldown_fork_trace.exact_owner_policy_sha256 =
        params.cooldown_duration_fork_expected_owner_policy_sha256;
    const bool cooldown_duration_abi_enabled =
        params.trace_cooldown_duration_opportunities_max > 0 ||
        params.cooldown_duration_fork_enabled ||
        f05_cooldown_runtime.has_value();
    std::int64_t cooldown_duration_exposure_fill_ordinal = 0;
    std::int64_t cooldown_duration_campaign_count =
        std::abs(inventory) > 1e-10 ? 1 : 0;
    std::int64_t cooldown_duration_active_campaign_id =
        cooldown_duration_campaign_count;
    bool cooldown_duration_fork_assigned = false;
    bool cooldown_duration_fork_quarantine = false;
    bool cooldown_duration_fork_terminal = false;
    bool cooldown_duration_target_override_active = false;
    std::int64_t cooldown_duration_target_control_deadline_ts_ms = 0;
    std::int64_t cooldown_duration_fork_assignment_buy_fills = 0;
    std::int64_t cooldown_duration_fork_assignment_sell_fills = 0;
    std::int64_t cooldown_duration_fork_path_last_ts_ms = 0;
    double cooldown_duration_fork_path_inventory_btc = inventory;
    double cooldown_duration_fork_inventory_time_btc_s = 0.0;
    double cooldown_duration_fork_mae_usdc = 0.0;
    double cooldown_duration_fork_max_abs_inventory_btc = 0.0;

    const auto update_cooldown_duration_fork_path = [&] (
        std::int64_t now_ts_ms,
        double mark_price
    ) {
        if (!cooldown_duration_fork_assigned) {
            return;
        }
        if (now_ts_ms < cooldown_duration_fork_path_last_ts_ms) {
            throw std::runtime_error(
                "cooldown duration fork path clock regressed"
            );
        }
        const double elapsed_s = static_cast<double>(
            now_ts_ms - cooldown_duration_fork_path_last_ts_ms
        ) / 1000.0;
        cooldown_duration_fork_inventory_time_btc_s +=
            std::abs(cooldown_duration_fork_path_inventory_btc) * elapsed_s;
        const double value =
            cash + inventory * mark_price -
            cooldown_fork_trace.assignment_equity_usdc;
        cooldown_duration_fork_mae_usdc =
            std::min(cooldown_duration_fork_mae_usdc, value);
        cooldown_duration_fork_max_abs_inventory_btc = std::max(
            cooldown_duration_fork_max_abs_inventory_btc,
            std::abs(inventory)
        );
        cooldown_duration_fork_path_last_ts_ms = now_ts_ms;
        cooldown_duration_fork_path_inventory_btc = inventory;
    };

    bool buy_soft_widen_release_target_consumed = false;
    const auto native_same_ms_trade_ticks = [&] (std::int64_t ts_ms) {
        std::optional<std::int64_t> sell_min;
        std::optional<std::int64_t> buy_max;
        const auto* begin = input.trade_ts_ms.data();
        const auto* end = begin + input.trade_ts_ms.size();
        auto cursor = std::lower_bound(begin, end, ts_ms);
        while (cursor != end && *cursor == ts_ms) {
            const auto index = static_cast<std::size_t>(cursor - begin);
            if (input.trade_qty.data()[index] > 0.0) {
                const auto tick = price_to_tick(
                    input.trade_price.data()[index], tick_size
                );
                if (input.is_buyer_maker.data()[index] == 1) {
                    sell_min = sell_min.has_value()
                        ? std::min(*sell_min, tick)
                        : tick;
                } else {
                    buy_max = buy_max.has_value()
                        ? std::max(*buy_max, tick)
                        : tick;
                }
            }
            ++cursor;
        }
        return std::make_pair(sell_min, buy_max);
    };
    const auto apply_native_advance = [&] (const NativeTapeAdvance& advance) {
        const auto [sell_min, buy_max] = native_same_ms_trade_ticks(
            advance.exchange_ts_ns / 1'000'000
        );
        apply_native_book_advance_to_orders(
            advance,
            bid_orders,
            ask_orders,
            sell_min,
            buy_max,
            params,
            summary
        );
    };
    const auto advance_native = [&] (std::int64_t boundary_ms, bool inclusive) {
        if (!native_book.has_value()) {
            return;
        }
        native_book->advance_to(
            boundary_ms * 1'000'000,
            inclusive,
            summary,
            apply_native_advance
        );
    };
    const auto mark_native_cancel_ambiguity = [&] (std::int64_t boundary_ms) {
        if (!native_book.has_value()) {
            return;
        }
        const auto preview = native_book->preview_at(boundary_ms * 1'000'000);
        mark_native_cancel_boundary_ambiguity(
            bid_orders, preview, boundary_ms, summary
        );
        mark_native_cancel_boundary_ambiguity(
            ask_orders, preview, boundary_ms, summary
        );
        for (auto* orders : {&bid_orders, &ask_orders}) {
            for (auto& order : *orders) {
                if (order.cancel_effective_ts == boundary_ms) {
                    order.native_cancel_effective_processed = true;
                }
            }
        }
    };
    const auto transition_at_native_boundary = [&] (
        std::int64_t boundary_ms,
        double fallback_bid,
        double fallback_ask,
        bool defer_cancel_ack
    ) {
        transition_orders(
            bid_orders,
            input,
            boundary_ms,
            fallback_bid,
            fallback_ask,
            tick_size,
            params.cancel_order_latency_ms,
            params.latency_jitter_ms,
            &params.cancel_order_latency_samples_ms,
            params,
            summary,
            result,
            params.trace_quotes_max,
            circuit_breaker_close_gtx_reject_streak,
            native_book.has_value() ? &*native_book : nullptr,
            false,
            defer_cancel_ack
        );
        transition_orders(
            ask_orders,
            input,
            boundary_ms,
            fallback_bid,
            fallback_ask,
            tick_size,
            params.cancel_order_latency_ms,
            params.latency_jitter_ms,
            &params.cancel_order_latency_samples_ms,
            params,
            summary,
            result,
            params.trace_quotes_max,
            circuit_breaker_close_gtx_reject_streak,
            native_book.has_value() ? &*native_book : nullptr,
            false,
            defer_cancel_ack
        );
    };
    const auto advance_native_through_lifecycle = [&] (
        std::int64_t target_ms,
        double fallback_bid,
        double fallback_ask
    ) {
        if (!native_book.has_value()) {
            return;
        }
        while (true) {
            std::optional<std::int64_t> next_boundary;
            for (const auto* orders : {&bid_orders, &ask_orders}) {
                for (const auto& order : *orders) {
                    std::array<std::int64_t, 4> candidates{
                        !order.exchange_accepted ? order.activate_ts : 0,
                        order.state == OrderState::PendingNew
                            ? order.new_ack_ts : 0,
                        order.cancel_effective_ts > 0 &&
                                !order.native_cancel_effective_processed
                            ? order.cancel_effective_ts : 0,
                        order.cancel_ack_ts > 0 &&
                                !order.native_cancel_ack_processed
                            ? order.cancel_ack_ts : 0,
                    };
                    for (const auto boundary : candidates) {
                        if (boundary <= 0 || boundary >= target_ms) {
                            continue;
                        }
                        if (!next_boundary.has_value() ||
                            boundary < *next_boundary) {
                            next_boundary = boundary;
                        }
                    }
                }
            }
            if (!next_boundary.has_value()) {
                return;
            }
            advance_native(*next_boundary, false);
            mark_native_cancel_ambiguity(*next_boundary);
            transition_at_native_boundary(
                *next_boundary,
                fallback_bid,
                fallback_ask,
                false
            );
            for (auto* orders : {&bid_orders, &ask_orders}) {
                for (auto& order : *orders) {
                    if (order.cancel_ack_ts == *next_boundary) {
                        order.native_cancel_ack_processed = true;
                    }
                }
            }
            advance_native(*next_boundary, true);
        }
    };
    std::size_t last_processed_event_idx = 0;
    for (std::size_t i = 0; i < input.trade_ts_ms.size(); ++i) {
        last_processed_event_idx = i;
        const std::int64_t ts = input.trade_ts_ms.data()[i];
        hard_risk_clock_ts = ts;
        if (f05_cooldown_runtime.has_value()) {
            if (ts <= 0 ||
                ts > std::numeric_limits<std::int64_t>::max() / 1'000'000) {
                throw std::runtime_error(
                    "F05 repeated cooldown replay event clock is invalid"
                );
            }
            const auto event_ts_ns = ts * 1'000'000;
            f05_cooldown_runtime->advance_time(ts);
            const bool withhold_same_time_receive_window =
                f05_cooldown_runtime->config().feature_clock_semantics ==
                "receive_time_full_mid_ema_bank_v1";
            while (
                f05_cooldown_window_cursor <
                    f05_cooldown_window_tape.size() &&
                (f05_cooldown_window_tape[f05_cooldown_window_cursor]
                         .feature_ready_ts_ns < event_ts_ns ||
                 (!withhold_same_time_receive_window &&
                  f05_cooldown_window_tape[f05_cooldown_window_cursor]
                          .feature_ready_ts_ns == event_ts_ns))
            ) {
                f05_cooldown_runtime->update_window(
                    f05_cooldown_window_tape[
                        f05_cooldown_window_cursor]
                );
                ++f05_cooldown_window_cursor;
            }
        }
        update_p3_reach_budget_lifecycle(ts);
        const double price = input.trade_price.data()[i];
        const double qty = std::max(0.0, input.trade_qty.data()[i]);
        const bool execution_event = qty > 0.0;
        const std::int64_t trade_price_tick = execution_event
            ? price_to_tick(price, tick_size)
            : 0;
        const std::uint8_t maker_event_code = input.is_buyer_maker.data()[i];
        const bool seller_aggressor = maker_event_code == 1;
        const bool empirical_quote_event =
            params.empirical_requote_clock && !execution_event && maker_event_code == 2;
        const bool empirical_block_event =
            params.empirical_requote_clock && !execution_event && maker_event_code == 3;
        const bool sync_adjust_event =
            !execution_event && maker_event_code == kSyncAdjustEventCode;
        const bool sync_censor_event =
            !execution_event && maker_event_code == kSyncCensorEventCode;
        const std::size_t l2_idx_before_event = l2_idx;
        const double queue_base_now = value_at_or_default(input.queue_base_by_trade, i, params.queue_base);
        const double queue_decay_now = value_at_or_default(input.queue_decay_by_trade, i, params.queue_decay);
        const double buy_fill_prob_now = value_at_or_default(input.buy_fill_prob_by_trade, i, params.buy_fill_prob);
        const double sell_fill_prob_now = value_at_or_default(input.sell_fill_prob_by_trade, i, params.sell_fill_prob);
        double buy_queue_deplete_mult_now = value_at_or_default(
            input.buy_queue_deplete_mult_by_trade, i, 1.0);
        double sell_queue_deplete_mult_now = value_at_or_default(
            input.sell_queue_deplete_mult_by_trade, i, 1.0);
        if ((input.buy_queue_deplete_mult_by_trade.size() == 0 && !params.queue_deplete_buy_mult.empty()) ||
            (input.sell_queue_deplete_mult_by_trade.size() == 0 && !params.queue_deplete_sell_mult.empty())) {
            const double trade_local_rank = causal_local_rank_cpp(input, i, ts, price, 120'000, tick_size);
            if (input.buy_queue_deplete_mult_by_trade.size() == 0) {
                buy_queue_deplete_mult_now = queue_deplete_multiplier_cpp<Side::Buy>(params, trade_local_rank);
            }
            if (input.sell_queue_deplete_mult_by_trade.size() == 0) {
                sell_queue_deplete_mult_now = queue_deplete_multiplier_cpp<Side::Sell>(params, trade_local_rank);
            }
        }
        decay_markout_ema(ts);

        if (execution_event) {
            if (seller_aggressor) {
                inferred_best_bid = price;
                if (inferred_best_ask <= inferred_best_bid) {
                    inferred_best_ask = inferred_best_bid + tick_size;
                }
            } else {
                inferred_best_ask = price;
                if (inferred_best_bid >= inferred_best_ask) {
                    inferred_best_bid = inferred_best_ask - tick_size;
                }
            }
        }

        if (!input.bbo_ts_ms.empty()) {
            bbo_idx = advance_index(input.bbo_ts_ms, bbo_idx, ts);
        }
        if (!input.l2_ts_ms.empty()) {
            l2_idx = advance_index(input.l2_ts_ms, l2_idx, ts);
        }
        const auto book = book_snapshot_at(
            input, ts, bbo_idx, l2_idx,
            inferred_best_bid, inferred_best_ask, tick_size, params);
        const bool has_historical_book =
            input.bbo_ts_ms.size() > 0 || input.l2_ts_ms.size() > 0;
        const double current_loop_mid =
            (!params.use_bar_pricing && has_historical_book && book.mid > 0.0)
            ? book.mid
            : price;

        if (native_book.has_value()) {
            advance_native_through_lifecycle(
                ts, book.best_bid, book.best_ask
            );
            advance_native(ts, false);
            mark_native_cancel_ambiguity(ts);
            const auto [same_ms_sell_min, same_ms_buy_max] =
                native_same_ms_trade_ticks(ts);
            mark_native_cancel_trade_ambiguity(
                bid_orders,
                ts,
                same_ms_sell_min,
                same_ms_buy_max,
                summary
            );
            mark_native_cancel_trade_ambiguity(
                ask_orders,
                ts,
                same_ms_sell_min,
                same_ms_buy_max,
                summary
            );
            transition_at_native_boundary(
                ts,
                book.best_bid,
                book.best_ask,
                execution_event
            );
            advance_native(ts, true);
        } else {
            transition_orders(
                bid_orders,
                input,
                ts,
                book.best_bid,
                book.best_ask,
                tick_size,
                params.cancel_order_latency_ms,
                params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms,
                params,
                summary,
                result,
                params.trace_quotes_max,
                circuit_breaker_close_gtx_reject_streak
            );
            transition_orders(
                ask_orders,
                input,
                ts,
                book.best_bid,
                book.best_ask,
                tick_size,
                params.cancel_order_latency_ms,
                params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms,
                params,
                summary,
                result,
                params.trace_quotes_max,
                circuit_breaker_close_gtx_reject_streak
            );
        }
        paired_probe.on_event(
            ts,
            book.best_bid,
            book.best_ask,
            l2_idx,
            l2_idx != l2_idx_before_event
        );
        if (params.queue_l2_cancel_ahead_enabled && l2_idx != l2_idx_before_event) {
            apply_l2_cancel_ahead<Side::Buy>(
                bid_orders,
                input,
                params,
                l2_idx,
                summary
            );
            apply_l2_cancel_ahead<Side::Sell>(
                ask_orders,
                input,
                params,
                l2_idx,
                summary
            );
        }
        const double q_before_trade = inventory;
        const std::int64_t fills_before_trade = summary.fills_bid + summary.fills_ask;

        if (i > 0) {
            const double dt_s = std::max(
                0.0,
                static_cast<double>(ts - input.trade_ts_ms.data()[i - 1]) / 1000.0
            );
            const double mark = input.trade_price.data()[i - 1] > 0.0
                ? input.trade_price.data()[i - 1]
                : price;
            summary.signed_inventory_time_s += inventory * dt_s;
            summary.abs_inventory_time_s += std::abs(inventory) * dt_s;
            summary.sq_inventory_time_s += inventory * inventory * dt_s;
            summary.signed_notional_inventory_time_s += inventory * mark * dt_s;
            summary.notional_inventory_time_s += std::abs(inventory) * mark * dt_s;
            // inventory_pnl is a decomposition of passive inventory-path price
            // drift, not a risk penalty.  InvAdj = raw - inventory_pnl can mask
            // toxic markout, so promotion gates must also inspect raw PnL,
            // maker-signed markout, tail loss, inventory-time, and daily stability.
            summary.inventory_pnl += inventory * (price - input.trade_price.data()[i - 1]);
        }

        if (markout_enabled && !pending_markouts.empty()) {
            // The live/Python markout observer reads partial depth through its
            // own receive-time clock only while an observation is pending.
            // Do not advance this feed merely because another market event was
            // processed; that would leak future depth into the next decision.
            double markout_observation_mid = current_loop_mid;
            if (!params.use_bar_pricing && !input.l2_ts_ms.empty() &&
                !input.l2_bid_px.empty() && !input.l2_ask_px.empty()) {
                const std::int64_t sampled_delay_ms =
                    exec_book_visibility_delay_enabled
                    ? sample_exec_book_visibility_delay_ms(ts, params)
                    : 0;
                std::int64_t markout_visible_l2_ts = ts - sampled_delay_ms;
                if (exec_book_visibility_delay_enabled) {
                    markout_visible_l2_ts = std::max(
                        last_visible_l2_ts,
                        markout_visible_l2_ts
                    );
                    last_visible_l2_ts = markout_visible_l2_ts;
                }
                const std::ptrdiff_t markout_l2_pos =
                    index_at_or_before(input.l2_ts_ms, markout_visible_l2_ts);
                if (markout_l2_pos >= 0) {
                    const auto markout_l2_idx =
                        static_cast<std::size_t>(markout_l2_pos);
                    const double markout_bid = input.l2_bid_px(markout_l2_idx, 0);
                    const double markout_ask = input.l2_ask_px(markout_l2_idx, 0);
                    if (markout_bid > 0.0 && markout_ask > markout_bid) {
                        markout_observation_mid = 0.5 * (markout_bid + markout_ask);
                    }
                }
            }
            std::size_t write = 0;
            for (std::size_t j = 0; j < pending_markouts.size(); ++j) {
                const auto& item = pending_markouts[j];
                const std::int64_t age_ms = ts - item.fill_ts;
                if (age_ms >= markout_horizon_ms) {
                    if (age_ms > adverse_markout_max_resolve_gap_ms) {
                        ++summary.adverse_markout_stale_drop_count;
                        continue;
                    }
                    const double mo_val = item.is_bid
                        ? markout_observation_mid - item.fill_price
                        : item.fill_price - markout_observation_mid;
                    if (item.is_bid) {
                        mo_ema_bid = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_bid;
                        mo_sum_bid += mo_val * item.fill_qty_btc;
                        mo_qty_bid += item.fill_qty_btc;
                        extend_markout_pause_until(true, ts);
                    } else {
                        mo_ema_ask = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_ask;
                        mo_sum_ask += mo_val * item.fill_qty_btc;
                        mo_qty_ask += item.fill_qty_btc;
                        extend_markout_pause_until(false, ts);
                    }
                    mo_ema_all = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_all;
                    mo_sum_all += mo_val * item.fill_qty_btc;
                    ++mo_count_all;
                    mo_qty_all += item.fill_qty_btc;
                    if (item.final_compressed) {
                        mo_sum_final_compressed += mo_val * item.fill_qty_btc;
                        mo_qty_final_compressed += item.fill_qty_btc;
                    } else {
                        mo_sum_not_final_compressed += mo_val * item.fill_qty_btc;
                        mo_qty_not_final_compressed += item.fill_qty_btc;
                    }
                } else {
                    pending_markouts[write++] = item;
                }
            }
            pending_markouts.resize(write);
        }

        std::int64_t bid_fills_before_trade = summary.fills_bid;
        std::int64_t ask_fills_before_trade = summary.fills_ask;
        if (execution_event && seller_aggressor) {
            paired_probe.on_trade<Side::Buy>(
                ts,
                price,
                qty,
                params.queue_deplete_base_mult * buy_queue_deplete_mult_now
            );
        } else if (execution_event) {
            paired_probe.on_trade<Side::Sell>(
                ts,
                price,
                qty,
                params.queue_deplete_base_mult * sell_queue_deplete_mult_now
            );
        }
        const double buy_cooldown_deadline_before_trade =
            static_cast<double>(last_buy_fill_ts) + last_buy_fill_cooldown_ms;
        const double sell_cooldown_deadline_before_trade =
            static_cast<double>(last_sell_fill_ts) + last_sell_fill_cooldown_ms;
        std::int64_t duration_shadow_buy_fill_ts = last_buy_fill_ts;
        std::int64_t duration_shadow_sell_fill_ts = last_sell_fill_ts;
        double duration_shadow_buy_cooldown_ms = last_buy_fill_cooldown_ms;
        double duration_shadow_sell_cooldown_ms = last_sell_fill_cooldown_ms;
        bool cooldown_duration_fixed_target_assigned_this_event = false;
        Side cooldown_duration_fixed_target_side = Side::Buy;
        bool f05_cooldown_runtime_observed_fill_this_event = false;

        const auto cooldown_duration_fill_observer = [&] (
            Side fill_side,
            const ReplayOrder& order,
            std::size_t event_idx,
            std::int64_t fill_ts_ms,
            double fill_price,
            double fill_qty,
            double inventory_before_fill,
            double inventory_after_fill,
            double consecutive_units_after,
            double cash_after_fill
        ) {
            if (!cooldown_duration_abi_enabled) {
                return;
            }
            const bool is_buy = fill_side == Side::Buy;
            const bool exposure_increasing = is_buy
                ? inventory_before_fill >= 0.0
                : inventory_before_fill <= 0.0;
            const bool before_flat = std::abs(inventory_before_fill) <= 1e-10;
            const bool after_flat = std::abs(inventory_after_fill) <= 1e-10;
            const bool crossed_inventory_side =
                (inventory_before_fill < -1e-10 &&
                 inventory_after_fill > 1e-10) ||
                (inventory_before_fill > 1e-10 &&
                 inventory_after_fill < -1e-10);
            std::int64_t campaign_id = cooldown_duration_active_campaign_id;
            if (before_flat && !after_flat) {
                campaign_id = cooldown_duration_campaign_count + 1;
            } else if (campaign_id <= 0 && !after_flat) {
                campaign_id = std::max<std::int64_t>(
                    1,
                    cooldown_duration_campaign_count
                );
            }

            std::int64_t& previous_fill_ts = is_buy
                ? duration_shadow_buy_fill_ts
                : duration_shadow_sell_fill_ts;
            double& previous_cooldown_ms = is_buy
                ? duration_shadow_buy_cooldown_ms
                : duration_shadow_sell_cooldown_ms;
            const std::int64_t prior_deadline_ts_ms =
                round_ties_to_even(
                    static_cast<double>(previous_fill_ts) +
                    previous_cooldown_ms
                );
            const double baseline_duration_ms = is_buy
                ? cooldown_ms_for_fill.template operator()<Side::Buy>(
                    inventory_before_fill,
                    consecutive_units_after,
                    fill_ts_ms,
                    false
                )
                : cooldown_ms_for_fill.template operator()<Side::Sell>(
                    inventory_before_fill,
                    consecutive_units_after,
                    fill_ts_ms,
                    false
                );
            double applied_duration_ms = baseline_duration_ms > 0.0
                ? baseline_duration_ms
                : std::max(
                    0.0,
                    static_cast<double>(prior_deadline_ts_ms - fill_ts_ms)
                );

            bool target_fill = false;
            std::int64_t exposure_fill_ordinal = 0;
            if (exposure_increasing) {
                if (!std::isfinite(baseline_duration_ms) ||
                    baseline_duration_ms <= 0.0) {
                    throw std::runtime_error(
                        "exposure-increasing fill lacks a positive baseline cooldown"
                    );
                }
                ++cooldown_duration_exposure_fill_ordinal;
                exposure_fill_ordinal = cooldown_duration_exposure_fill_ordinal;
                const std::int64_t ordinal = exposure_fill_ordinal;
                const std::int64_t order_id = order.trace
                    ? order.trace->order_id
                    : -1;
                const double units_added = fill_qty /
                    std::max(order_size, lot_size);

                CooldownDurationOpportunityRow opportunity;
                opportunity.exposure_fill_ordinal = ordinal;
                opportunity.fill_visible_ts_ms = fill_ts_ms;
                opportunity.fill_exchange_ts_ms = fill_ts_ms;
                opportunity.side = fill_side;
                opportunity.opener = before_flat;
                opportunity.order_id = order_id;
                opportunity.campaign_id = campaign_id;
                opportunity.inventory_before_fill_btc =
                    inventory_before_fill;
                opportunity.inventory_after_fill_btc =
                    inventory_after_fill;
                opportunity.fill_qty_btc = fill_qty;
                opportunity.unit_qty_btc =
                    std::max(order_size, lot_size);
                opportunity.consecutive_units_before = std::max(
                    0.0,
                    consecutive_units_after - units_added
                );
                opportunity.consecutive_units_after =
                    consecutive_units_after;
                opportunity.prior_deadline_ts_ms = prior_deadline_ts_ms;
                opportunity.baseline_duration_ms = baseline_duration_ms;
                // Match Python/live's round(): half-millisecond cooldown
                // projections round to even, not away from zero.
                opportunity.baseline_deadline_ts_ms =
                    round_ties_to_even(
                        static_cast<double>(fill_ts_ms) +
                        baseline_duration_ms
                    );
                opportunity.canonical_mid = current_loop_mid;
                opportunity.best_bid = book.best_bid;
                opportunity.best_ask = book.best_ask;
                opportunity.decision_visible_bbo_index =
                    !input.bbo_ts_ms.empty() &&
                        bbo_idx < input.bbo_ts_ms.size() &&
                        input.bbo_ts_ms.data()[bbo_idx] <= fill_ts_ms
                    ? static_cast<std::int64_t>(bbo_idx)
                    : -1;
                opportunity.decision_visible_l2_index =
                    !input.l2_ts_ms.empty() &&
                        l2_idx < input.l2_ts_ms.size() &&
                        input.l2_ts_ms.data()[l2_idx] <= fill_ts_ms
                    ? static_cast<std::int64_t>(l2_idx)
                    : -1;
                opportunity.market_event_index =
                    static_cast<std::int64_t>(event_idx);
                opportunity.assignment_equity_usdc =
                    cash_after_fill +
                    inventory_after_fill * current_loop_mid;

                if (params.trace_cooldown_duration_opportunities_max > 0) {
                    if (result.cooldown_duration_opportunity_trace.size() >=
                        static_cast<std::size_t>(
                            params.trace_cooldown_duration_opportunities_max)) {
                        throw std::runtime_error(
                            "cooldown duration opportunity trace limit exhausted"
                        );
                    }
                    result.cooldown_duration_opportunity_trace.push_back(
                        opportunity
                    );
                }

                if (params.cooldown_duration_fork_enabled &&
                    !cooldown_duration_fork_assigned &&
                    ordinal > params.cooldown_duration_fork_target_ordinal) {
                    throw std::runtime_error(
                        "cooldown duration fork target ordinal was skipped"
                    );
                }
                if (params.cooldown_duration_fork_enabled &&
                    ordinal == params.cooldown_duration_fork_target_ordinal) {
                    const std::string side_name_value = is_buy ? "BUY" : "SELL";
                    if (fill_ts_ms !=
                            params.cooldown_duration_fork_target_ts_ms ||
                        side_name_value !=
                            params.cooldown_duration_fork_target_side ||
                        order_id !=
                            params.cooldown_duration_fork_target_order_id ||
                        campaign_id !=
                            params.cooldown_duration_fork_target_campaign_id) {
                        throw std::runtime_error(
                            "cooldown duration fork target identity drifted"
                        );
                    }
                    if (cooldown_duration_fork_assigned) {
                        throw std::runtime_error(
                            "cooldown duration fork attempted a second assignment"
                        );
                    }
                    if (std::abs(
                            baseline_duration_ms -
                            params.cooldown_duration_fork_expected_baseline_ms) >
                        1e-9) {
                        throw std::runtime_error(
                            "cooldown duration fork baseline duration drifted"
                        );
                    }
                    cooldown_duration_fork_assigned = true;
                    cooldown_duration_target_override_active = true;
                    target_fill = true;
                    cooldown_fork_trace.side = fill_side;
                    cooldown_fork_trace.campaign_id = campaign_id;
                    cooldown_fork_trace.assignment_ts_ms = fill_ts_ms;
                    cooldown_fork_trace.assignment_inventory_btc =
                        inventory_after_fill;
                    cooldown_fork_trace.assignment_equity_usdc =
                        opportunity.assignment_equity_usdc;
                    cooldown_fork_trace.baseline_duration_ms =
                        baseline_duration_ms;
                    cooldown_fork_trace.applied_duration_ms =
                        params.cooldown_duration_fork_action == "CONTROL_85N"
                        ? baseline_duration_ms
                        : params.cooldown_duration_fork_fixed_ms;
                    cooldown_fork_trace.applied_deadline_ts_ms =
                        round_ties_to_even(
                            static_cast<double>(fill_ts_ms) +
                            cooldown_fork_trace.applied_duration_ms
                        );
                    cooldown_duration_target_control_deadline_ts_ms =
                        opportunity.baseline_deadline_ts_ms;
                    cooldown_fork_trace.terminal_reason = "assigned";
                    cooldown_duration_fork_assignment_buy_fills =
                        summary.fills_bid;
                    cooldown_duration_fork_assignment_sell_fills =
                        summary.fills_ask;
                    cooldown_duration_fork_path_last_ts_ms = fill_ts_ms;
                    cooldown_duration_fork_path_inventory_btc =
                        inventory_after_fill;
                    cooldown_duration_fork_mae_usdc = 0.0;
                    cooldown_duration_fork_max_abs_inventory_btc =
                        std::abs(inventory_after_fill);
                    applied_duration_ms =
                        cooldown_fork_trace.applied_duration_ms;
                    if (params.cooldown_duration_fork_action ==
                        "FIXED_DURATION_MS") {
                        cooldown_duration_fixed_target_assigned_this_event =
                            true;
                        cooldown_duration_fixed_target_side = fill_side;
                    }
                }
            }

            if (f05_cooldown_runtime.has_value()) {
                const auto expected_control_duration_ms =
                    static_cast<std::int64_t>(std::llround(
                        static_cast<double>(kF05BooleanCooldownControlUnitMs) *
                        std::max(1.0, consecutive_units_after)
                    ));
                if (exposure_increasing &&
                    std::abs(
                        baseline_duration_ms -
                        static_cast<double>(expected_control_duration_ms)
                    ) > 1e-9) {
                    throw std::runtime_error(
                        "F05 repeated cooldown CONTROL_85N duration drifted"
                    );
                }

                F05CooldownFillInput runtime_input;
                runtime_input.snapshot_id =
                    "f05-cpp-full-replay:" +
                    std::to_string(exposure_fill_ordinal) + ":" +
                    std::to_string(fill_ts_ms) + ":" +
                    (is_buy ? "BUY:" : "SELL:") +
                    std::to_string(campaign_id);
                runtime_input.side = fill_side;
                runtime_input.role = exposure_increasing
                    ? (before_flat ? F05CooldownFillRole::Opener
                                   : F05CooldownFillRole::Add)
                    : F05CooldownFillRole::Reducing;
                runtime_input.fill_ts_ms = fill_ts_ms;
                runtime_input.decision_ts_ns = fill_ts_ms * 1'000'000;
                runtime_input.campaign_id = std::max<std::int64_t>(1, campaign_id);
                runtime_input.campaign_age_s = before_flat
                    ? 0.0
                    : campaign_age_s(
                          fill_ts_ms,
                          pos_open_ts,
                          inventory_before_fill,
                          lot_size
                      );
                runtime_input.inventory_before_fill_btc = inventory_before_fill;
                runtime_input.inventory_after_fill_btc = inventory_after_fill;
                runtime_input.consecutive_units_after = consecutive_units_after;
                runtime_input.baseline_duration_ms =
                    expected_control_duration_ms;

                if (exposure_increasing &&
                    !params.f05_cooldown_predicate_rows.empty()) {
                    if (f05_cooldown_predicate_cursor <
                        params.f05_cooldown_predicate_rows.size() &&
                        params.f05_cooldown_predicate_rows[
                            f05_cooldown_predicate_cursor]
                                .exposure_fill_ordinal < exposure_fill_ordinal) {
                        throw std::runtime_error(
                            "F05 repeated cooldown predicate-row identity drifted"
                        );
                    }
                    if (f05_cooldown_predicate_cursor <
                            params.f05_cooldown_predicate_rows.size() &&
                        params.f05_cooldown_predicate_rows[
                            f05_cooldown_predicate_cursor]
                                .exposure_fill_ordinal == exposure_fill_ordinal) {
                        const auto& row = params.f05_cooldown_predicate_rows[
                            f05_cooldown_predicate_cursor];
                        if (row.fill_ts_ms != fill_ts_ms ||
                            row.side != fill_side ||
                            row.campaign_id != campaign_id) {
                            throw std::runtime_error(
                                "F05 repeated cooldown predicate-row identity drifted"
                            );
                        }
                        runtime_input.snapshot_id = row.snapshot_id.empty()
                            ? runtime_input.snapshot_id
                            : row.snapshot_id;
                        runtime_input.policy_input_valid = row.policy_input_valid;
                        runtime_input.support_valid = row.support_valid;
                        runtime_input.channel_support_valid =
                            row.channel_support_valid;
                        runtime_input.snapshot_fallback_reason =
                            row.snapshot_fallback_reason;
                        runtime_input.predicate_values = row.predicate_values;
                        ++f05_cooldown_predicate_cursor;
                    }
                }

                auto runtime_decision =
                    f05_cooldown_runtime->apply_fill(runtime_input);
                f05_cooldown_runtime_observed_fill_this_event = true;
                runtime_decision.exposure_fill_ordinal =
                    exposure_fill_ordinal;
                if (exposure_increasing) {
                    if (!runtime_decision.lineage_applied ||
                        runtime_decision.duration_ms <= 0 ||
                        runtime_decision.deadline_ts_ms !=
                            fill_ts_ms + runtime_decision.duration_ms) {
                        throw std::runtime_error(
                            "F05 repeated cooldown deadline application failed"
                        );
                    }
                    if (target_fill &&
                        params.cooldown_duration_fork_baseline_policy_enabled) {
                        if (runtime_decision.action_id !=
                                params.cooldown_duration_fork_expected_owner_action ||
                            runtime_decision.policy_sha256 !=
                                params.cooldown_duration_fork_expected_owner_policy_sha256) {
                            throw std::runtime_error(
                                "F05 one-shot exact-owner target decision drifted: "
                                "expected_action=" +
                                params.cooldown_duration_fork_expected_owner_action +
                                ", observed_action=" + runtime_decision.action_id +
                                ", expected_policy_sha256=" +
                                params.cooldown_duration_fork_expected_owner_policy_sha256 +
                                ", observed_policy_sha256=" +
                                runtime_decision.policy_sha256 +
                                ", exposure_fill_ordinal=" +
                                std::to_string(exposure_fill_ordinal) +
                                ", fill_ts_ms=" + std::to_string(fill_ts_ms) +
                                ", side=" +
                                (fill_side == Side::Buy ? "BUY" : "SELL") +
                                ", campaign_id=" + std::to_string(campaign_id)
                            );
                        }
                        cooldown_fork_trace.schema_version =
                            "multiscale_ema_boolean_cooldown_duration_fork_trace.v3";
                        cooldown_fork_trace.exact_owner_action =
                            runtime_decision.action_id;
                        cooldown_fork_trace.exact_owner_baseline_duration_ms =
                            static_cast<double>(runtime_decision.duration_ms);
                        cooldown_duration_target_control_deadline_ts_ms =
                            runtime_decision.deadline_ts_ms;
                        const auto one_shot_duration_ms =
                            round_ties_to_even(
                                cooldown_fork_trace.applied_duration_ms);
                        f05_cooldown_runtime->override_active_lineage_duration(
                            fill_side,
                            fill_ts_ms,
                            campaign_id,
                            one_shot_duration_ms,
                            params.cooldown_duration_fork_action == "CONTROL_85N"
                                ? std::string("ONE_SHOT_CONTROL_85N")
                                : std::string("ONE_SHOT_FIXED_DURATION_MS"),
                            "one_shot_target_override"
                        );
                        applied_duration_ms =
                            cooldown_fork_trace.applied_duration_ms;
                    } else {
                        applied_duration_ms =
                            static_cast<double>(runtime_decision.duration_ms);
                    }
                }
                result.f05_repeated_cooldown_decisions.push_back(
                    std::move(runtime_decision)
                );
            }

            if (cooldown_duration_fork_assigned && !target_fill) {
                const bool is_target_side = fill_side == cooldown_fork_trace.side;
                if (!is_target_side || exposure_increasing) {
                    // Opposite fills clear the target-side control deadline. A
                    // later same-side exposure fill creates a new baseline
                    // lineage revision; only a same-side reducing fill may
                    // retain the immutable target revision.
                    cooldown_duration_target_override_active = false;
                }
            }

            previous_fill_ts = fill_ts_ms;
            previous_cooldown_ms = applied_duration_ms;
            if (is_buy) {
                duration_shadow_sell_cooldown_ms = 0.0;
            } else {
                duration_shadow_buy_cooldown_ms = 0.0;
            }

            if (cooldown_duration_fork_assigned) {
                CooldownDurationFillPathRow path_row;
                path_row.path_fill_ordinal = static_cast<std::int64_t>(
                    result.cooldown_duration_fill_path.size() + 1
                );
                path_row.fill_visible_ts_ms = fill_ts_ms;
                path_row.side = fill_side;
                path_row.order_id = order.trace ? order.trace->order_id : -1;
                path_row.campaign_id = campaign_id;
                path_row.exposure_increasing = exposure_increasing;
                path_row.target_fill = target_fill;
                path_row.fill_price_usdc_per_btc = fill_price;
                path_row.fill_qty_btc = fill_qty;
                path_row.inventory_before_fill_btc = inventory_before_fill;
                path_row.inventory_after_fill_btc = inventory_after_fill;
                path_row.cash_after_fill_usdc = cash_after_fill;
                path_row.baseline_duration_ms = baseline_duration_ms;
                path_row.applied_duration_ms = applied_duration_ms;
                path_row.applied_deadline_ts_ms =
                    round_ties_to_even(
                        static_cast<double>(fill_ts_ms) +
                        applied_duration_ms
                    );
                result.cooldown_duration_fill_path.push_back(path_row);
            }

            if (before_flat && !after_flat) {
                ++cooldown_duration_campaign_count;
                cooldown_duration_active_campaign_id =
                    cooldown_duration_campaign_count;
            } else if (crossed_inventory_side) {
                ++cooldown_duration_campaign_count;
                cooldown_duration_active_campaign_id =
                    cooldown_duration_campaign_count;
            } else if (after_flat) {
                cooldown_duration_active_campaign_id = 0;
            }
        };
        process_ioc_close_orders<Side::Buy>(
            bid_orders,
            result,
            input,
            i,
            ts,
            book,
            params,
            lot_size,
            params.taker_fee,
            order_size,
            cash,
            inventory,
            entry_price,
            loss_cooldown,
            observe_fill_risk,
            consecutive_buy_fills,
            consecutive_sell_fills,
            last_buy_fill_ts,
            pending_markouts,
            markout_enabled,
            summary.fills_bid,
            params.trace_fills_max,
            params.trace_quotes_max,
            params.trace_window_ms,
            cooldown_duration_fill_observer
        );
        process_ioc_close_orders<Side::Sell>(
            ask_orders,
            result,
            input,
            i,
            ts,
            book,
            params,
            lot_size,
            params.taker_fee,
            order_size,
            cash,
            inventory,
            entry_price,
            loss_cooldown,
            observe_fill_risk,
            consecutive_sell_fills,
            consecutive_buy_fills,
            last_sell_fill_ts,
            pending_markouts,
            markout_enabled,
            summary.fills_ask,
            params.trace_fills_max,
            params.trace_quotes_max,
            params.trace_window_ms,
            cooldown_duration_fill_observer
        );
        if (execution_event && seller_aggressor) {
            if (native_book.has_value()) {
                record_native_same_price_trade(
                    bid_orders, trade_price_tick, qty
                );
            }
            process_side_fill<Side::Buy>(
                bid_orders,
                result,
                input,
                i,
                ts,
                price,
                trade_price_tick,
                qty,
                params.queue_deplete_base_mult * buy_queue_deplete_mult_now,
                params.queue_l2_cancel_ahead_enabled,
                lot_size,
                params.maker_fee,
                order_size,
                cash,
                inventory,
                entry_price,
                loss_cooldown,
                observe_fill_risk,
                consecutive_buy_fills,
                consecutive_sell_fills,
                last_buy_fill_ts,
                pending_markouts,
                markout_enabled,
                summary.fills_bid,
                summary.pending_cancel_fills,
                fills_bid_final_compressed,
                params.trace_fills_max,
                params.trace_quotes_max,
                params.trace_window_ms,
                cooldown_duration_fill_observer
            );
        } else if (execution_event) {
            if (native_book.has_value()) {
                record_native_same_price_trade(
                    ask_orders, trade_price_tick, qty
                );
            }
            process_side_fill<Side::Sell>(
                ask_orders,
                result,
                input,
                i,
                ts,
                price,
                trade_price_tick,
                qty,
                params.queue_deplete_base_mult * sell_queue_deplete_mult_now,
                params.queue_l2_cancel_ahead_enabled,
                lot_size,
                params.maker_fee,
                order_size,
                cash,
                inventory,
                entry_price,
                loss_cooldown,
                observe_fill_risk,
                consecutive_sell_fills,
                consecutive_buy_fills,
                last_sell_fill_ts,
                pending_markouts,
                markout_enabled,
                summary.fills_ask,
                summary.pending_cancel_fills,
                fills_ask_final_compressed,
                params.trace_fills_max,
                params.trace_quotes_max,
                params.trace_window_ms,
                cooldown_duration_fill_observer
            );
        }
        if (native_book.has_value() && execution_event) {
            // A cancel ACK sharing this millisecond was retained until the
            // exchange fill boundary had been evaluated.  Publish/remove it
            // now, matching the strict Python lifecycle ordering.
            transition_at_native_boundary(
                ts,
                book.best_bid,
                book.best_ask,
                false
            );
            for (auto* orders : {&bid_orders, &ask_orders}) {
                for (auto& order : *orders) {
                    if (order.cancel_ack_ts == ts) {
                        order.native_cancel_ack_processed = true;
                    }
                }
            }
        }
        max_abs_inventory = std::max(max_abs_inventory, std::abs(inventory));
        update_p3_reach_budget_lifecycle(ts);
        const bool filled_trade = (summary.fills_bid + summary.fills_ask) > fills_before_trade;
        if (filled_trade && !fixed_spread_probe) {
            // Live cancels residual same-side orders immediately after an
            // active fill cooldown starts.  Without this, replay
            // can keep filling stale same-side orders during fill_cd and drift
            // away from Python/live fills.
            if (summary.fills_bid > bid_fills_before_trade) {
                const double new_cooldown_ms = cooldown_ms_for_fill.template operator()<Side::Buy>(
                    q_before_trade,
                    consecutive_buy_fills,
                    ts,
                    true
                );
                if (new_cooldown_ms > 0.0) {
                    last_buy_fill_cooldown_ms = new_cooldown_ms;
                } else {
                    last_buy_fill_cooldown_ms = std::max(
                        0.0,
                        buy_cooldown_deadline_before_trade - static_cast<double>(ts)
                    );
                }
                last_sell_fill_cooldown_ms = 0.0;
            }
            if (summary.fills_ask > ask_fills_before_trade) {
                const double new_cooldown_ms = cooldown_ms_for_fill.template operator()<Side::Sell>(
                    q_before_trade,
                    consecutive_sell_fills,
                    ts,
                    true
                );
                if (new_cooldown_ms > 0.0) {
                    last_sell_fill_cooldown_ms = new_cooldown_ms;
                } else {
                    last_sell_fill_cooldown_ms = std::max(
                        0.0,
                        sell_cooldown_deadline_before_trade - static_cast<double>(ts)
                    );
                }
                last_buy_fill_cooldown_ms = 0.0;
            }
            if (cooldown_duration_fixed_target_assigned_this_event) {
                if (cooldown_duration_fixed_target_side == Side::Buy) {
                    last_buy_fill_ts = duration_shadow_buy_fill_ts;
                    last_buy_fill_cooldown_ms =
                        duration_shadow_buy_cooldown_ms;
                } else {
                    last_sell_fill_ts = duration_shadow_sell_fill_ts;
                    last_sell_fill_cooldown_ms =
                        duration_shadow_sell_cooldown_ms;
                }
            }
            if (f05_cooldown_runtime_observed_fill_this_event) {
                const auto buy_lineage =
                    f05_cooldown_runtime->lineage(Side::Buy);
                const auto sell_lineage =
                    f05_cooldown_runtime->lineage(Side::Sell);
                if (buy_lineage.active) {
                    last_buy_fill_ts = buy_lineage.fill_ts_ms;
                    last_buy_fill_cooldown_ms =
                        static_cast<double>(buy_lineage.duration_ms);
                } else {
                    last_buy_fill_cooldown_ms = 0.0;
                }
                if (sell_lineage.active) {
                    last_sell_fill_ts = sell_lineage.fill_ts_ms;
                    last_sell_fill_cooldown_ms =
                        static_cast<double>(sell_lineage.duration_ms);
                } else {
                    last_sell_fill_cooldown_ms = 0.0;
                }
            }
            if (last_buy_fill_cooldown_ms > 0.0 && summary.fills_bid > bid_fills_before_trade) {
                request_cancel_all(
                    bid_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::FillCooldown
                );
            }
            if (last_sell_fill_cooldown_ms > 0.0 && summary.fills_ask > ask_fills_before_trade) {
                request_cancel_all(
                    ask_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::FillCooldown
                );
            }
            if (std::abs(inventory) < 1e-10 ||
                std::abs(q_before_trade) < 1e-10 ||
                (q_before_trade <= 0.0 && inventory > 0.0) ||
                (q_before_trade >= 0.0 && inventory < 0.0)) {
                pos_open_ts = ts;
            }
            if (inventory >= params.max_inventory) {
                request_cancel_all(bid_orders, ts, 0, 0, nullptr, params, &result, params.trace_quotes_max, CancelReason::InventoryLimit);
            } else if (inventory <= -params.max_inventory) {
                request_cancel_all(ask_orders, ts, 0, 0, nullptr, params, &result, params.trace_quotes_max, CancelReason::InventoryLimit);
            }
        }
        if (cooldown_duration_fork_assigned) {
            update_cooldown_duration_fork_path(ts, current_loop_mid);
            if (!cooldown_duration_fork_quarantine &&
                std::abs(inventory) <= 1e-10 &&
                cooldown_duration_active_campaign_id == 0) {
                cooldown_duration_fork_quarantine = true;
                cooldown_fork_trace.quarantine_entered = true;
                cooldown_fork_trace.quarantine_ts_ms = ts;
            }
            if (cooldown_duration_fork_quarantine &&
                std::abs(inventory) <= 1e-10 &&
                cooldown_duration_active_campaign_id == 0 &&
                bid_orders.empty() && ask_orders.empty()) {
                cooldown_duration_fork_terminal = true;
                cooldown_fork_trace.arm_washout_complete = true;
                // The estimand uses replay-visible time.  A cancel ACK may
                // carry an earlier physical effective timestamp, but the arm
                // is not scheduler-drained until this replay event observes
                // every order container empty.
                cooldown_fork_trace.terminal_ts_ms = ts;
                cooldown_fork_trace.terminal_reason =
                    "arm_economic_washout";
                break;
            }
        }
        if (params.planned_quote_stop_ts_ms > 0 &&
            ts >= params.planned_quote_stop_ts_ms) {
            if (!planned_shutdown_requested) {
                planned_shutdown_requested = true;
                summary.planned_quote_stop_triggered = true;
                summary.planned_quote_stop_trigger_ts_ms = ts;
                summary.planned_shutdown_orders_at_trigger =
                    static_cast<std::int64_t>(bid_orders.size() + ask_orders.size());
                request_cancel_all(
                    bid_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::PlannedMaintenance
                );
                request_cancel_all(
                    ask_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::PlannedMaintenance
                );
            }
            // Orders remain in the fill risk set until their sampled cancel
            // ACK. No policy state or new quote may be generated after stop.
            continue;
        }
        if (circuit_breaker_closing && std::abs(inventory) < lot_size) {
            circuit_breaker_closing = false;
            circuit_breaker_close_start_ts = 0;
            circuit_breaker_close_gtx_reject_streak = 0;
            entry_price = 0.0;
            pos_open_ts = ts;
            request_cancel_all(
                bid_orders, ts, 0, 0, nullptr, params, &result,
                params.trace_quotes_max, CancelReason::CircuitBreaker
            );
            request_cancel_all(
                ask_orders, ts, 0, 0, nullptr, params, &result,
                params.trace_quotes_max, CancelReason::CircuitBreaker
            );
        }


        if (sync_censor_event) {
            summary.sync_adjust_censored = true;
            summary.sync_adjust_censor_ts_ms = ts;
            break;
        }
        if (sync_adjust_event) {
            if (!params.sync_adjust_degrade_enabled ||
                params.sync_adjust_replay_mode == "disabled") {
                throw std::runtime_error(
                    "sync-degrade event supplied while live control is disabled"
                );
            }
            ++summary.sync_adjust_degrade_trigger_count;
            const auto pause_ms = std::max<std::int64_t>(
                0,
                static_cast<std::int64_t>(
                    std::llround(params.sync_adjust_pause_s * 1000.0)
                )
            );
            sync_adjust_degrade_until_ms = std::max(
                sync_adjust_degrade_until_ms,
                ts + pause_ms
            );
            if (params.sync_adjust_cancel_orders) {
                request_cancel_all(
                    bid_orders, ts, params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms, params, &result,
                    params.trace_quotes_max, CancelReason::SyncAdjustDegrade
                );
                request_cancel_all(
                    ask_orders, ts, params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms, params, &result,
                    params.trace_quotes_max, CancelReason::SyncAdjustDegrade
                );
            }
            continue;
        }

        std::int64_t exec_book_visibility_delay_ms =
            exec_book_visibility_delay_enabled
            ? sample_exec_book_visibility_delay_ms(ts, params)
            : 0;
        std::int64_t visible_bbo_ts =
            ts - exec_book_visibility_delay_ms;
        std::int64_t visible_l2_ts = visible_bbo_ts;
        if (exec_book_visibility_delay_enabled) {
            visible_bbo_ts = std::max(last_visible_bbo_ts, visible_bbo_ts);
            visible_l2_ts = std::max(last_visible_l2_ts, visible_l2_ts);
            last_visible_bbo_ts = visible_bbo_ts;
            last_visible_l2_ts = visible_l2_ts;
            exec_book_visibility_delay_ms = std::max<std::int64_t>(0, ts - visible_bbo_ts);
        }
        const std::ptrdiff_t visible_bbo_pos =
            index_at_or_before(input.bbo_ts_ms, visible_bbo_ts);
        const std::ptrdiff_t visible_l2_pos =
            index_at_or_before(input.l2_ts_ms, visible_l2_ts);
        const std::size_t visible_bbo_idx = visible_bbo_pos >= 0
            ? static_cast<std::size_t>(visible_bbo_pos)
            : input.bbo_ts_ms.size();
        const std::size_t visible_l2_idx = visible_l2_pos >= 0
            ? static_cast<std::size_t>(visible_l2_pos)
            : input.l2_ts_ms.size();
        const auto decision_book = book_snapshot_at(
            input,
            std::max(visible_bbo_ts, visible_l2_ts),
            visible_bbo_idx,
            visible_l2_idx,
            inferred_best_bid,
            inferred_best_ask,
            tick_size,
            params,
            ts
        );
        const double risk_mark = !params.use_bar_pricing && decision_book.mid > 0.0
            ? decision_book.mid : quote_mid_state;
        if (hard_risk_enabled) {
            hard_risk_last_mark = risk_mark;
            observe_risk(ts, risk_mark);
        }
        if (risk_emergency_latched && risk_emergency_submit_sent) {
            continue;
        }
        // Check active-order freshness before the requote interval, reusing
        // this same snapshot if the current policy event is also quote-due.
        if (!params.empirical_requote_clock && !params.use_bar_pricing &&
            has_historical_book && params.max_exec_book_age_ms > 0 &&
            (!bid_orders.empty() || !ask_orders.empty()) && !decision_book.fresh) {
            ++summary.stale_book_skip_count;
            paired_probe.on_decision_cancel(ts);
            request_cancel_all(bid_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::StaleBook);
            request_cancel_all(ask_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::StaleBook);
            continue;
        }

        if (!risk_emergency_latched && loss_cooldown.active(ts)) {
            ++summary.consecutive_loss_cooldown_block_count;
            if (ts - loss_cooldown.last_cancel_ts_ms >= 5'000) {
                request_cancel_all(
                    bid_orders, ts, params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms, params, &result,
                    params.trace_quotes_max,
                    CancelReason::ConsecutiveLossCooldown
                );
                request_cancel_all(
                    ask_orders, ts, params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms, params, &result,
                    params.trace_quotes_max,
                    CancelReason::ConsecutiveLossCooldown
                );
                ++summary.consecutive_loss_cooldown_cancel_count;
                loss_cooldown.last_cancel_ts_ms = ts;
            }
            continue;
        }

        if (empirical_block_event) {
            ++summary.stale_book_skip_count;
            paired_probe.on_decision_cancel(ts);
            request_cancel_all(
                bid_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms, params, &result,
                params.trace_quotes_max, CancelReason::StaleBook);
            request_cancel_all(
                ask_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms, params, &result,
                params.trace_quotes_max, CancelReason::StaleBook);
            continue;
        }
        if (!risk_emergency_latched && params.empirical_requote_clock && !empirical_quote_event) {
            continue;
        }
        if (!risk_emergency_latched && !params.empirical_requote_clock &&
            ts - last_requote_ts < current_rq_ms) {
            continue;
        }
        if (params.empirical_requote_clock) {
            last_requote_ts = ts;
        } else if (params.random_passive_enabled && params.random_passive_timing_jitter_fraction > 0.0) {
            const double jitter_fraction = clamp(
                params.random_passive_timing_jitter_fraction, 0.0, 0.95);
            const double centered = 2.0 * keyed_random_passive_unit(
                params.random_passive_seed,
                ts,
                static_cast<std::int64_t>(i),
                RandomPassiveOperation::TimingJitter
            ) - 1.0;
            const double interval_mult = 1.0 + centered * jitter_fraction;
            const std::int64_t target_interval_ms = std::max<std::int64_t>(
                1,
                static_cast<std::int64_t>(std::llround(
                    static_cast<double>(current_rq_ms) * interval_mult))
            );
            last_requote_ts = ts + target_interval_ms - current_rq_ms;
            ++summary.random_passive_timing_jitter_count;
        } else if (params.requote_clock_fixed) {
            const std::int64_t intervals = std::max<std::int64_t>(
                1,
                (ts - last_requote_ts) / std::max<std::int64_t>(current_rq_ms, 1)
            );
            last_requote_ts += intervals * current_rq_ms;
        } else {
            last_requote_ts = ts;
        }

        if (exec_book_visibility_delay_enabled) {
            ++summary.exec_book_visibility_delay_applied_count;
            summary.exec_book_visibility_delay_sum_ms +=
                static_cast<double>(exec_book_visibility_delay_ms);
            summary.exec_book_visibility_delay_max_ms = std::max(
                summary.exec_book_visibility_delay_max_ms,
                exec_book_visibility_delay_ms
            );
        }

        if (!params.empirical_requote_clock && !params.use_bar_pricing && has_historical_book &&
            params.max_exec_book_age_ms > 0 && !decision_book.fresh) {
            ++summary.stale_book_skip_count;
            paired_probe.on_decision_cancel(ts);
            request_cancel_all(bid_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms, &params.cancel_order_latency_samples_ms, params, &result, params.trace_quotes_max, CancelReason::StaleBook);
            request_cancel_all(ask_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms, &params.cancel_order_latency_samples_ms, params, &result, params.trace_quotes_max, CancelReason::StaleBook);
            continue;
        }
        if (fixed_spread_probe &&
            (decision_book.best_bid <= 0.0 ||
             decision_book.best_ask <= decision_book.best_bid)) {
            ++summary.stale_book_skip_count;
            paired_probe.on_decision_cancel(ts);
            request_cancel_all(
                bid_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms, params, &result,
                params.trace_quotes_max, CancelReason::StaleBook);
            request_cancel_all(
                ask_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms, params, &result,
                params.trace_quotes_max, CancelReason::StaleBook);
            continue;
        }

        if (input.var_ts_ms.size() > 0) {
            while (static_cast<std::size_t>(var_idx + 1) < input.var_ts_ms.size() &&
                   input.var_ts_ms.data()[var_idx + 1] + 1000 <= visible_bbo_ts) {
                ++var_idx;
                if (var_idx == 0) {
                    // First complete close is the return-series anchor.
                    continue;
                }
                if (dynamic_rq) {
                    const double rsq = input.var_retsq.data()[var_idx];
                    if (!dyn_rq_inited) {
                        ema_var_fast = rsq;
                        ema_var_slow = rsq;
                        dyn_rq_inited = true;
                    } else {
                        ema_var_fast = 0.067 * rsq + 0.933 * ema_var_fast;
                        ema_var_slow = 0.011 * rsq + 0.989 * ema_var_slow;
                    }
                }
                if (params.ber_guard_thresh > 0.0 && input.var_ti.size() == input.var_ts_ms.size()) {
                    const double ti_val = ber_held_ti;
                    if (!ber_inited) {
                        ema_ti_fast = ti_val;
                        ema_ti_slow = ti_val;
                        ber_inited = true;
                    } else {
                        ema_ti_fast = 0.13 * ti_val + 0.87 * ema_ti_fast;
                        ema_ti_slow = 0.011 * ti_val + 0.989 * ema_ti_slow;
                    }
                    if ((input.var_ts_ms.data()[var_idx] + 1000) % 10'000 == 0) {
                        // SignalEngine publishes mean aggregate trades per 10s
                        // bar. var_ti is the trailing per-second mean.
                        ber_pending_ti = input.var_ti.data()[var_idx] * 10.0;
                        ber_pending_feature_ts_ms = input.var_ts_ms.data()[var_idx];
                    }
                }
            }
            if (var_idx >= 0) {
                sigma_sq = std::max(input.var_ssq.data()[var_idx], 1e-6);
            }
        }
        // This legacy native path has one common source visibility clock;
        // do not publish a new liquidity input at a 1s left label or every
        // intervening second. Message-level source clocks remain Python-only.
        while (quote_ti_cursor < quote_ti_source_indices.size()) {
            const std::size_t source_idx = quote_ti_source_indices[quote_ti_cursor];
            if (input.var_ts_ms.data()[source_idx] + 1000 > visible_bbo_ts) {
                break;
            }
            cur_ti = input.var_ti.data()[source_idx] * 10.0;
            ++quote_ti_cursor;
        }
        if (dynamic_rq && dyn_rq_inited && ema_var_slow > 1e-12 && summary.n_requotes > 6) {
            double vol_ratio = ema_var_fast / ema_var_slow;
            vol_ratio = clamp(vol_ratio, 0.0, 2.0);
            const double log_ratio = std::log(static_cast<double>(rq_min_ms) / static_cast<double>(rq_max_ms));
            const double rq_f = static_cast<double>(rq_max_ms) * std::exp(log_ratio * vol_ratio);
            current_rq_ms = std::max<std::int64_t>(
                rq_min_ms,
                std::min<std::int64_t>(rq_max_ms, static_cast<std::int64_t>(rq_f))
            );
        }
        if (params.ber_guard_thresh > 0.0 && ber_inited) {
            ber_active = ema_ti_slow > 1e-6 &&
                (ema_ti_fast / ema_ti_slow) > params.ber_guard_thresh;
        }
        if (ber_pending_feature_ts_ms > ber_published_feature_ts_ms) {
            // Live callbacks update BER before the same requote publishes a
            // newly completed 10s feature. The value is visible only to later
            // 1s callbacks.
            ber_held_ti = ber_pending_ti;
            ber_published_feature_ts_ms = ber_pending_feature_ts_ms;
            ++summary.ber_feature_publish_count;
        }
        if (input.ml_ts_ms.size() > 0) {
            const std::int64_t prediction_cutoff_ts =
                (visible_bbo_ts / 1'000) * 1'000;
            ml_idx = advance_index(
                input.ml_ts_ms,
                ml_idx,
                prediction_cutoff_ts
            );
            if (input.ml_ts_ms.data()[ml_idx] <= prediction_cutoff_ts) {
                ml_ready = true;
                pred.dir_10s = input.ml_dir_10s.data()[ml_idx];
                pred.vol_10s = input.ml_vol_10s.data()[ml_idx];
                const double raw_ret_10s = input.ml_ret_10s.data()[ml_idx];
                if (params.quote.ret_skew > 0.0 && ret_demean_alpha > 0.0) {
                    pred_ret_ema = ret_demean_alpha * raw_ret_10s + (1.0 - ret_demean_alpha) * pred_ret_ema;
                    pred.ret_10s = raw_ret_10s - pred_ret_ema;
                } else {
                    pred.ret_10s = raw_ret_10s;
                }
                pred.tox_bid = input.ml_tox_bid.data()[ml_idx];
                pred.tox_ask = input.ml_tox_ask.data()[ml_idx];
            }
        }
        if (input.p3_ts_ms.size() > 0) {
            p3_idx = advance_index(input.p3_ts_ms, p3_idx, ts);
            p3_ready = input.p3_ts_ms.data()[p3_idx] <= ts;
        }

        QuoteCoreConfig quote_cfg = params.quote;
        if (p3_ready) {
            quote_cfg.p3_delta_star = input.p3_delta_star.data()[p3_idx];
            quote_cfg.p3_kappa_eff = input.p3_kappa_eff.data()[p3_idx];
        }
        quote_cfg.order_size = order_size;
        quote_cfg.max_inventory = params.max_inventory;
        quote_cfg.maker_fee = params.maker_fee;
        quote_cfg.use_bar_pricing = params.use_bar_pricing;
        const DepthView depth = params.use_bar_pricing
            ? DepthView{}
            : l2_depth_at(input, visible_l2_ts, visible_l2_idx);
        // Queue/fill truth uses the exchange-time `book` above. Quote decisions
        // use `decision_book`, which can be delayed by an environment-specific
        // receive-time visibility profile.
        const double mid = (
            !params.use_bar_pricing &&
            has_historical_book &&
            decision_book.mid > 0.0
        )
            ? decision_book.mid
            : quote_mid_state;
        const double activation_mid = mid;
        const auto position_timeout_ms = std::max<std::int64_t>(
            0, static_cast<std::int64_t>(std::llround(params.position_timeout_s * 1000.0))
        );
        if (!fixed_spread_probe && !circuit_breaker_closing && position_timeout_ms > 0 &&
            std::abs(inventory) >= lot_size && ts - pos_open_ts >= position_timeout_ms) {
            // TIMEOUT_CLOSING cancels first; a timeout is not itself a fill.
            circuit_breaker_closing = true;
            circuit_breaker_close_start_ts = ts;
            circuit_breaker_close_gtx_reject_streak = 0;
            ++summary.position_timeout_count;
            request_cancel_all(bid_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::PositionTimeout);
            request_cancel_all(ask_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::PositionTimeout);
            continue;
        }
        if (circuit_breaker_closing) {
            if (std::abs(inventory) < lot_size) {
                circuit_breaker_closing = false;
                circuit_breaker_close_start_ts = 0;
                circuit_breaker_close_gtx_reject_streak = 0;
                entry_price = 0.0;
                pos_open_ts = ts;
                continue;
            }

            const bool close_buy = inventory < 0.0;
            ReplayOrders& close_orders = close_buy ? bid_orders : ask_orders;
            ReplayOrders& opening_orders = close_buy ? ask_orders : bid_orders;
            // Emergency cancellation completes before any market liquidation.
            if (risk_emergency_latched &&
                (!bid_orders.empty() || !ask_orders.empty())) {
                continue;
            }
            request_cancel_all(
                opening_orders,
                ts,
                params.cancel_order_latency_ms,
                params.latency_jitter_ms,
                &params.cancel_order_latency_samples_ms,
                params,
                &result,
                params.trace_quotes_max,
                CancelReason::CircuitBreaker
            );

            const double close_qty = floor_lot(
                risk_emergency_latched ? std::abs(inventory)
                    : std::min(std::abs(inventory), order_size),
                lot_size
            );
            if (close_qty < lot_size) {
                continue;
            }
            const bool use_ioc =
                risk_emergency_latched || circuit_breaker_close_gtx_reject_streak >= 3 ||
                (
                    circuit_breaker_close_start_ts > 0 &&
                    ts - circuit_breaker_close_start_ts >= 60'000
                );
            const bool aggressive_passive =
                !use_ioc &&
                circuit_breaker_close_start_ts > 0 &&
                ts - circuit_breaker_close_start_ts >= 30'000;
            double close_price = 0.0;
            if (use_ioc && close_buy) {
                const double touch = decision_book.best_ask > 0.0
                    ? decision_book.best_ask
                    : mid;
                close_price =
                    std::ceil((touch + 2.0 * tick_size) / tick_size - 1e-12) *
                    tick_size;
            } else if (use_ioc) {
                const double touch = decision_book.best_bid > 0.0
                    ? decision_book.best_bid
                    : mid;
                close_price =
                    std::floor((touch - 2.0 * tick_size) / tick_size + 1e-12) *
                    tick_size;
            } else {
                close_price = close_buy
                    ? std::floor(mid / tick_size + 1e-12) * tick_size
                    : std::ceil(mid / tick_size - 1e-12) * tick_size;
            }
            if (aggressive_passive) {
                close_price += close_buy ? tick_size : -tick_size;
            }
            close_price = std::round(close_price / tick_size) * tick_size;

            const ReplayOrder* existing_close = nullptr;
            for (const auto& order : close_orders) {
                if (order.circuit_breaker_close &&
                    (order.state == OrderState::Open ||
                     order.state == OrderState::PendingCancel) &&
                    order.remaining + 1e-12 >= lot_size) {
                    existing_close = &order;
                    break;
                }
            }
            if (existing_close != nullptr && !use_ioc) {
                const double drift_bps =
                    std::abs(close_price - existing_close->price) /
                    std::max(existing_close->price, tick_size) * 10'000.0;
                if (drift_bps <= std::max(0.0, params.requote_threshold_bps)) {
                    ++summary.circuit_breaker_close_keep_count;
                    continue;
                }
            }

            if (!close_orders.empty()) {
                request_cancel_all(
                    close_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::CircuitBreakerCloseRequote
                );
            }
            const auto close_price_tick = price_to_tick(close_price, tick_size);
            const bool would_cross = close_buy
                ? (book.best_ask > 0.0 &&
                   close_price_tick >= price_to_tick(book.best_ask, tick_size))
                : (book.best_bid > 0.0 &&
                   close_price_tick <= price_to_tick(book.best_bid, tick_size));
            if (would_cross && !use_ioc) {
                ++summary.circuit_breaker_close_gtx_reject_count;
                ++circuit_breaker_close_gtx_reject_streak;
                continue;
            }
            if (!use_ioc) {
                circuit_breaker_close_gtx_reject_streak = 0;
            }

            const auto new_deadlines = sample_new_order_deadlines(
                ts,
                params.new_order_latency_ms,
                params.latency_jitter_ms,
                &params.new_order_latency_samples_ms,
                params,
                close_buy ? Side::Buy : Side::Sell,
                LatencyOperation::NewOrder,
                ts,
                false
            );
            const std::int64_t activate_latency_ms =
                new_deadlines.effective_ts - ts;
            const std::int64_t ack_latency_ms = new_deadlines.ack_ts - ts;
            if (use_ioc && ack_latency_ms > activate_latency_ms) {
                throw std::runtime_error(
                    "split new-order ACK does not yet support pre-ACK IOC fills"
                );
            }
            SideQuoteContext close_ctx;
            TraceOrderPtr close_trace{nullptr, PmrTraceDeleter{&trace_resource}};
            if (trace_enabled) {
                auto* trace_row = trace_allocator.new_object<TraceOrderRow>();
                trace_row->order_id = next_trace_order_id++;
                trace_row->side = close_buy ? Side::Buy : Side::Sell;
                trace_row->submit_ts = ts;
                trace_row->activate_ts = ts + activate_latency_ms;
                trace_row->quote_ts = ts;
                trace_row->price = close_price;
                trace_row->quantity = close_qty;
                trace_row->inventory = inventory;
                trace_row->mid = mid;
                trace_row->best_bid = decision_book.best_bid;
                trace_row->best_ask = decision_book.best_ask;
                trace_row->final_price = close_price;
                trace_row->remaining = close_qty;
                close_trace.reset(trace_row);
            }
            if (close_buy) {
                close_orders.push_back(make_order<Side::Buy>(
                    input,
                    params,
                    close_price,
                    close_qty,
                    ts,
                    activate_latency_ms,
                    ack_latency_ms,
                    activation_mid,
                    queue_base_now,
                    queue_decay_now,
                    inventory,
                    i,
                    mo_ema_bid,
                    close_ctx,
                    false,
                    std::move(close_trace)
                ));
            } else {
                close_orders.push_back(make_order<Side::Sell>(
                    input,
                    params,
                    close_price,
                    close_qty,
                    ts,
                    activate_latency_ms,
                    ack_latency_ms,
                    activation_mid,
                    queue_base_now,
                    queue_decay_now,
                    inventory,
                    i,
                    mo_ema_ask,
                    close_ctx,
                    false,
                    std::move(close_trace)
                ));
            }
            close_orders.back().reduce_only = true;
            close_orders.back().circuit_breaker_close = true;
            close_orders.back().immediate_or_cancel = use_ioc;
            close_orders.back().emergency_market = risk_emergency_latched;
            if (risk_emergency_latched) risk_emergency_submit_sent = true;
            ++summary.circuit_breaker_close_place_count;
            if (use_ioc) {
                ++summary.circuit_breaker_close_ioc_place_count;
            }
            continue;
        }

        const double unrealized_pnl = entry_price > 0.0 ? (mid - entry_price) * inventory : 0.0;
        const double circuit_sigma_pnl = std::sqrt(
            std::max(sigma_sq, 0.0) *
            std::max(quote_cfg.pnl_volatility_horizon_s, 1e-6)
        ) * std::abs(inventory);
        const double circuit_loss_threshold =
            std::max(params.circuit_breaker_sigma, 0.0) * circuit_sigma_pnl;
        if (!fixed_spread_probe && std::abs(inventory) >= lot_size &&
            circuit_loss_threshold > 0.0 &&
            unrealized_pnl < -circuit_loss_threshold) [[unlikely]] {
            ++summary.circuit_breaker_count;
            if (!params.circuit_breaker_maker_close) {
                const double close_qty = std::abs(inventory);
                const bool close_buy = inventory < 0.0;
                if (inventory > 0.0) {
                    cash += inventory * mid * (1.0 - params.taker_fee);
                } else {
                    cash += inventory * mid * (1.0 + params.taker_fee);
                }
                record_loss_fill(
                    close_buy,
                    close_qty,
                    mid,
                    params.taker_fee,
                    0.0
                );
                inventory = 0.0;
                if (observe_fill_risk) observe_fill_risk();
                entry_price = 0.0;
                pos_open_ts = ts;
                update_p3_reach_budget_lifecycle(ts);
                request_cancel_all(
                    bid_orders, ts, 0, 0, nullptr, params, &result,
                    params.trace_quotes_max, CancelReason::CircuitBreaker
                );
                request_cancel_all(
                    ask_orders, ts, 0, 0, nullptr, params, &result,
                    params.trace_quotes_max, CancelReason::CircuitBreaker
                );
            } else {
                circuit_breaker_closing = true;
                circuit_breaker_close_start_ts = ts;
                circuit_breaker_close_gtx_reject_streak = 0;
                request_cancel_all(
                    bid_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::CircuitBreaker
                );
                request_cancel_all(
                    ask_orders,
                    ts,
                    params.cancel_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params,
                    &result,
                    params.trace_quotes_max,
                    CancelReason::CircuitBreaker
                );
            }
            continue;
        }
        if (risk_emergency_latched) {
            // Never resume ordinary quoting after an emergency shutdown.
            continue;
        }
        CancelReason hard_risk = CancelReason::None;
        if (risk_last - risk_day_start < -params.max_daily_loss) {
            hard_risk = CancelReason::DailyLoss;
            ++summary.risk_daily_loss_block_count;
        } else if (std::abs(inventory) * mid > params.max_position_value) {
            hard_risk = CancelReason::PositionValue;
            ++summary.risk_position_value_block_count;
        } else if (std::max(0.0, risk_peak - risk_last) > params.emergency_close_dd) {
            hard_risk = CancelReason::EmergencyDrawdown;
            risk_emergency_latched = true;
            circuit_breaker_closing = std::abs(inventory) >= lot_size;
            circuit_breaker_close_start_ts = ts;
            ++summary.risk_emergency_close_count;
        }
        if (hard_risk != CancelReason::None) {
            request_cancel_all(bid_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, hard_risk);
            request_cancel_all(ask_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, hard_risk);
            continue;
        }
        const auto loss_transition = loss_cooldown.on_policy_clock(ts);
        if (loss_transition == LossCooldownTransition::Triggered ||
            loss_transition == LossCooldownTransition::Active) {
            request_cancel_all(bid_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::ConsecutiveLossCooldown);
            request_cancel_all(ask_orders, ts, params.cancel_order_latency_ms,
                params.latency_jitter_ms, &params.cancel_order_latency_samples_ms,
                params, &result, params.trace_quotes_max, CancelReason::ConsecutiveLossCooldown);
            ++summary.consecutive_loss_cooldown_cancel_count;
            loss_cooldown.last_cancel_ts_ms = ts;
            continue;
        }
        QuoteCoreResult quote;
        QuoteCoreResult global_ber_quote;
        QuoteCoreResult ber_bypass_quote;
        BerInventoryRole ber_buy_role = BerInventoryRole::Opener;
        BerInventoryRole ber_sell_role = BerInventoryRole::Opener;
        bool role_safe_ber_applied = false;
        if (fixed_spread_probe) {
            const double distance =
                params.fixed_spread_probe_ticks * tick_size;
            quote.bid_price = floor_tick(
                decision_book.best_bid - distance, tick_size);
            quote.ask_price = ceil_tick(
                decision_book.best_ask + distance, tick_size);
            quote.spread = std::max(quote.ask_price - quote.bid_price, tick_size);
            quote.raw_half_spread = 0.5 * quote.spread;
            quote.capped_half_spread = quote.raw_half_spread;
            quote.fair = mid;
            quote.reservation_price = mid;
            quote.delta_raw = quote.raw_half_spread;
            quote.delta_after_regime = quote.raw_half_spread;
            quote.delta_pre_cap = quote.raw_half_spread;
            quote.delta_after_cap = quote.raw_half_spread;
            quote.buy.raw_price = quote.bid_price;
            quote.buy.pre_guard_price = quote.bid_price;
            quote.buy.raw_quote_delta_to_bbo =
                decision_book.best_bid - quote.bid_price;
            quote.buy.pre_guard_delta_to_bbo =
                quote.buy.raw_quote_delta_to_bbo;
            quote.buy.raw_distance_to_mid = mid - quote.bid_price;
            quote.sell.raw_price = quote.ask_price;
            quote.sell.pre_guard_price = quote.ask_price;
            quote.sell.raw_quote_delta_to_bbo =
                quote.ask_price - decision_book.best_ask;
            quote.sell.pre_guard_delta_to_bbo =
                quote.sell.raw_quote_delta_to_bbo;
            quote.sell.raw_distance_to_mid = quote.ask_price - mid;
            global_ber_quote = quote;
            ber_bypass_quote = quote;
        } else {
            const QuoteState quote_state{
                mid,
                inventory,
                sigma_sq,
                cur_ti,
                decision_book.best_bid,
                decision_book.best_ask,
                ber_active,
                mo_ema_all,
                mo_ema_bid,
                mo_ema_ask,
                markout_pause_latch_active(true, ts),
                markout_pause_latch_active(false, ts),
                mo_ref,
                std::abs(inventory) > 1e-8,
                std::max(0.0, static_cast<double>(ts - pos_open_ts) / 1000.0),
                unrealized_pnl,
            };
            global_ber_quote = compute_quote_core(
                quote_state,
                quote_cfg,
                pred,
                depth
            );
            quote = global_ber_quote;
            ber_bypass_quote = global_ber_quote;
            if (params.ber_exposure_add_only && ber_active) {
                QuoteState bypass_state = quote_state;
                bypass_state.ber_active = false;
                ber_bypass_quote = compute_quote_core(
                    bypass_state,
                    quote_cfg,
                    pred,
                    depth
                );
                quote = compose_ber_exposure_add_only_quote(
                    global_ber_quote,
                    ber_bypass_quote,
                    inventory,
                    order_size,
                    order_size
                );
                role_safe_ber_applied = true;
                ber_buy_role = ber_inventory_role_for_target(
                    Side::Buy, inventory, order_size
                );
                ber_sell_role = ber_inventory_role_for_target(
                    Side::Sell, inventory, order_size
                );
                ++summary.ber_role_safe_decision_count;
                summary.ber_role_safe_buy_add_count +=
                    ber_buy_role == BerInventoryRole::Add ? 1 : 0;
                summary.ber_role_safe_sell_add_count +=
                    ber_sell_role == BerInventoryRole::Add ? 1 : 0;
                summary.ber_role_safe_flat_bypass_count +=
                    ber_buy_role == BerInventoryRole::Opener &&
                    ber_sell_role == BerInventoryRole::Opener ? 1 : 0;
                summary.ber_role_safe_mixed_fail_closed_count +=
                    ber_buy_role == BerInventoryRole::MixedCrossZero ||
                    ber_sell_role == BerInventoryRole::MixedCrossZero ? 1 : 0;

                const double expected_bid =
                    ber_buy_role == BerInventoryRole::Add ||
                    ber_buy_role == BerInventoryRole::MixedCrossZero
                        ? global_ber_quote.bid_price
                        : ber_bypass_quote.bid_price;
                const double expected_ask =
                    ber_sell_role == BerInventoryRole::Add ||
                    ber_sell_role == BerInventoryRole::MixedCrossZero
                        ? global_ber_quote.ask_price
                        : ber_bypass_quote.ask_price;
                if (!same_price_tick(quote.bid_price, expected_bid, tick_size) ||
                    !same_price_tick(quote.ask_price, expected_ask, tick_size)) {
                    ++summary.ber_role_safe_source_mismatch_count;
                }
            }
        }

        double bid_quote_price = quote.bid_price;
        double ask_quote_price = quote.ask_price;
        double global_ber_reference_bid = global_ber_quote.bid_price;
        double global_ber_reference_ask = global_ber_quote.ask_price;
        double ber_bypass_reference_bid = ber_bypass_quote.bid_price;
        double ber_bypass_reference_ask = ber_bypass_quote.ask_price;
        SideQuoteContext bid_quote_context = quote.buy;
        SideQuoteContext ask_quote_context = quote.sell;
        const double l2_near_depth = depth.has_book()
            ? top_depth_total(depth, quote_cfg.trace_book_imb_levels)
            : top_qty_depth_total(decision_book);
        const double near_depth = quote.near_depth_total > 0.0
            ? quote.near_depth_total
            : l2_near_depth;
        const L2RefillCancelMetrics l2_metrics =
            l2_refill_cancel_metrics_at(
                input,
                visible_l2_ts,
                ts,
                visible_l2_idx,
                params
            );
        bid_quote_context.l2_quote_flip_rate = l2_metrics.quote_flip_rate;
        bid_quote_context.l2_book_refresh_ratio = l2_metrics.book_refresh_ratio;
        bid_quote_context.l2_book_cancel_ratio = l2_metrics.book_cancel_ratio;
        bid_quote_context.l2_near_depth_total = l2_metrics.near_depth_total;
        ask_quote_context.l2_quote_flip_rate = l2_metrics.quote_flip_rate;
        ask_quote_context.l2_book_refresh_ratio = l2_metrics.book_refresh_ratio;
        ask_quote_context.l2_book_cancel_ratio = l2_metrics.book_cancel_ratio;
        ask_quote_context.l2_near_depth_total = l2_metrics.near_depth_total;

        if (!fixed_spread_probe && params.emergency_taker_close_enabled &&
            std::abs(inventory) >= lot_size &&
            (quote.buy.defense_emergency || quote.sell.defense_emergency)) [[unlikely]] {
            const double close_qty = std::abs(inventory);
            const bool close_buy = inventory < 0.0;
            if (inventory > 0.0) {
                cash += inventory * mid * (1.0 - params.taker_fee);
            } else {
                cash += inventory * mid * (1.0 + params.taker_fee);
            }
            record_loss_fill(
                close_buy,
                close_qty,
                mid,
                params.taker_fee,
                0.0
            );
            inventory = 0.0;
            if (observe_fill_risk) observe_fill_risk();
            entry_price = 0.0;
            pos_open_ts = ts;
            update_p3_reach_budget_lifecycle(ts);
            ++summary.emergency_close_count;
            request_cancel_all(bid_orders, ts, 0, 0, nullptr, params, &result, params.trace_quotes_max, CancelReason::EmergencyTakerClose);
            request_cancel_all(ask_orders, ts, 0, 0, nullptr, params, &result, params.trace_quotes_max, CancelReason::EmergencyTakerClose);
            continue;
        }

        double local_low = mid;
        double local_high = mid;
        const double local_rank = local_extreme_rank_cpp(input, i, ts, mid, params, local_low, local_high);
        const double queue_local_rank = causal_local_rank_cpp(input, i, ts, price, 120'000, tick_size);
        apply_local_extreme_cpp(
            bid_quote_context,
            ask_quote_context,
            params,
            mid,
            local_rank,
            local_low,
            local_high,
            near_depth,
            summary
        );
        const bool bid_fill_cooldown_active =
            consecutive_buy_fills > 1e-10 &&
            last_buy_fill_cooldown_ms > 0.0 &&
            static_cast<double>(ts - last_buy_fill_ts) < last_buy_fill_cooldown_ms;
        const bool ask_fill_cooldown_active =
            consecutive_sell_fills > 1e-10 &&
            last_sell_fill_cooldown_ms > 0.0 &&
            static_cast<double>(ts - last_sell_fill_ts) < last_sell_fill_cooldown_ms;
        const double inventory_ratio = std::min(
            std::abs(inventory) / std::max(params.max_inventory, 1e-9),
            1.0
        );
        const auto bid_common_policy = evaluate_common_side_policy_cpp(
            bid_quote_context,
            inventory >= 0.0,
            bid_fill_cooldown_active,
            inventory_ratio,
            static_cast<double>(decision_book.age_ms) / 1000.0,
            static_cast<double>(params.max_exec_book_age_ms) / 1000.0,
            mo_ema_bid,
            quote_cfg.markout_spread_scale,
            mo_ref,
            quote.microprice_shift_bps,
            quote_cfg.kappa_depth_baseline,
            params.thin_depth_threshold
        );
        const auto ask_common_policy = evaluate_common_side_policy_cpp(
            ask_quote_context,
            inventory <= 0.0,
            ask_fill_cooldown_active,
            inventory_ratio,
            static_cast<double>(decision_book.age_ms) / 1000.0,
            static_cast<double>(params.max_exec_book_age_ms) / 1000.0,
            mo_ema_ask,
            quote_cfg.markout_spread_scale,
            mo_ref,
            quote.microprice_shift_bps,
            quote_cfg.kappa_depth_baseline,
            params.thin_depth_threshold
        );
        double bid_policy_mult = bid_common_policy.spread_mult;
        double ask_policy_mult = ask_common_policy.spread_mult;
        if (params.campaign_soft_control_enabled) {
            const double bid_refill_edge =
                bid_quote_context.l2_book_refresh_ratio -
                bid_quote_context.l2_book_cancel_ratio;
            const double ask_refill_edge =
                ask_quote_context.l2_book_refresh_ratio -
                ask_quote_context.l2_book_cancel_ratio;
            const auto micro_reversion_score = [&](double refill_edge, double adverse_ret) {
                const double trend_norm = params.campaign_soft_gate_trend_ret_ref > 0.0
                    ? clamp(
                        adverse_ret /
                            std::max(params.campaign_soft_gate_trend_ret_ref, 1e-12),
                        0.0,
                        1.0
                    )
                    : 0.0;
                const double good_refill_norm = params.campaign_soft_gate_refill_ref > 0.0
                    ? clamp(
                        std::max(0.0, refill_edge) /
                            std::max(params.campaign_soft_gate_refill_ref, 1e-12),
                        0.0,
                        1.0
                    )
                    : 0.0;
                return good_refill_norm * (1.0 - trend_norm);
            };
            const double bid_micro_reversion = micro_reversion_score(
                bid_refill_edge,
                side_adverse_ret_from_pred<Side::Buy>(pred.ret_10s)
            );
            const double ask_micro_reversion = micro_reversion_score(
                ask_refill_edge,
                side_adverse_ret_from_pred<Side::Sell>(pred.ret_10s)
            );
            const bool bid_campaign_soft =
                campaign_exposure_risk_active<Side::Buy>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    params.campaign_soft_inv_threshold,
                    params.campaign_soft_age_s
                ) &&
                campaign_soft_gate_active<Side::Buy>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    pred.ret_10s,
                    bid_refill_edge,
                    bid_micro_reversion
                );
            const bool ask_campaign_soft =
                campaign_exposure_risk_active<Side::Sell>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    params.campaign_soft_inv_threshold,
                    params.campaign_soft_age_s
                ) &&
                campaign_soft_gate_active<Side::Sell>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    pred.ret_10s,
                    ask_refill_edge,
                    ask_micro_reversion
                );
            if (bid_campaign_soft) {
                bid_policy_mult = std::max(bid_policy_mult, params.campaign_soft_spread_mult);
                ++summary.campaign_soft_control_count;
                ++summary.bid_campaign_soft_control_count;
            }
            if (ask_campaign_soft) {
                ask_policy_mult = std::max(ask_policy_mult, params.campaign_soft_spread_mult);
                ++summary.campaign_soft_control_count;
                ++summary.ask_campaign_soft_control_count;
            }
        }
        if (params.buy_soft_widen_release_probe_enabled &&
            !buy_soft_widen_release_target_consumed &&
            ts == params.buy_soft_widen_release_target_ts_ms) {
            buy_soft_widen_release_target_consumed = true;
            ++summary.buy_soft_widen_release_target_reached_count;

            const double role_tolerance = std::max(std::abs(lot_size) * 0.5, 1e-12);
            const std::string observed_role =
                std::abs(inventory) < role_tolerance
                    ? "opener"
                    : inventory > 0.0 ? "add" : "reducing";
            constexpr std::uint32_t hard_reason_mask =
                PolicyReasonFillCooldown |
                PolicyReasonStaleHard |
                PolicyReasonDefense |
                PolicyReasonBurst |
                PolicyReasonInventoryLimit;
            const bool eligible =
                observed_role == params.buy_soft_widen_release_target_role &&
                bid_common_policy.allow_post &&
                bid_common_policy.allow_exposure_increase &&
                (bid_common_policy.reason_mask & hard_reason_mask) == 0;
            const double baseline_mult = bid_policy_mult;
            const double selected_mult = eligible
                ? std::min(
                    baseline_mult,
                    params.buy_soft_widen_release_spread_mult_cap
                )
                : baseline_mult;
            const bool effective_mult = selected_mult < baseline_mult - 1e-12;
            const double baseline_bid = apply_replay_side_policy_price<Side::Buy>(
                bid_quote_price,
                mid,
                baseline_mult,
                tick_size
            );
            const double candidate_bid = apply_replay_side_policy_price<Side::Buy>(
                bid_quote_price,
                mid,
                selected_mult,
                tick_size
            );
            const bool effective_price =
                price_to_tick(baseline_bid, tick_size) !=
                price_to_tick(candidate_bid, tick_size);

            summary.buy_soft_widen_release_eligible_count += eligible ? 1 : 0;
            summary.buy_soft_widen_release_requested_count += eligible ? 1 : 0;
            summary.buy_soft_widen_release_effective_mult_count +=
                effective_mult ? 1 : 0;
            summary.buy_soft_widen_release_effective_price_count +=
                effective_price ? 1 : 0;
            summary.buy_soft_widen_release_role_observed = observed_role;
            summary.buy_soft_widen_release_reason = !eligible
                ? "role_or_permission_ineligible"
                : effective_mult
                    ? "applied"
                    : "spread_mult_already_at_or_below_cap";
            summary.buy_soft_widen_release_baseline_spread_mult = baseline_mult;
            summary.buy_soft_widen_release_selected_spread_mult = selected_mult;
            summary.buy_soft_widen_release_baseline_bid_price = baseline_bid;
            summary.buy_soft_widen_release_candidate_bid_price = candidate_bid;
            if (params.buy_soft_widen_release_probe_apply_candidate && effective_mult) {
                bid_policy_mult = selected_mult;
            }
        }
        if (params.buy_fill_selection_live_enabled && !params.buy_fill_selection_models.empty()) {
            const bool bid_exposure_increasing_for_score = inventory >= 0.0;
            if (bid_exposure_increasing_for_score || params.buy_fill_selection_live_apply_reducing) {
                const auto score = score_buy_fill_selection_cpp(
                    input,
                    params,
                    quote,
                    bid_quote_context,
                    pred,
                    ml_ready ? ml_idx + 1 : 0,
                    mid,
                    inventory,
                    mo_ema_bid,
                    queue_local_rank,
                    bid_common_policy
                );
                ++summary.buy_fill_selection_live_eval_count;
                summary.buy_fill_selection_live_score_sum += score.score;
                summary.buy_fill_selection_live_score_max =
                    std::max(summary.buy_fill_selection_live_score_max, score.score);
                summary.buy_fill_selection_live_score_ge_042 += score.score >= 0.42 ? 1 : 0;
                summary.buy_fill_selection_live_score_ge_043 += score.score >= 0.43 ? 1 : 0;
                summary.buy_fill_selection_live_score_ge_044 += score.score >= 0.44 ? 1 : 0;
                summary.buy_fill_selection_live_score_ge_045 += score.score >= 0.45 ? 1 : 0;
                const bool threshold_hit =
                    score.score >= params.buy_fill_selection_live_score_threshold &&
                    score.missing <= params.buy_fill_selection_live_max_missing_features;
                constexpr std::uint32_t hard_reason_mask =
                    PolicyReasonFillCooldown |
                    PolicyReasonStaleHard |
                    PolicyReasonDefense |
                    PolicyReasonBurst |
                    PolicyReasonInventoryLimit;
                const bool hit =
                    threshold_hit &&
                    bid_common_policy.allow_post &&
                    bid_common_policy.allow_exposure_increase &&
                    (bid_common_policy.reason_mask & hard_reason_mask) == 0;
                bid_quote_context.buy_fill_selection_live_score = score.score;
                bid_quote_context.buy_fill_selection_live_hit = hit;
                bid_quote_context.buy_fill_selection_live_missing_features = score.missing;
                if (hit) {
                    ++summary.buy_fill_selection_live_hit_count;
                    bid_policy_mult = std::min(
                        bid_policy_mult,
                        std::max(1.0, params.buy_fill_selection_live_spread_mult_cap)
                    );
                }
            }
        }
        bid_quote_price = apply_replay_side_policy_price<Side::Buy>(
            bid_quote_price,
            mid,
            bid_policy_mult,
            tick_size
        );
        ask_quote_price = apply_replay_side_policy_price<Side::Sell>(
            ask_quote_price,
            mid,
            ask_policy_mult,
            tick_size
        );
        if (role_safe_ber_applied) {
            global_ber_reference_bid = apply_replay_side_policy_price<Side::Buy>(
                global_ber_reference_bid,
                mid,
                bid_policy_mult,
                tick_size
            );
            global_ber_reference_ask = apply_replay_side_policy_price<Side::Sell>(
                global_ber_reference_ask,
                mid,
                ask_policy_mult,
                tick_size
            );
            ber_bypass_reference_bid = apply_replay_side_policy_price<Side::Buy>(
                ber_bypass_reference_bid,
                mid,
                bid_policy_mult,
                tick_size
            );
            ber_bypass_reference_ask = apply_replay_side_policy_price<Side::Sell>(
                ber_bypass_reference_ask,
                mid,
                ask_policy_mult,
                tick_size
            );
        }
        bool random_passive_mirrored = false;
        const bool random_mirror_eligible =
            params.random_passive_enabled &&
            (!params.random_passive_preserve_inventory_skew ||
             std::abs(inventory) <= lot_size * 0.5);
        if (random_mirror_eligible) {
            ++summary.random_passive_mirror_eligible_count;
            const double mirror_prob = clamp(params.random_passive_side_mirror_prob, 0.0, 1.0);
            if (keyed_random_passive_unit(
                    params.random_passive_seed,
                    ts,
                    static_cast<std::int64_t>(i),
                    RandomPassiveOperation::SideMirror
                ) < mirror_prob) {
                const double bid_distance = std::max(mid - bid_quote_price, tick_size);
                const double ask_distance = std::max(ask_quote_price - mid, tick_size);
                // Python's executable random-passive arm intentionally uses
                // raw floor/ceil here rather than quote_core's tolerant tick
                // helpers. Preserve that exact randomized action identity.
                const double mirrored_bid =
                    std::floor((mid - ask_distance) / tick_size) * tick_size;
                const double mirrored_ask =
                    std::ceil((mid + bid_distance) / tick_size) * tick_size;
                bid_quote_price = std::min(
                    mirrored_bid, mid - tick_size);
                ask_quote_price = std::max(
                    mirrored_ask, mid + tick_size);
                ++summary.random_passive_mirror_count;
                random_passive_mirrored = true;
                if (role_safe_ber_applied) {
                    const double global_bid_distance = std::max(
                        mid - global_ber_reference_bid, tick_size
                    );
                    const double global_ask_distance = std::max(
                        global_ber_reference_ask - mid, tick_size
                    );
                    global_ber_reference_bid = std::min(
                        std::floor(
                            (mid - global_ask_distance) / tick_size
                        ) * tick_size,
                        mid - tick_size
                    );
                    global_ber_reference_ask = std::max(
                        std::ceil(
                            (mid + global_bid_distance) / tick_size
                        ) * tick_size,
                        mid + tick_size
                    );

                    const double bypass_bid_distance = std::max(
                        mid - ber_bypass_reference_bid, tick_size
                    );
                    const double bypass_ask_distance = std::max(
                        ber_bypass_reference_ask - mid, tick_size
                    );
                    ber_bypass_reference_bid = std::min(
                        std::floor(
                            (mid - bypass_ask_distance) / tick_size
                        ) * tick_size,
                        mid - tick_size
                    );
                    ber_bypass_reference_ask = std::max(
                        std::ceil(
                            (mid + bypass_bid_distance) / tick_size
                        ) * tick_size,
                        mid + tick_size
                    );
                }
            }
        }
        bool post_policy_cap_hit = false;
        bool post_policy_cap_compressed = false;
        bool role_safe_cap_feasible = true;
        bool role_safe_cap_collision = false;
        double global_ref_bid_cap = global_ber_reference_bid;
        double global_ref_ask_cap = global_ber_reference_ask;
        bool global_ref_cap_hit = false;
        if (quote.max_spread > 0.0) {
            if (role_safe_ber_applied) {
                const auto global_capped = apply_final_spread_cap(
                    mid,
                    global_ber_reference_bid,
                    global_ber_reference_ask,
                    quote.max_spread,
                    tick_size
                );
                global_ref_bid_cap = std::get<0>(global_capped);
                global_ref_ask_cap = std::get<1>(global_capped);
                global_ref_cap_hit = std::get<2>(global_capped);
            }

            const bool preserve_sell =
                role_safe_ber_applied &&
                ber_buy_role == BerInventoryRole::Add &&
                ber_sell_role == BerInventoryRole::Reducing;
            const bool preserve_buy =
                role_safe_ber_applied &&
                ber_sell_role == BerInventoryRole::Add &&
                ber_buy_role == BerInventoryRole::Reducing;
            double capped_bid = bid_quote_price;
            double capped_ask = ask_quote_price;
            if (preserve_sell || preserve_buy) {
                const auto capped = apply_role_safe_spread_cap(
                    mid,
                    bid_quote_price,
                    ask_quote_price,
                    quote.max_spread,
                    tick_size,
                    preserve_sell ? Side::Sell : Side::Buy
                );
                capped_bid = capped.bid;
                capped_ask = capped.ask;
                post_policy_cap_hit = capped.hit;
                role_safe_cap_feasible = capped.feasible;
                role_safe_cap_collision = capped.hit;
                summary.ber_role_safe_cap_collision_count += capped.hit ? 1 : 0;
                summary.ber_role_safe_cap_infeasible_count +=
                    capped.hit && !capped.feasible ? 1 : 0;
            } else {
                const auto capped = apply_final_spread_cap(
                    mid,
                    bid_quote_price,
                    ask_quote_price,
                    quote.max_spread,
                    tick_size
                );
                capped_bid = std::get<0>(capped);
                capped_ask = std::get<1>(capped);
                post_policy_cap_hit = std::get<2>(capped);
            }
            if (post_policy_cap_hit) {
                if (quote_cfg.spread_cap_mode == 0) {
                    if (role_safe_cap_collision && !role_safe_cap_feasible) {
                        bid_quote_price = global_ref_bid_cap;
                        ask_quote_price = global_ref_ask_cap;
                    } else {
                        bid_quote_price = capped_bid;
                        ask_quote_price = capped_ask;
                    }
                    post_policy_cap_compressed = true;
                } else if (quote_cfg.spread_cap_mode == 1) {
                    bid_quote_context.cap_exposure_block = inventory >= 0.0;
                    ask_quote_context.cap_exposure_block = inventory <= 0.0;
                }
            }
        }
        if (role_safe_ber_applied && quote_cfg.spread_cap_mode == 0 &&
            global_ref_cap_hit) {
            global_ber_reference_bid = global_ref_bid_cap;
            global_ber_reference_ask = global_ref_ask_cap;
        }
        if (role_safe_ber_applied) {
            const bool bid_changed = !same_price_tick(
                bid_quote_price, global_ber_reference_bid, tick_size
            );
            const bool ask_changed = !same_price_tick(
                ask_quote_price, global_ber_reference_ask, tick_size
            );
            summary.ber_role_safe_bid_change_count += bid_changed ? 1 : 0;
            summary.ber_role_safe_ask_change_count += ask_changed ? 1 : 0;
            summary.ber_role_safe_pair_change_count +=
                bid_changed || ask_changed ? 1 : 0;

            if (role_safe_cap_feasible) {
                const bool preserve_sell =
                    ber_buy_role == BerInventoryRole::Add &&
                    ber_sell_role == BerInventoryRole::Reducing;
                const bool preserve_buy =
                    ber_sell_role == BerInventoryRole::Add &&
                    ber_buy_role == BerInventoryRole::Reducing;
                if ((preserve_sell && !same_price_tick(
                        ask_quote_price,
                        ber_bypass_reference_ask,
                        tick_size
                    )) ||
                    (preserve_buy && !same_price_tick(
                        bid_quote_price,
                        ber_bypass_reference_bid,
                        tick_size
                    ))) {
                    ++summary.ber_role_safe_source_mismatch_count;
                }
            }
        }
        if (fixed_spread_probe) {
            const double bid_ttl_ms = bid_quote_context.order_ttl_ms;
            const double ask_ttl_ms = ask_quote_context.order_ttl_ms;
            bid_quote_price = quote.bid_price;
            ask_quote_price = quote.ask_price;
            bid_quote_context = SideQuoteContext{};
            ask_quote_context = SideQuoteContext{};
            bid_quote_context.raw_price = bid_quote_price;
            bid_quote_context.pre_guard_price = bid_quote_price;
            bid_quote_context.raw_quote_delta_to_bbo =
                decision_book.best_bid - bid_quote_price;
            bid_quote_context.pre_guard_delta_to_bbo =
                bid_quote_context.raw_quote_delta_to_bbo;
            bid_quote_context.raw_distance_to_mid = mid - bid_quote_price;
            bid_quote_context.near_depth_total = near_depth;
            bid_quote_context.l2_near_depth_total = l2_metrics.near_depth_total;
            bid_quote_context.l2_quote_flip_rate = l2_metrics.quote_flip_rate;
            bid_quote_context.l2_book_refresh_ratio = l2_metrics.book_refresh_ratio;
            bid_quote_context.l2_book_cancel_ratio = l2_metrics.book_cancel_ratio;
            bid_quote_context.order_ttl_ms = bid_ttl_ms;
            ask_quote_context.raw_price = ask_quote_price;
            ask_quote_context.pre_guard_price = ask_quote_price;
            ask_quote_context.raw_quote_delta_to_bbo =
                ask_quote_price - decision_book.best_ask;
            ask_quote_context.pre_guard_delta_to_bbo =
                ask_quote_context.raw_quote_delta_to_bbo;
            ask_quote_context.raw_distance_to_mid = ask_quote_price - mid;
            ask_quote_context.near_depth_total = near_depth;
            ask_quote_context.l2_near_depth_total = l2_metrics.near_depth_total;
            ask_quote_context.l2_quote_flip_rate = l2_metrics.quote_flip_rate;
            ask_quote_context.l2_book_refresh_ratio = l2_metrics.book_refresh_ratio;
            ask_quote_context.l2_book_cancel_ratio = l2_metrics.book_cancel_ratio;
            ask_quote_context.order_ttl_ms = ask_ttl_ms;
            post_policy_cap_hit = false;
            post_policy_cap_compressed = false;
        }
        refresh_final_context(
            bid_quote_context,
            ask_quote_context,
            mid,
            decision_book.best_bid,
            decision_book.best_ask,
            bid_quote_price,
            ask_quote_price,
            tick_size
        );

        if (quote.flags.cap_hit || post_policy_cap_hit) {
            ++summary.cap_hit_count;
        }
        if (quote.flags.delta_cap) {
            ++summary.delta_cap_hit_count;
        }
        if (quote.flags.final_compressed || post_policy_cap_compressed) {
            ++summary.final_cap_compress_count;
        }
        if (quote.final_cap_rounding) {
            ++summary.final_cap_rounding_count;
        }
        if (quote.final_cap_mid_guard) {
            ++summary.final_cap_mid_guard_count;
        }
        if (quote.final_cap_post_only || quote.flags.post_only) {
            ++summary.final_cap_post_only_count;
        }
        if (quote.final_cap_delta || quote.flags.delta_cap) {
            ++summary.final_cap_delta_count;
        }
        if (quote.flags.post_only) {
            ++summary.post_only_guard_hits;
        }
        if (quote.flags.bid_adverse) {
            ++summary.adverse_guard_count;
        }
        if (quote.flags.ask_adverse) {
            ++summary.adverse_guard_count;
        }
        if (quote.buy.side_adverse_pause) {
            ++summary.adverse_pause_count;
        }
        if (quote.sell.side_adverse_pause) {
            ++summary.adverse_pause_count;
        }
        if (quote.buy.defense_guard) {
            ++summary.defense_guard_count;
        }
        if (quote.sell.defense_guard) {
            ++summary.defense_guard_count;
        }
        if (quote.buy.defense_pause) {
            ++summary.defense_pause_count;
        }
        if (quote.sell.defense_pause) {
            ++summary.defense_pause_count;
        }
        if (quote.delta_after_cap < 100.0) {
            ++summary.quote_spread_lt_100_count;
        }
        if (quote.delta_after_cap < 150.0) {
            ++summary.quote_spread_lt_150_count;
        }
        if (ber_active) {
            ++summary.ber_active_count;
        }

        // Side-adverse and local-extreme pauses veto only exposure-increasing
        // quotes. The common policy below preserves a reducing quote, matching
        // Python/live. Defense remains an all-side hard pause through its
        // reason mask.
        bool bid_allowed = inventory < params.max_inventory;
        bool ask_allowed = inventory > -params.max_inventory;
        constexpr std::uint32_t common_hard_pause_mask =
            PolicyReasonStaleHard | PolicyReasonDefense;
        if ((bid_common_policy.reason_mask & common_hard_pause_mask) != 0U) {
            bid_allowed = false;
        }
        if ((ask_common_policy.reason_mask & common_hard_pause_mask) != 0U) {
            ask_allowed = false;
        }
        if (inventory >= 0.0 && !bid_common_policy.allow_exposure_increase) {
            bid_allowed = false;
        }
        if (inventory <= 0.0 && !ask_common_policy.allow_exposure_increase) {
            ask_allowed = false;
        }

        if (flat_unilateral_max_ms > 0 && std::abs(inventory) <= lot_size * 0.5) {
            const bool bid_blocked =
                !bid_allowed && inventory >= 0.0 && order_size >= lot_size && inventory < params.max_inventory;
            const bool ask_blocked =
                !ask_allowed && inventory <= 0.0 && order_size >= lot_size && inventory > -params.max_inventory;
            if (bid_blocked && ask_allowed && !ask_blocked) {
                if (flat_unilateral_bid_started_ms <= 0) {
                    flat_unilateral_bid_started_ms = ts;
                }
                if (ts - flat_unilateral_bid_started_ms >= flat_unilateral_max_ms) {
                    bid_allowed = true;
                    ++summary.flat_unilateral_release_count;
                    ++summary.flat_unilateral_bid_release_count;
                }
                flat_unilateral_ask_started_ms = 0;
            } else if (ask_blocked && bid_allowed && !bid_blocked) {
                if (flat_unilateral_ask_started_ms <= 0) {
                    flat_unilateral_ask_started_ms = ts;
                }
                if (ts - flat_unilateral_ask_started_ms >= flat_unilateral_max_ms) {
                    ask_allowed = true;
                    ++summary.flat_unilateral_release_count;
                    ++summary.flat_unilateral_ask_release_count;
                }
                flat_unilateral_bid_started_ms = 0;
            } else {
                if (!bid_blocked) {
                    flat_unilateral_bid_started_ms = 0;
                }
                if (!ask_blocked) {
                    flat_unilateral_ask_started_ms = 0;
                }
            }
        } else {
            flat_unilateral_bid_started_ms = 0;
            flat_unilateral_ask_started_ms = 0;
        }

        // A spread cap is a risk threshold in pause_exposure mode. Apply this
        // after flat-unilateral release so the safety TTL cannot reopen a side
        // whose required quote width still exceeds the cap.
        if (bid_quote_context.cap_exposure_block && inventory >= 0.0) {
            bid_allowed = false;
            ++summary.cap_exposure_block_count;
            ++summary.bid_cap_exposure_block_count;
        }
        if (ask_quote_context.cap_exposure_block && inventory <= 0.0) {
            ask_allowed = false;
            ++summary.cap_exposure_block_count;
            ++summary.ask_cap_exposure_block_count;
        }

        if (params.campaign_stop_add_enabled) {
            const bool bid_campaign_stop =
                bid_allowed &&
                campaign_exposure_risk_active<Side::Buy>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    params.campaign_stop_add_inv_threshold,
                    params.campaign_stop_add_age_s
                );
            const bool ask_campaign_stop =
                ask_allowed &&
                campaign_exposure_risk_active<Side::Sell>(
                    params,
                    inventory,
                    lot_size,
                    ts,
                    pos_open_ts,
                    params.campaign_stop_add_inv_threshold,
                    params.campaign_stop_add_age_s
                );
            if (bid_campaign_stop) {
                bid_allowed = false;
                ++summary.campaign_stop_add_count;
                ++summary.bid_campaign_stop_add_count;
            }
            if (ask_campaign_stop) {
                ask_allowed = false;
                ++summary.campaign_stop_add_count;
                ++summary.ask_campaign_stop_add_count;
            }
        }

        if (consecutive_buy_fills > 1e-10 && last_buy_fill_cooldown_ms > 0.0) {
            if (static_cast<double>(ts - last_buy_fill_ts) >= last_buy_fill_cooldown_ms) {
                if (fill_cooldown_reset_consec_on_expiry) {
                    consecutive_buy_fills = 0.0;
                }
                last_buy_fill_cooldown_ms = 0.0;
            } else {
                bool block_bid = true;
                if (cooldown_duration_target_override_active &&
                    cooldown_fork_trace.side == Side::Buy && inventory < 0.0) {
                    ++cooldown_fork_trace.reducing_permission_control_checks;
                    block_bid = ts < cooldown_duration_target_control_deadline_ts_ms;
                }
                if (block_bid) {
                    bid_allowed = false;
                    ++summary.fill_cooldown_bid_block_count;
                }
            }
        }
        if (consecutive_sell_fills > 1e-10 && last_sell_fill_cooldown_ms > 0.0) {
            if (static_cast<double>(ts - last_sell_fill_ts) >= last_sell_fill_cooldown_ms) {
                if (fill_cooldown_reset_consec_on_expiry) {
                    consecutive_sell_fills = 0.0;
                }
                last_sell_fill_cooldown_ms = 0.0;
            } else {
                bool block_ask = true;
                if (cooldown_duration_target_override_active &&
                    cooldown_fork_trace.side == Side::Sell && inventory > 0.0) {
                    ++cooldown_fork_trace.reducing_permission_control_checks;
                    block_ask = ts < cooldown_duration_target_control_deadline_ts_ms;
                }
                if (block_ask) {
                    ask_allowed = false;
                    ++summary.fill_cooldown_ask_block_count;
                }
            }
        }

        if (cooldown_duration_target_override_active) {
            const bool target_is_buy = cooldown_fork_trace.side == Side::Buy;
            const bool target_is_exposure = target_is_buy
                ? inventory >= 0.0
                : inventory <= 0.0;
            if (target_is_exposure) {
                const bool candidate_active = target_is_buy
                    ? (last_buy_fill_cooldown_ms > 0.0 &&
                       static_cast<double>(ts - last_buy_fill_ts) <
                           last_buy_fill_cooldown_ms)
                    : (last_sell_fill_cooldown_ms > 0.0 &&
                       static_cast<double>(ts - last_sell_fill_ts) <
                           last_sell_fill_cooldown_ms);
                const bool control_active =
                    ts < cooldown_duration_target_control_deadline_ts_ms;
                if (candidate_active != control_active) {
                    ++cooldown_fork_trace.exposure_permission_change_count;
                }
            }
        }

        if (cooldown_duration_fork_quarantine) {
            if (inventory >= -lot_size * 0.5) {
                bid_allowed = false;
            }
            if (inventory <= lot_size * 0.5) {
                ask_allowed = false;
            }
        }

        if (!fixed_spread_probe && params.sync_adjust_degrade_enabled &&
            ts < sync_adjust_degrade_until_ms) {
            if (inventory >= -lot_size * 0.5 && bid_allowed) {
                bid_allowed = false;
                ++summary.sync_adjust_degrade_block_bid_count;
            }
            if (inventory <= lot_size * 0.5 && ask_allowed) {
                ask_allowed = false;
                ++summary.sync_adjust_degrade_block_ask_count;
            }
        }

        if (fixed_spread_probe) {
            bid_allowed = true;
            ask_allowed = true;
        } else {
            bid_allowed =
                bid_allowed && fill_gate(params.maker_fill_prob * buy_fill_prob_now, rng);
            ask_allowed =
                ask_allowed && fill_gate(params.maker_fill_prob * sell_fill_prob_now, rng);
        }

        double bid_size = order_size;
        double ask_size = order_size;
        if (!fixed_spread_probe) {
            if (params.eta > 0.0 && params.max_inventory > 1e-10) {
                const double q_norm = inventory / params.max_inventory;
                if (inventory > 0.0) {
                    bid_size = std::max(lot_size, floor_lot(order_size * std::exp(-params.eta * q_norm), lot_size));
                } else if (inventory < 0.0) {
                    ask_size = std::max(lot_size, floor_lot(order_size * std::exp(params.eta * q_norm), lot_size));
                }
            }
            if (params.symmetric_size) {
                const double mirrored = std::min(bid_size, ask_size);
                bid_size = mirrored;
                ask_size = mirrored;
            }
            const auto apply_common_policy_size = [lot_size](double size, double multiplier) {
                const double raw = size * multiplier;
                if (raw <= 0.0) {
                    return 0.0;
                }
                const double floored = floor_lot(raw, lot_size);
                return size >= lot_size && floored < lot_size ? lot_size : floored;
            };
            bid_size = apply_common_policy_size(bid_size, bid_common_policy.size_mult);
            ask_size = apply_common_policy_size(ask_size, ask_common_policy.size_mult);
            if (inventory > 0.0) {
                const double room = floor_lot(std::max(0.0, params.max_inventory - inventory), lot_size);
                bid_size = room >= lot_size ? std::min(bid_size, room) : 0.0;
            } else if (inventory < -lot_size) {
                const double close_cap = floor_lot(std::abs(inventory), lot_size);
                if (close_cap >= lot_size) {
                    bid_size = std::min(bid_size, close_cap);
                }
            }
            if (inventory < 0.0) {
                const double room = floor_lot(std::max(0.0, params.max_inventory - std::abs(inventory)), lot_size);
                ask_size = room >= lot_size ? std::min(ask_size, room) : 0.0;
            } else if (inventory > lot_size) {
                const double close_cap = floor_lot(inventory, lot_size);
                if (close_cap >= lot_size) {
                    ask_size = std::min(ask_size, close_cap);
                }
            }
        }
        const double capped_bid_size = cap_notional_qty(true, bid_size, mid);
        const double capped_ask_size = cap_notional_qty(false, ask_size, mid);
        summary.risk_notional_cap_count += (capped_bid_size + 1e-12 < bid_size)
            + (capped_ask_size + 1e-12 < ask_size);
        bid_size = capped_bid_size;
        ask_size = capped_ask_size;
        bid_allowed = bid_allowed && bid_size >= lot_size;
        ask_allowed = ask_allowed && ask_size >= lot_size;

        if (!fixed_spread_probe &&
            params.cross_venue_fair_center_shift_enabled) {
            ++summary.fair_center_eval_count;
            fair_center_idx = advance_index(
                input.fair_center_ts_ms,
                fair_center_idx,
                ts
            );
            const auto fair_ready_ts =
                input.fair_center_ts_ms.data()[fair_center_idx];
            const auto fair_age_ms = static_cast<double>(ts - fair_ready_ts);
            const bool fair_valid =
                fair_ready_ts <= ts &&
                fair_age_ms <= params.cross_venue_fair_center_max_state_age_ms &&
                input.fair_center_valid.data()[fair_center_idx] == 1 &&
                std::isfinite(input.fair_center_price.data()[fair_center_idx]) &&
                input.fair_center_price.data()[fair_center_idx] > 0.0 &&
                std::isfinite(input.fair_center_gain.data()[fair_center_idx]) &&
                input.fair_center_gain.data()[fair_center_idx] >= 0.0 &&
                input.fair_center_gain.data()[fair_center_idx] <= 1.0;
            if (!fair_valid) {
                ++summary.fair_center_invalid_count;
            } else {
                ++summary.fair_center_valid_count;
                const double center_shift_price =
                    input.fair_center_gain.data()[fair_center_idx] *
                    (input.fair_center_price.data()[fair_center_idx] - mid);
                const auto requested_shift_ticks =
                    round_ties_to_even(center_shift_price / tick_size);
                if (requested_shift_ticks != 0) {
                    ++summary.fair_center_nonzero_request_count;
                }

                const auto baseline_bid_tick =
                    price_to_tick(bid_quote_price, tick_size);
                const auto baseline_ask_tick =
                    price_to_tick(ask_quote_price, tick_size);
                const auto best_bid_tick =
                    price_to_tick(decision_book.best_bid, tick_size);
                const auto best_ask_tick =
                    price_to_tick(decision_book.best_ask, tick_size);
                const auto minimum_shift_ticks =
                    best_bid_tick + 1 - baseline_ask_tick;
                const auto maximum_shift_ticks =
                    best_ask_tick - 1 - baseline_bid_tick;
                if (minimum_shift_ticks > maximum_shift_ticks) {
                    ++summary.fair_center_no_pair_support_count;
                } else {
                    const auto effective_shift_ticks = std::clamp(
                        requested_shift_ticks,
                        minimum_shift_ticks,
                        maximum_shift_ticks
                    );
                    if (effective_shift_ticks != requested_shift_ticks) {
                        ++summary.fair_center_gtx_clamp_count;
                    }
                    if (effective_shift_ticks != 0 &&
                        (bid_allowed || ask_allowed)) {
                        const auto candidate_bid_tick =
                            baseline_bid_tick + effective_shift_ticks;
                        const auto candidate_ask_tick =
                            baseline_ask_tick + effective_shift_ticks;
                        if (candidate_bid_tick >= best_ask_tick ||
                            candidate_ask_tick <= best_bid_tick ||
                            candidate_ask_tick - candidate_bid_tick !=
                                baseline_ask_tick - baseline_bid_tick) {
                            throw std::runtime_error(
                                "cross-venue fair-center projection violated tick/GTX invariants"
                            );
                        }
                        bid_quote_price =
                            static_cast<double>(candidate_bid_tick) * tick_size;
                        ask_quote_price =
                            static_cast<double>(candidate_ask_tick) * tick_size;
                        ++summary.fair_center_price_change_count;
                        const auto abs_shift_ticks = std::abs(effective_shift_ticks);
                        summary.fair_center_effective_shift_ticks_abs_sum +=
                            abs_shift_ticks;
                        summary.fair_center_effective_shift_ticks_abs_max = std::max(
                            summary.fair_center_effective_shift_ticks_abs_max,
                            abs_shift_ticks
                        );
                        refresh_final_context(
                            bid_quote_context,
                            ask_quote_context,
                            mid,
                            decision_book.best_bid,
                            decision_book.best_ask,
                            bid_quote_price,
                            ask_quote_price,
                            tick_size
                        );
                    }
                }
            }
        }

        const double p3_baseline_bid_price = bid_quote_price;
        const double p3_baseline_ask_price = ask_quote_price;
        std::uint8_t p3_bid_gate_status = 0;
        std::uint8_t p3_ask_gate_status = 0;
        std::int64_t p3_bid_distance_ticks =
            price_to_tick(decision_book.best_bid, tick_size) -
            price_to_tick(bid_quote_price, tick_size);
        std::int64_t p3_ask_distance_ticks =
            price_to_tick(ask_quote_price, tick_size) -
            price_to_tick(decision_book.best_ask, tick_size);
        bool p3_bid_requested = false;
        bool p3_ask_requested = false;
        bool p3_bid_price_changed = false;
        bool p3_ask_price_changed = false;
        bool p3_bid_spread_cap_noop = false;
        bool p3_ask_spread_cap_noop = false;
        const bool p3_bid_opener = std::abs(inventory) <= lot_size * 0.5;
        const bool p3_ask_opener = p3_bid_opener;
        const bool p3_bid_exposure = inventory >= 0.0;
        const bool p3_ask_exposure = inventory <= 0.0;

        const auto p3_gate_status_for = [&](Side side, bool opener, std::int64_t distance_ticks) {
            if (!ml_ready || input.p3_reach_gate_status.empty() ||
                ml_idx >= input.p3_reach_gate_status.rows) {
                return static_cast<std::uint8_t>(0);
            }
            const std::size_t grid_size = input.p3_reach_gate_status.cols / 4;
            const std::int64_t offset =
                distance_ticks - params.conditional_p3_reach_gate_grid_min_ticks;
            if (offset < 0 || static_cast<std::size_t>(offset) >= grid_size) {
                return static_cast<std::uint8_t>(0);
            }
            const std::size_t block = side == Side::Buy
                ? (opener ? 0U : 1U)
                : (opener ? 2U : 3U);
            return input.p3_reach_gate_status(
                ml_idx,
                block * grid_size + static_cast<std::size_t>(offset)
            );
        };

        if (!fixed_spread_probe && params.conditional_p3_reach_gate_enabled) {
            const auto bid_price_tick = price_to_tick(bid_quote_price, tick_size);
            const auto ask_price_tick = price_to_tick(ask_quote_price, tick_size);
            if (bid_allowed && p3_bid_exposure) {
                ++summary.p3_reach_gate_eval_count;
                p3_bid_gate_status = p3_gate_status_for(
                    Side::Buy, p3_bid_opener, p3_bid_distance_ticks);
                if (p3_bid_gate_status > 0) {
                    ++summary.p3_reach_gate_supported_count;
                }
                const bool toxicity_trigger =
                    pred.tox_bid >= params.conditional_p3_reach_gate_buy_toxicity_threshold;
                if (toxicity_trigger) {
                    ++summary.p3_reach_gate_toxicity_trigger_count;
                }
                p3_bid_requested = toxicity_trigger && p3_bid_gate_status == 2;
                if (p3_bid_requested) {
                    ++summary.p3_reach_gate_pass_count;
                }
            }
            if (ask_allowed && p3_ask_exposure) {
                ++summary.p3_reach_gate_eval_count;
                p3_ask_gate_status = p3_gate_status_for(
                    Side::Sell, p3_ask_opener, p3_ask_distance_ticks);
                if (p3_ask_gate_status > 0) {
                    ++summary.p3_reach_gate_supported_count;
                }
                const bool toxicity_trigger =
                    pred.tox_ask >= params.conditional_p3_reach_gate_sell_toxicity_threshold;
                if (toxicity_trigger) {
                    ++summary.p3_reach_gate_toxicity_trigger_count;
                }
                p3_ask_requested = toxicity_trigger && p3_ask_gate_status == 2;
                if (p3_ask_requested) {
                    ++summary.p3_reach_gate_pass_count;
                }
            }

            double proposed_bid = bid_quote_price;
            double proposed_ask = ask_quote_price;
            if (p3_bid_requested) {
                proposed_bid = static_cast<double>(
                    bid_price_tick - params.conditional_p3_reach_gate_outward_ticks
                ) * tick_size;
            }
            if (p3_ask_requested) {
                proposed_ask = static_cast<double>(
                    ask_price_tick + params.conditional_p3_reach_gate_outward_ticks
                ) * tick_size;
            }
            const bool spread_supported =
                quote.max_spread <= 0.0 || proposed_ask - proposed_bid <= quote.max_spread + 1e-12;
            if (spread_supported) {
                if (p3_bid_requested && proposed_bid < bid_quote_price - 1e-12) {
                    bid_quote_price = proposed_bid;
                    p3_bid_price_changed = true;
                    ++summary.p3_reach_gate_price_change_count;
                    ++summary.p3_reach_gate_buy_price_change_count;
                }
                if (p3_ask_requested && proposed_ask > ask_quote_price + 1e-12) {
                    ask_quote_price = proposed_ask;
                    p3_ask_price_changed = true;
                    ++summary.p3_reach_gate_price_change_count;
                    ++summary.p3_reach_gate_sell_price_change_count;
                }
            } else {
                p3_bid_spread_cap_noop = p3_bid_requested;
                p3_ask_spread_cap_noop = p3_ask_requested;
                summary.p3_reach_gate_spread_cap_noop_count +=
                    static_cast<std::int64_t>(p3_bid_requested) +
                    static_cast<std::int64_t>(p3_ask_requested);
            }
            if (p3_bid_price_changed || p3_ask_price_changed) {
                refresh_final_context(
                    bid_quote_context,
                    ask_quote_context,
                    mid,
                    decision_book.best_bid,
                    decision_book.best_ask,
                    bid_quote_price,
                    ask_quote_price,
                    tick_size
                );
            }
        }

        if (!fixed_spread_probe && params.conditional_p3_reach_budget_policy_enabled) {
            const auto prepare_episode = [&](Side side,
                                             bool opener,
                                             bool exposure_increasing,
                                             std::int64_t baseline_distance_ticks,
                                             double toxicity_score,
                                             double toxicity_threshold,
                                             P3ReachBudgetEpisode& episode) {
                if (!exposure_increasing || !ml_ready) {
                    return false;
                }
                const auto bucket_start = input.ml_ts_ms.data()[ml_idx];
                if (ts < bucket_start || ts >= bucket_start + kP3ReachBudgetBucketMs) {
                    return false;
                }
                if (episode.last_evaluated_bucket_ms == bucket_start) {
                    return false;
                }
                episode.last_evaluated_bucket_ms = bucket_start;
                ++summary.p3_reach_budget_bucket_eval_count;
                if (!(toxicity_score >= toxicity_threshold)) {
                    episode.deactivate();
                    ++summary.p3_reach_budget_no_action_count;
                    return false;
                }
                ++summary.p3_reach_budget_toxicity_trigger_count;
                const std::size_t grid_size = input.p3_reach_budget_selected_k.cols / 4;
                const auto offset = baseline_distance_ticks -
                    params.conditional_p3_reach_budget_grid_min_ticks;
                if (offset < 0 || static_cast<std::size_t>(offset) >= grid_size) {
                    episode.deactivate();
                    ++summary.p3_reach_budget_unsupported_count;
                    return false;
                }
                const std::size_t block = side == Side::Buy
                    ? (opener ? 0U : 1U)
                    : (opener ? 2U : 3U);
                const std::size_t column =
                    block * grid_size + static_cast<std::size_t>(offset);
                const auto selected_k = input.p3_reach_budget_selected_k(ml_idx, column);
                if (selected_k == kP3ReachBudgetUnsupported) {
                    episode.deactivate();
                    ++summary.p3_reach_budget_unsupported_count;
                    return false;
                }
                if (selected_k == 0) {
                    episode.deactivate();
                    ++summary.p3_reach_budget_no_action_count;
                    return false;
                }
                episode.active_bucket_ms = bucket_start;
                episode.selected_k = selected_k;
                episode.active = true;
                episode.saw_nonflat_inventory = std::abs(inventory) > 1e-10;
                ++summary.p3_reach_budget_activation_count;
                summary.p3_reach_budget_selected_k_sum += selected_k;
                summary.p3_reach_budget_selected_k_max = std::max<std::int64_t>(
                    summary.p3_reach_budget_selected_k_max,
                    selected_k
                );
                if (side == Side::Buy) {
                    ++summary.p3_reach_budget_buy_activation_count;
                } else {
                    ++summary.p3_reach_budget_sell_activation_count;
                }
                return true;
            };

            const bool bid_activated_now = prepare_episode(
                Side::Buy,
                p3_bid_opener,
                p3_bid_exposure,
                p3_bid_distance_ticks,
                pred.tox_bid,
                params.conditional_p3_reach_budget_buy_toxicity_threshold,
                p3_reach_budget_buy_episode
            );
            const bool ask_activated_now = prepare_episode(
                Side::Sell,
                p3_ask_opener,
                p3_ask_exposure,
                p3_ask_distance_ticks,
                pred.tox_ask,
                params.conditional_p3_reach_budget_sell_toxicity_threshold,
                p3_reach_budget_sell_episode
            );
            const bool bid_episode_exposure =
                p3_reach_budget_buy_episode.active && p3_bid_exposure;
            const bool ask_episode_exposure =
                p3_reach_budget_sell_episode.active && p3_ask_exposure;
            if (p3_reach_budget_buy_episode.active && !p3_bid_exposure) {
                ++summary.p3_reach_budget_reducing_unchanged_count;
            }
            if (p3_reach_budget_sell_episode.active && !p3_ask_exposure) {
                ++summary.p3_reach_budget_reducing_unchanged_count;
            }
            if (bid_episode_exposure) {
                ++summary.p3_reach_budget_exposure_decision_count;
                if (!bid_activated_now) {
                    ++summary.p3_reach_budget_reuse_count;
                }
                if (!bid_allowed) {
                    ++summary.p3_reach_budget_hard_safety_suppressed_count;
                }
            }
            if (ask_episode_exposure) {
                ++summary.p3_reach_budget_exposure_decision_count;
                if (!ask_activated_now) {
                    ++summary.p3_reach_budget_reuse_count;
                }
                if (!ask_allowed) {
                    ++summary.p3_reach_budget_hard_safety_suppressed_count;
                }
            }

            const bool bid_requested = bid_episode_exposure && bid_allowed;
            const bool ask_requested = ask_episode_exposure && ask_allowed;
            const auto baseline_bid_tick = price_to_tick(bid_quote_price, tick_size);
            const auto baseline_ask_tick = price_to_tick(ask_quote_price, tick_size);
            const auto proposed_bid_tick = bid_requested
                ? baseline_bid_tick - p3_reach_budget_buy_episode.selected_k
                : baseline_bid_tick;
            const auto proposed_ask_tick = ask_requested
                ? baseline_ask_tick + p3_reach_budget_sell_episode.selected_k
                : baseline_ask_tick;
            const double proposed_bid = static_cast<double>(proposed_bid_tick) * tick_size;
            const double proposed_ask = static_cast<double>(proposed_ask_tick) * tick_size;
            const bool spread_supported = quote.max_spread <= 0.0 ||
                proposed_ask - proposed_bid <= quote.max_spread + 1e-12;
            bool price_changed = false;
            if (spread_supported) {
                if (bid_requested && proposed_bid_tick < baseline_bid_tick) {
                    bid_quote_price = proposed_bid;
                    price_changed = true;
                    ++summary.p3_reach_budget_price_change_count;
                    ++summary.p3_reach_budget_buy_price_change_count;
                }
                if (ask_requested && proposed_ask_tick > baseline_ask_tick) {
                    ask_quote_price = proposed_ask;
                    price_changed = true;
                    ++summary.p3_reach_budget_price_change_count;
                    ++summary.p3_reach_budget_sell_price_change_count;
                }
            } else {
                summary.p3_reach_budget_spread_cap_noop_count +=
                    static_cast<std::int64_t>(bid_requested) +
                    static_cast<std::int64_t>(ask_requested);
            }
            if (price_changed) {
                refresh_final_context(
                    bid_quote_context,
                    ask_quote_context,
                    mid,
                    decision_book.best_bid,
                    decision_book.best_ask,
                    bid_quote_price,
                    ask_quote_price,
                    tick_size
                );
            }
        }

        if (paired_fixed_spread_probe) {
            paired_probe.on_decision_cancel(ts);
            paired_probe.create_cohort<Side::Buy>(
                ts,
                i,
                decision_book.best_bid,
                decision_book.best_ask,
                mid,
                queue_base_now,
                queue_decay_now,
                bid_quote_context
            );
            paired_probe.create_cohort<Side::Sell>(
                ts,
                i,
                decision_book.best_bid,
                decision_book.best_ask,
                mid,
                queue_base_now,
                queue_decay_now,
                ask_quote_context
            );
            // The paired ladder is a shadow opportunity panel. Do not submit a
            // scalar strategy order whose fills could mutate inventory,
            // cooldown, or the next decision.
            bid_allowed = false;
            ask_allowed = false;
        }

        const auto* bid_ref_order = best_live_order<Side::Buy>(bid_orders, lot_size);
        const auto* ask_ref_order = best_live_order<Side::Sell>(ask_orders, lot_size);
        const auto* bid_pending_order = pending_lifecycle_order(bid_orders, lot_size);
        const auto* ask_pending_order = pending_lifecycle_order(ask_orders, lot_size);
        const bool bid_active_before = bid_ref_order != nullptr;
        const bool ask_active_before = ask_ref_order != nullptr;

        bool bid_updated = true;
        bool ask_updated = true;
        const double rq_threshold = std::max(0.0, params.requote_threshold_bps) / 10000.0;
        if (rq_threshold > 0.0) {
            if (bid_allowed && bid_ref_order != nullptr && bid_ref_order->price > 0.0) {
                const double drift = std::abs(bid_quote_price - bid_ref_order->price) / bid_ref_order->price;
                if (drift <= rq_threshold) {
                    bid_updated = false;
                }
            }
            if (ask_allowed && ask_ref_order != nullptr && ask_ref_order->price > 0.0) {
                const double drift = std::abs(ask_quote_price - ask_ref_order->price) / ask_ref_order->price;
                if (drift <= rq_threshold) {
                    ask_updated = false;
                }
            }
        }

        const double lifecycle_inventory = fixed_spread_probe ? 0.0 : inventory;
        if (bid_allowed) {
            bid_updated = apply_replace_throttle<Side::Buy>(
                params, summary, ts, lifecycle_inventory, bid_quote_price,
                bid_ref_order, bid_updated, tick_size);
        }
        if (ask_allowed) {
            ask_updated = apply_replace_throttle<Side::Sell>(
                params, summary, ts, lifecycle_inventory, ask_quote_price,
                ask_ref_order, ask_updated, tick_size);
        }

        // A safety resize must not be suppressed by drift or replace throttles.
        if (bid_ref_order && cap_notional_qty(true, bid_ref_order->remaining, mid)
                + 1e-12 < bid_ref_order->remaining) bid_updated = true;
        if (ask_ref_order && cap_notional_qty(false, ask_ref_order->remaining, mid)
                + 1e-12 < ask_ref_order->remaining) ask_updated = true;
        const bool bid_pending_coalesce =
            bid_allowed && bid_updated && params.replace_pending_coalesce && bid_pending_order != nullptr;
        const bool ask_pending_coalesce =
            ask_allowed && ask_updated && params.replace_pending_coalesce && ask_pending_order != nullptr;
        if (bid_pending_coalesce) {
            bid_updated = false;
        }
        if (ask_pending_coalesce) {
            ask_updated = false;
        }

        const bool bid_cancel_first = should_cancel_first_replace<Side::Buy>(
            params, lifecycle_inventory, bid_ref_order, bid_updated, bid_allowed);
        const bool ask_cancel_first = should_cancel_first_replace<Side::Sell>(
            params, lifecycle_inventory, ask_ref_order, ask_updated, ask_allowed);

        std::string_view bid_action = "none";
        if (bid_cancel_first) {
            bid_action = "cancel_first";
        } else if (bid_pending_coalesce) {
            bid_action = "pending_coalesce";
        } else if (bid_allowed && bid_updated) {
            bid_action = bid_active_before ? "replace" : "place";
        } else if (!bid_allowed) {
            bid_action = "pause";
        } else if (!bid_updated && bid_active_before) {
            bid_action = "keep";
        }
        std::string_view ask_action = "none";
        if (ask_cancel_first) {
            ask_action = "cancel_first";
        } else if (ask_pending_coalesce) {
            ask_action = "pending_coalesce";
        } else if (ask_allowed && ask_updated) {
            ask_action = ask_active_before ? "replace" : "place";
        } else if (!ask_allowed) {
            ask_action = "pause";
        } else if (!ask_updated && ask_active_before) {
            ask_action = "keep";
        }

        const auto append_p3_reach_trace = [&](Side side, std::string_view action) {
            if (params.trace_p3_reach_decisions_max <= 0 || !ml_ready ||
                result.p3_reach_decision_trace.size() >=
                    static_cast<std::size_t>(params.trace_p3_reach_decisions_max)) {
                return;
            }
            const bool is_buy = side == Side::Buy;
            const bool exposure = is_buy ? p3_bid_exposure : p3_ask_exposure;
            const bool eligible = exposure &&
                (action == "place" || action == "replace" || action == "keep");
            auto& last_index = is_buy
                ? last_p3_trace_ml_idx_buy
                : last_p3_trace_ml_idx_sell;
            if (!eligible || last_index == ml_idx) {
                return;
            }
            last_index = ml_idx;
            result.p3_reach_decision_trace.push_back(P3ReachDecisionRow{
                .decision_ts_ms = ts,
                .prediction_ts_ms = input.ml_ts_ms.data()[ml_idx],
                .side = side,
                .opener = is_buy ? p3_bid_opener : p3_ask_opener,
                .exposure_increasing = exposure,
                .baseline_eligible = eligible,
                .toxicity_score = is_buy ? pred.tox_bid : pred.tox_ask,
                .toxicity_threshold = is_buy
                    ? params.conditional_p3_reach_gate_buy_toxicity_threshold
                    : params.conditional_p3_reach_gate_sell_toxicity_threshold,
                .best_bid = decision_book.best_bid,
                .best_ask = decision_book.best_ask,
                .inventory_btc = inventory,
                .baseline_price = is_buy
                    ? p3_baseline_bid_price
                    : p3_baseline_ask_price,
                .candidate_price = is_buy ? bid_quote_price : ask_quote_price,
                .side_policy_spread_mult = is_buy
                    ? bid_policy_mult
                    : ask_policy_mult,
                .side_policy_allow_post = is_buy
                    ? bid_common_policy.allow_post
                    : ask_common_policy.allow_post,
                .side_policy_allow_exposure_increase = is_buy
                    ? bid_common_policy.allow_exposure_increase
                    : ask_common_policy.allow_exposure_increase,
                .side_policy_reason_mask = is_buy
                    ? bid_common_policy.reason_mask
                    : ask_common_policy.reason_mask,
                .baseline_distance_ticks = is_buy
                    ? p3_bid_distance_ticks
                    : p3_ask_distance_ticks,
                .reach_gate_status = is_buy
                    ? p3_bid_gate_status
                    : p3_ask_gate_status,
                .reach_gate_requested = is_buy
                    ? p3_bid_requested
                    : p3_ask_requested,
                .price_changed = is_buy
                    ? p3_bid_price_changed
                    : p3_ask_price_changed,
                .spread_cap_noop = is_buy
                    ? p3_bid_spread_cap_noop
                    : p3_ask_spread_cap_noop,
            });
        };

        if (bid_cancel_first) {
            if (!bid_orders.empty()) {
                request_cancel_all(
                    bid_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params, &result, params.trace_quotes_max, CancelReason::RequoteReplace);
            }
        } else if (bid_updated) {
            if (!bid_orders.empty()) {
                request_cancel_all(
                    bid_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params, &result, params.trace_quotes_max,
                    bid_allowed
                        ? CancelReason::RequoteReplace
                        : CancelReason::SideDisabled);
            }
            if (bid_allowed && !bid_orders.empty()) {
                bid_action = "pending_coalesce";
            }
            if (bid_allowed && bid_orders.empty()) {
                const auto new_deadlines = sample_new_order_deadlines(
                    ts,
                    params.new_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.new_order_latency_samples_ms,
                    params,
                    Side::Buy,
                    LatencyOperation::NewOrder,
                    ts
                );
                const std::int64_t activate_latency_ms =
                    new_deadlines.effective_ts - ts;
                const std::int64_t ack_latency_ms = new_deadlines.ack_ts - ts;
                const std::int64_t activate_ts = ts + activate_latency_ms;
                TraceOrderPtr trace{nullptr, PmrTraceDeleter{&trace_resource}};
                if (trace_enabled) {
                    auto* trace_row = trace_allocator.new_object<TraceOrderRow>(make_trace_order_row(
                        bid_quote_context,
                        quote,
                        pred,
                        Side::Buy,
                        next_trace_order_id++,
                        ts,
                        activate_ts,
                        bid_quote_price,
                        bid_size,
                        inventory,
                        mid,
                        decision_book.best_bid,
                        decision_book.best_ask,
                        mo_ema_bid,
                        mo_ema_ask,
                        post_policy_cap_compressed,
                        random_passive_mirrored
                    ));
                    trace.reset(trace_row);
                }
                bid_orders.push_back(make_order<Side::Buy>(
                    input,
                    params,
                    bid_quote_price,
                    bid_size,
                    ts,
                    activate_latency_ms,
                    ack_latency_ms,
                    activation_mid,
                    queue_base_now,
                    queue_decay_now,
                    lifecycle_inventory,
                    i,
                    fixed_spread_probe ? 0.0 : mo_ema_bid,
                    bid_quote_context,
                    quote.flags.final_compressed || post_policy_cap_compressed,
                    std::move(trace)
                ));
                if (fixed_spread_probe) {
                    bid_orders.back().fixed_spread_probe = true;
                    ++summary.fixed_spread_probe_bid_submitted_orders;
                }
                if (bid_orders.back().exchange_accepted &&
                    !activate_resting_order(
                        bid_orders.back(),
                        book.best_bid,
                        book.best_ask,
                        tick_size,
                        has_historical_book,
                        summary,
                        circuit_breaker_close_gtx_reject_streak
                    )) {
                    bid_orders.pop_back();
                }
            }
        }

        if (ask_cancel_first) {
            if (!ask_orders.empty()) {
                request_cancel_all(
                    ask_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params, &result, params.trace_quotes_max, CancelReason::RequoteReplace);
            }
        } else if (ask_updated) {
            if (!ask_orders.empty()) {
                request_cancel_all(
                    ask_orders, ts, params.cancel_order_latency_ms, params.latency_jitter_ms,
                    &params.cancel_order_latency_samples_ms,
                    params, &result, params.trace_quotes_max,
                    ask_allowed
                        ? CancelReason::RequoteReplace
                        : CancelReason::SideDisabled);
            }
            if (ask_allowed && !ask_orders.empty()) {
                ask_action = "pending_coalesce";
            }
            if (ask_allowed && ask_orders.empty()) {
                const auto new_deadlines = sample_new_order_deadlines(
                    ts,
                    params.new_order_latency_ms,
                    params.latency_jitter_ms,
                    &params.new_order_latency_samples_ms,
                    params,
                    Side::Sell,
                    LatencyOperation::NewOrder,
                    ts
                );
                const std::int64_t activate_latency_ms =
                    new_deadlines.effective_ts - ts;
                const std::int64_t ack_latency_ms = new_deadlines.ack_ts - ts;
                const std::int64_t activate_ts = ts + activate_latency_ms;
                TraceOrderPtr trace{nullptr, PmrTraceDeleter{&trace_resource}};
                if (trace_enabled) {
                    auto* trace_row = trace_allocator.new_object<TraceOrderRow>(make_trace_order_row(
                        ask_quote_context,
                        quote,
                        pred,
                        Side::Sell,
                        next_trace_order_id++,
                        ts,
                        activate_ts,
                        ask_quote_price,
                        ask_size,
                        inventory,
                        mid,
                        decision_book.best_bid,
                        decision_book.best_ask,
                        mo_ema_bid,
                        mo_ema_ask,
                        post_policy_cap_compressed,
                        random_passive_mirrored
                    ));
                    trace.reset(trace_row);
                }
                ask_orders.push_back(make_order<Side::Sell>(
                    input,
                    params,
                    ask_quote_price,
                    ask_size,
                    ts,
                    activate_latency_ms,
                    ack_latency_ms,
                    activation_mid,
                    queue_base_now,
                    queue_decay_now,
                    lifecycle_inventory,
                    i,
                    fixed_spread_probe ? 0.0 : mo_ema_ask,
                    ask_quote_context,
                    quote.flags.final_compressed || post_policy_cap_compressed,
                    std::move(trace)
                ));
                if (fixed_spread_probe) {
                    ask_orders.back().fixed_spread_probe = true;
                    ++summary.fixed_spread_probe_ask_submitted_orders;
                }
                if (ask_orders.back().exchange_accepted &&
                    !activate_resting_order(
                        ask_orders.back(),
                        book.best_bid,
                        book.best_ask,
                        tick_size,
                        has_historical_book,
                        summary,
                        circuit_breaker_close_gtx_reject_streak
                    )) {
                    ask_orders.pop_back();
                }
            }
        }

        count_decision_action(summary, bid_action);
        count_decision_action(summary, ask_action);
        append_p3_reach_trace(Side::Buy, bid_action);
        append_p3_reach_trace(Side::Sell, ask_action);

        const auto [pending_new_count, pending_cancel_count] =
            pending_order_counts(bid_orders, ask_orders);
        summary.max_pending_new_orders = std::max(summary.max_pending_new_orders, pending_new_count);
        summary.max_pending_cancel_orders =
            std::max(summary.max_pending_cancel_orders, pending_cancel_count);

        const auto* bid_top_order = best_live_order<Side::Buy>(bid_orders, lot_size);
        const auto* ask_top_order = best_live_order<Side::Sell>(ask_orders, lot_size);
        const double final_spread = (bid_top_order != nullptr && ask_top_order != nullptr)
            ? ask_top_order->price - bid_top_order->price
            : 0.0;
        if (final_spread > 0.0) {
            final_spread_sum += final_spread;
            ++final_spread_count;
            if (final_spread < 100.0) {
                ++summary.final_spread_lt_100_count;
            }
            if (final_spread < 150.0) {
                ++summary.final_spread_lt_150_count;
            }
        }
        spread_sum += quote.delta_after_cap;
        quote_mid_state = (!params.use_bar_pricing && has_historical_book && book.mid > 0.0)
            ? mid
            : price;
        ++summary.n_requotes;
        rq_sum += static_cast<double>(current_rq_ms);

        const double current_pnl = cash + inventory * quote_mid_state;
        peak_pnl = std::max(peak_pnl, current_pnl);
        max_drawdown = std::max(max_drawdown, peak_pnl - current_pnl);
        if (pnl_stats_initialized) {
            const double dt_s = static_cast<double>(ts - previous_pnl_ts) / 1000.0;
            if (dt_s > 0.0) {
                const double delta = current_pnl - previous_pnl;
                const double normalized = delta / std::sqrt(dt_s);
                pnl_delta_sum += delta;
                pnl_dt_sum += dt_s;
                normalized_delta_sum += normalized;
                normalized_delta_sq_sum += normalized * normalized;
                ++normalized_delta_count;
            }
        }
        previous_pnl_ts = ts;
        previous_pnl = current_pnl;
        pnl_stats_initialized = true;
        if (params.collect_curves) {
            result.pnl_ts_ms.push_back(ts);
            result.pnl.push_back(current_pnl);
            result.inventory.push_back(inventory);
        }

        if (cooldown_duration_fork_assigned &&
            cooldown_duration_fork_quarantine &&
            std::abs(inventory) <= 1e-10 &&
            cooldown_duration_active_campaign_id == 0 &&
            bid_orders.empty() && ask_orders.empty()) {
            cooldown_duration_fork_terminal = true;
            cooldown_fork_trace.arm_washout_complete = true;
            cooldown_fork_trace.terminal_ts_ms = ts;
            cooldown_fork_trace.terminal_reason =
                "arm_economic_washout";
            break;
        }
    }

    const std::int64_t end_ts = input.trade_ts_ms.data()[last_processed_event_idx];
    paired_probe.finish(end_ts);
    const auto count_probe_end_censor = [&](const ReplayOrder& order) {
        if (!order.fixed_spread_probe ||
            !order.fixed_spread_probe_activated ||
            order.fixed_spread_probe_filled) {
            return;
        }
        const std::int64_t age_ms =
            std::max<std::int64_t>(0, end_ts - order.activate_ts);
        if (order.side == Side::Buy) {
            ++summary.fixed_spread_probe_bid_end_censored_unfilled;
            summary.fixed_spread_probe_bid_end_censored_before_1s +=
                age_ms < 1'000 ? 1 : 0;
            summary.fixed_spread_probe_bid_end_censored_before_5s +=
                age_ms < 5'000 ? 1 : 0;
            summary.fixed_spread_probe_bid_end_censored_before_10s +=
                age_ms < 10'000 ? 1 : 0;
        } else {
            ++summary.fixed_spread_probe_ask_end_censored_unfilled;
            summary.fixed_spread_probe_ask_end_censored_before_1s +=
                age_ms < 1'000 ? 1 : 0;
            summary.fixed_spread_probe_ask_end_censored_before_5s +=
                age_ms < 5'000 ? 1 : 0;
            summary.fixed_spread_probe_ask_end_censored_before_10s +=
                age_ms < 10'000 ? 1 : 0;
        }
    };
    for (const auto& order : bid_orders) {
        count_probe_end_censor(order);
    }
    for (const auto& order : ask_orders) {
        count_probe_end_censor(order);
    }
    if (params.trace_quotes_max > 0) {
        for (const auto& order : bid_orders) {
            if (result.quote_trace.size() >= static_cast<std::size_t>(params.trace_quotes_max)) {
                break;
            }
            append_order_trace(result, order, end_ts, TraceOutcome::OpenEnd, CancelReason::EndOfWindow, 0.0);
        }
        for (const auto& order : ask_orders) {
            if (result.quote_trace.size() >= static_cast<std::size_t>(params.trace_quotes_max)) {
                break;
            }
            append_order_trace(result, order, end_ts, TraceOutcome::OpenEnd, CancelReason::EndOfWindow, 0.0);
        }
    }

    const double final_price = input.trade_price.data()[last_processed_event_idx];
    const auto cooldown_duration_end_book = book_snapshot_at(
        input,
        end_ts,
        bbo_idx,
        l2_idx,
        inferred_best_bid,
        inferred_best_ask,
        tick_size,
        params
    );
    const bool cooldown_duration_has_historical_book =
        input.bbo_ts_ms.size() > 0 || input.l2_ts_ms.size() > 0;
    const double cooldown_duration_end_mid =
        !params.use_bar_pricing && cooldown_duration_has_historical_book &&
            cooldown_duration_end_book.mid > 0.0
        ? cooldown_duration_end_book.mid
        : final_price;
    if (params.cooldown_duration_fork_enabled) {
        if (!cooldown_duration_fork_assigned) {
            throw std::runtime_error(
                "cooldown duration fork target was never reached"
            );
        }
        update_cooldown_duration_fork_path(
            end_ts,
            cooldown_duration_end_mid
        );
        cooldown_fork_trace.right_censored =
            !cooldown_duration_fork_terminal;
        if (cooldown_fork_trace.right_censored) {
            cooldown_fork_trace.terminal_ts_ms = end_ts;
            cooldown_fork_trace.terminal_reason =
                "data_boundary_right_censored";
        }
        cooldown_fork_trace.terminal_inventory_btc = inventory;
        cooldown_fork_trace.terminal_mid_usdc_per_btc =
            cooldown_duration_end_mid;
        cooldown_fork_trace.final_cash_usdc = cash;
        cooldown_fork_trace.final_pnl_usdc =
            cash + inventory * cooldown_duration_end_mid;
        const double assignment_delta_mid =
            cooldown_fork_trace.final_pnl_usdc -
            cooldown_fork_trace.assignment_equity_usdc;
        double executable_equity = cooldown_fork_trace.final_pnl_usdc;
        if (inventory > 0.0 && cooldown_duration_end_book.best_bid > 0.0) {
            executable_equity =
                cash + inventory * cooldown_duration_end_book.best_bid;
        } else if (
            inventory < 0.0 &&
            cooldown_duration_end_book.best_ask >
                cooldown_duration_end_book.best_bid &&
            cooldown_duration_end_book.best_bid > 0.0) {
            executable_equity =
                cash + inventory * cooldown_duration_end_book.best_ask;
        }
        if (cooldown_duration_fork_terminal) {
            cooldown_fork_trace.assignment_to_washout_value_usdc =
                assignment_delta_mid;
            cooldown_fork_trace.accounting_residual_usdc =
                (cooldown_fork_trace.final_cash_usdc -
                 cooldown_fork_trace.assignment_equity_usdc) -
                assignment_delta_mid;
            cooldown_fork_trace.censor_time_mid_mark_usdc.reset();
            cooldown_fork_trace.censor_time_executable_mark_usdc.reset();
        } else {
            cooldown_fork_trace.accounting_residual_usdc = 0.0;
            cooldown_fork_trace.assignment_to_washout_value_usdc.reset();
            cooldown_fork_trace.censor_time_mid_mark_usdc =
                assignment_delta_mid;
            cooldown_fork_trace.censor_time_executable_mark_usdc =
                executable_equity -
                cooldown_fork_trace.assignment_equity_usdc;
        }
        cooldown_fork_trace.censor_marks_are_terminal_bounds = false;
        cooldown_fork_trace.post_assignment_buy_fill_count =
            summary.fills_bid -
            cooldown_duration_fork_assignment_buy_fills;
        cooldown_fork_trace.post_assignment_sell_fill_count =
            summary.fills_ask -
            cooldown_duration_fork_assignment_sell_fills;
        cooldown_fork_trace.inventory_time_btc_s =
            cooldown_duration_fork_inventory_time_btc_s;
        cooldown_fork_trace.mae_usdc = cooldown_duration_fork_mae_usdc;
        cooldown_fork_trace.max_abs_inventory_btc =
            cooldown_duration_fork_max_abs_inventory_btc;
        cooldown_fork_trace.active_or_pending_order_count =
            static_cast<std::int64_t>(bid_orders.size() + ask_orders.size());
        const auto [pending_submit_count, pending_cancel_count] =
            pending_order_counts(bid_orders, ask_orders);
        cooldown_fork_trace.pending_submit_count = pending_submit_count;
        cooldown_fork_trace.pending_cancel_count = pending_cancel_count;
        cooldown_fork_trace.pending_ack_count = pending_cancel_count;
        cooldown_fork_trace.campaign_active =
            cooldown_duration_active_campaign_id != 0 ||
            std::abs(inventory) > 1e-10;
        // The compact C++ replay has no q90 cursor/hazard runtime.  Keep these
        // fields explicit so the wrapper cannot silently invent zeroes.
        cooldown_fork_trace.cursor_owner_count = 0;
        cooldown_fork_trace.hazard_owner_count = 0;
        if (cooldown_fork_trace.arm_washout_complete &&
            (cooldown_fork_trace.active_or_pending_order_count != 0 ||
             cooldown_fork_trace.pending_submit_count != 0 ||
             cooldown_fork_trace.pending_cancel_count != 0 ||
             cooldown_fork_trace.pending_ack_count != 0 ||
             cooldown_fork_trace.campaign_active ||
             cooldown_fork_trace.cursor_owner_count != 0 ||
             cooldown_fork_trace.hazard_owner_count != 0)) {
            throw std::runtime_error(
                "completed cooldown duration fork retained descendant state"
            );
        }
    }
    const double mtm_before_terminal_fee = cash + inventory * final_price;
    // End-of-window inventory is marked, not synthetically liquidated. Real
    // timeout/emergency taker exits above still pay params.taker_fee.
    const double terminal_fee_drag = 0.0;
    const double terminal_liquidation_fee_estimate =
        std::abs(inventory) * final_price * std::max(0.0, params.taker_fee);
    const double final_pnl = mtm_before_terminal_fee;
    summary.cash = cash;
    summary.terminal_mark_price = final_price;
    summary.mtm_before_terminal_fee = mtm_before_terminal_fee;
    summary.terminal_fee_drag = terminal_fee_drag;
    summary.terminal_liquidation_fee_estimate = terminal_liquidation_fee_estimate;
    summary.max_drawdown = max_drawdown;
    if (normalized_delta_count > 0 && pnl_dt_sum > 0.0) {
        const double count = static_cast<double>(normalized_delta_count);
        const double normalized_mean = normalized_delta_sum / count;
        const double normalized_variance = std::max(
            0.0, normalized_delta_sq_sum / count - normalized_mean * normalized_mean);
        const double sigma_sec = std::sqrt(normalized_variance);
        const double mu_sec = pnl_delta_sum / pnl_dt_sum;
        summary.sharpe = sigma_sec > 0.0
            ? mu_sec / sigma_sec * std::sqrt(365.25 * 86'400.0)
            : 0.0;
    }
    summary.final_inventory = inventory;
    summary.ber_held_input_end = ber_held_ti;
    summary.ber_ema_fast_end = ema_ti_fast;
    summary.ber_ema_slow_end = ema_ti_slow;
    summary.ber_active_end = ber_active;
    summary.p3_reach_budget_active_end_count =
        static_cast<std::int64_t>(p3_reach_budget_buy_episode.active) +
        static_cast<std::int64_t>(p3_reach_budget_sell_episode.active);
    summary.p3_reach_budget_buy_selected_k_end =
        p3_reach_budget_buy_episode.active
        ? p3_reach_budget_buy_episode.selected_k
        : 0;
    summary.p3_reach_budget_sell_selected_k_end =
        p3_reach_budget_sell_episode.active
        ? p3_reach_budget_sell_episode.selected_k
        : 0;
    if (summary.planned_quote_stop_triggered) {
        for (const auto& order : bid_orders) {
            if (order.state == OrderState::Open) {
                ++summary.planned_shutdown_open_order_count;
            } else if (order.state == OrderState::PendingNew) {
                ++summary.planned_shutdown_pending_new_order_count;
            } else if (order.state == OrderState::PendingCancel) {
                ++summary.planned_shutdown_pending_cancel_order_count;
            }
        }
        for (const auto& order : ask_orders) {
            if (order.state == OrderState::Open) {
                ++summary.planned_shutdown_open_order_count;
            } else if (order.state == OrderState::PendingNew) {
                ++summary.planned_shutdown_pending_new_order_count;
            } else if (order.state == OrderState::PendingCancel) {
                ++summary.planned_shutdown_pending_cancel_order_count;
            }
        }
    }
    summary.pnl = final_pnl;
    summary.fills_total = summary.fills_bid + summary.fills_ask;
    summary.circuit_breaker_closing = circuit_breaker_closing;
    summary.risk_emergency_latched = risk_emergency_latched;
    summary.risk_utc_day = risk_utc_day;
    summary.risk_day_start_total_pnl = risk_day_start;
    summary.risk_session_peak_pnl = risk_peak;
    summary.risk_last_total_pnl = risk_last;
    summary.risk_total_pnl_offset = risk_offset;
    summary.max_abs_inventory = max_abs_inventory;
    summary.avg_markout = mo_qty_all > 0.0 ? mo_sum_all / mo_qty_all : 0.0;
    summary.avg_markout_bid = mo_qty_bid > 0.0 ? mo_sum_bid / mo_qty_bid : 0.0;
    summary.avg_markout_ask = mo_qty_ask > 0.0 ? mo_sum_ask / mo_qty_ask : 0.0;
    summary.avg_markout_final_compressed = mo_qty_final_compressed > 0.0
        ? mo_sum_final_compressed / mo_qty_final_compressed
        : 0.0;
    summary.avg_markout_not_final_compressed = mo_qty_not_final_compressed > 0.0
        ? mo_sum_not_final_compressed / mo_qty_not_final_compressed
        : 0.0;
    summary.fills_bid_final_compressed = fills_bid_final_compressed;
    summary.fills_ask_final_compressed = fills_ask_final_compressed;
    summary.fills_bid_not_final_compressed = summary.fills_bid - fills_bid_final_compressed;
    summary.fills_ask_not_final_compressed = summary.fills_ask - fills_ask_final_compressed;
    summary.markout_count = mo_count_all;
    summary.markout_qty_btc = mo_qty_all;
    summary.markout_qty_bid_btc = mo_qty_bid;
    summary.markout_qty_ask_btc = mo_qty_ask;
    summary.markout_qty_final_compressed_btc = mo_qty_final_compressed;
    summary.markout_qty_not_final_compressed_btc = mo_qty_not_final_compressed;
    summary.avg_rq_ms = summary.n_requotes > 0
        ? rq_sum / static_cast<double>(summary.n_requotes)
        : 0.0;
    summary.avg_spread = summary.n_requotes > 0
        ? spread_sum / static_cast<double>(summary.n_requotes)
        : 0.0;
    summary.n_final_spread = final_spread_count;
    summary.avg_final_spread = final_spread_count > 0
        ? final_spread_sum / static_cast<double>(final_spread_count)
        : 0.0;
    summary.consecutive_loss_cooldown_trigger_count =
        loss_cooldown.trigger_count;
    summary.consecutive_loss_cooldown_expiry_count =
        loss_cooldown.expiry_count;
    summary.consecutive_loss_round_trip_loss_count =
        loss_cooldown.losing_round_trips;
    summary.consecutive_loss_round_trip_nonloss_count =
        loss_cooldown.winning_or_flat_round_trips;
    summary.consecutive_loss_count_end = loss_cooldown.consecutive_losses;
    summary.consecutive_loss_count_max =
        loss_cooldown.max_observed_consecutive_losses;
    summary.consecutive_loss_cooldown_until_ms =
        loss_cooldown.cooldown_until_ms;
    summary.consecutive_loss_last_cancel_ts_end =
        loss_cooldown.last_cancel_ts_ms;
    summary.consecutive_loss_snapshot_schema =
        std::string(kLossCooldownSnapshotSchema);
    summary.consecutive_loss_inventory_end = loss_cooldown.inventory;
    summary.consecutive_loss_avg_entry_end = loss_cooldown.avg_entry;
    summary.consecutive_loss_open_commission_end =
        loss_cooldown.open_commission;
    summary.consecutive_loss_round_trip_pnl_end =
        loss_cooldown.round_trip_pnl;
    summary.consecutive_loss_threshold_pending_end =
        loss_cooldown.threshold_pending;
    summary.sync_adjust_degrade_until_ms = sync_adjust_degrade_until_ms;
    if (f05_cooldown_runtime.has_value()) {
        if (f05_cooldown_predicate_cursor !=
            params.f05_cooldown_predicate_rows.size()) {
            throw std::runtime_error(
                "F05 repeated cooldown predicate tape retained unmatched rows"
            );
        }
        const auto end_ts_ns = end_ts * 1'000'000;
        const bool withhold_same_time_receive_window =
            f05_cooldown_runtime->config().feature_clock_semantics ==
            "receive_time_full_mid_ema_bank_v1";
        if (f05_cooldown_window_cursor <
                f05_cooldown_window_tape.size() &&
            (f05_cooldown_window_tape[f05_cooldown_window_cursor]
                     .feature_ready_ts_ns < end_ts_ns ||
             (!withhold_same_time_receive_window &&
              f05_cooldown_window_tape[f05_cooldown_window_cursor]
                      .feature_ready_ts_ns == end_ts_ns))) {
            throw std::runtime_error(
                "F05 repeated cooldown causal window tape was not fully consumed"
            );
        }
        f05_cooldown_runtime->advance_time(end_ts);
        result.f05_repeated_cooldown_checkpoint =
            f05_cooldown_runtime->checkpoint();
    }
    return result;
}

}  // namespace narrowgate_cpp
