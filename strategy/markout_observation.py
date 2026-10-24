"""Output descriptions only: no price lookup, policy clock or admission logic.

Python replay exposes configured/effective ages separately; the existing age
normalization belongs to its consumer, not this descriptor. Message-scheduled
replay and MakerEngine do not apply the sampled total-fill-age discard.
Offline trade-array labels must not be merged with online visible-mid feedback.

Replay event identity is (journal account identity, observation_id); partial
fills get separate IDs. Existing L2 chunk delivery audits prove persisted rows
only when a journal was supplied. Counts without a sink are not saved events.
Pause snapshots describe a latch at original evaluation opportunities, not
continuous-time exact exits, independent vetoes or counterfactual profitability.
Unknown source-price clocks remain null. No native-backend instrumentation or
live completeness guarantee is implied by this Python output contract.
"""


def markout_semantics(*, consumer, minimum_age_s, configured_max_age_s,
                      effective_max_age_s, max_age_applied, price_selector,
                      clock_policy, aggregate_weighting="quantity_weighted_resolved_fills"):
    """Describe values already selected by the consumer, never normalize them."""
    return {
        "purpose": "online_feedback",
        "consumer": consumer,
        "minimum_fill_age_s": minimum_age_s,
        "configured_max_fill_age_s": configured_max_age_s,
        "effective_max_fill_age_s": effective_max_age_s,
        "max_fill_age_rule_applied": max_age_applied,
        "max_fill_age_comparison": ">" if max_age_applied else None,
        "price_selector": price_selector,
        "clock_policy": clock_policy,
        "price_age_known": False,
        "exact_target_time_query": False,
        "value_unit": "quote_currency_per_base_unit",
        "ema_weighting": "one_update_per_resolved_fill_alpha_2_over_span_plus_1",
        "aggregate_weighting": aggregate_weighting,
        "pause_duration": "EMA_below_threshold_AND_clock_before_until; repeat_extensions_allowed",
    }


def pause_transition(previous, current):
    """Classify snapshots evaluated by the original consumer, without a clock read."""
    if previous is None:
        return "INITIAL_STATE"
    if current["active"] != previous["active"]:
        return "ENTER" if current["active"] else "EXIT"
    if current["active"] and current["until"] > previous["until"]:
        return "EXTEND"
    return None
