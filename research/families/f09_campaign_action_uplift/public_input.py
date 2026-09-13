"""Source-bound assignment panels; never infer propensity from market trades."""

import math

import pandas as pd

from data.feature_cursor import FeatureCursor
from models.replay.continuous_accounting import marked_equity_change


def build_action_panel(root, assignments, *, feature_columns, missing_policy,
                       max_age_ns, max_mark_age_ms, training_boundary_ns):
    """Use post-assignment equity changes, excluding earlier campaign profit.

    The caller supplies real logged assignment probability and explicit start /
    terminal account states from the same experiment. This does not establish
    support, randomization or online causal benefit; the maintained OPE engine
    performs its own support/estimator checks on the resulting panel.
    """
    cursor = FeatureCursor(root)
    if max_mark_age_ms is None:
        raise ValueError("explicit reward valuation age policy required")
    records = list(assignments)
    cursor.require_binding(records)
    if len({row["decision_id"] for row in records}) != len(records):
        raise ValueError("duplicate assignment identity")
    rows = []
    for assignment in records:
        decision, end = assignment["decision_ts_ns"], assignment["actual_outcome_end_ns"]
        if type(decision) is not int or type(end) is not int or end <= decision:
            raise ValueError("invalid assignment outcome boundary")
        if decision >= training_boundary_ns or end >= training_boundary_ns:
            continue
        probability = float(assignment["behavior_propensity"])
        if not math.isfinite(probability) or not 0 < probability <= 1:
            raise ValueError("logged behavior propensity required")
        start_state, end_state = assignment["start_state"], assignment["end_state"]
        cursor.require_binding([start_state, end_state])
        if start_state["boundary_ts_ms"] * 1_000_000 != decision or end_state["boundary_ts_ms"] * 1_000_000 != end:
            raise ValueError("reward states must match assignment and actual outcome endpoints")
        economics = marked_equity_change(start_state, end_state,
            fees_usdc=assignment["fees_usdc"], funding_cashflow_usdc=assignment["funding_cashflow_usdc"],
            max_mark_age_ms=max_mark_age_ms)
        if not economics["economic_complete"]:
            raise ValueError("incomplete assignment reward cannot enter OPE as zero")
        features = cursor.row(decision, columns=feature_columns, missing_policy=missing_policy,
                              max_age_ns=max_age_ns)
        row = {"decision_id": assignment["decision_id"], "decision_ts_ns": decision,
               "day": pd.Timestamp(decision, unit="ns", tz="UTC").date().isoformat(),
               "action": assignment["action"], "behavior_propensity": probability,
               "reward": economics["net_equity_change_usdc"],
               "actual_outcome_end_ns": end,
               "feature_ready_ts_ns": cursor.at(decision, max_age_ns=max_age_ns).cutoff_ns}
        row.update({key: value for key, value in assignment.items()
                    if key == "candidate_action" or key.startswith(("behavior_prob_", "candidate_prob_"))})
        if set(features) & set(row):
            raise ValueError("feature names collide with assignment fields")
        rows.append({**row, **features})
    result = pd.DataFrame(rows)
    result.attrs.update(input_manifest_id=cursor.input_manifest_id, reward_units="USDC",
                        reward_origin="assignment_to_terminal_equity_change", causal_admission=False)
    return result


def evaluate_action_panel(panel, *, feature_columns, feature_registry_path, config):
    """Dispatch to the maintained estimator with actual-end purge and logged probabilities."""
    from dataclasses import replace
    from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import evaluate_offline_policy

    if panel.attrs.get("reward_origin") != "assignment_to_terminal_equity_change":
        raise ValueError("source-bound assignment panel required")
    if config.split_mode != "chronological" or feature_registry_path is None:
        raise ValueError("explicit timed feature registry and chronological OPE required")
    actions = set(panel[config.action_col])
    for action in actions:
        if config.behavior_prob_prefix + action not in panel:
            raise ValueError("full logged action probability vector required; no estimated fallback")
    config = replace(config, actual_outcome_end_col="actual_outcome_end_ns")
    return evaluate_offline_policy(panel, feature_names=feature_columns,
                                   feature_registry_path=feature_registry_path, config=config)
