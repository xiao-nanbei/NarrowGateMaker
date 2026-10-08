"""Explicit new-input action rules; no legacy model or exchange-state access."""

from copy import deepcopy
from math import isfinite
from operator import ge, le

from data.feature_cursor import FeatureCursor


class PublicStrategy:
    """One fresh replay, market-feature predicates and bounded action receipts.

    This is a configurable rule policy, not a fitted CIF/hazard model. Inputs
    are ready-time features only; no actual queue, unnotified fill or future Bar
    is supplied. KEEP is a request subordinate to the executor's safety rules.
    """

    def __init__(self, root, contract):
        self.cursor = FeatureCursor(root)
        self.contract = deepcopy(contract)
        required = {"schema", "policy_id", "family", "input_manifest_id",
                    "feature_cols", "missing_policy", "max_age_ns", "rules",
                    "trace_limit"}
        if set(contract) != required or contract["schema"] != "research.action_policy.v1":
            raise ValueError("explicit action policy contract required")
        if not isinstance(contract["policy_id"], str) or not contract["policy_id"]:
            raise ValueError("nonempty policy identity required")
        self.cursor.require_binding([contract])
        if contract["family"] not in {"F06", "F07"}:
            raise ValueError("action family must be F06 or F07")
        cols = contract["feature_cols"]
        if not isinstance(cols, list) or not cols or len(set(cols)) != len(cols):
            raise ValueError("explicit unique feature columns required")
        if contract["missing_policy"] not in {"reject", "skip_decision"}:
            raise ValueError("explicit reject or skip_decision missing policy required")
        for key in ("max_age_ns", "trace_limit"):
            if type(contract[key]) is not int or contract[key] < 0:
                raise ValueError(f"nonnegative integer {key} required")
        if set(contract["rules"]) != {"BUY", "SELL"}:
            raise ValueError("both side rule lists required")
        for rules in contract["rules"].values():
            if not isinstance(rules, list):
                raise ValueError("ordered rule list required")
            for rule in rules:
                if set(rule) != {"feature", "op", "threshold", "action", "spread_mult"}:
                    raise ValueError("explicit predicate and action fields required")
                if rule["feature"] not in cols or rule["op"] not in {"ge", "le"}:
                    raise ValueError("predicate must use declared features and operators")
                for key in ("threshold", "spread_mult"):
                    if isinstance(rule[key], bool) or not isfinite(float(rule[key])):
                        raise ValueError("finite numeric rule values required")
                allowed = {"default", "widen"} if contract["family"] == "F06" else {"default", "keep", "cancel"}
                if rule["action"] not in allowed:
                    raise ValueError("action does not belong to the selected family")
                mult = float(rule["spread_mult"])
                if mult < 1 or (rule["action"] != "widen" and mult != 1):
                    raise ValueError("only widening may change the positive spread multiplier")
        self.started = False
        self.last_ns = None
        self.current = None
        self.receipts = []
        self.counts = {"decisions": 0, "missing_decisions": 0, "requested_sides": 0,
                       "resolved_sides": 0, "trace_dropped": 0}

    def start(self, manifest_id):
        if self.started or manifest_id != self.cursor.input_manifest_id:
            raise ValueError("strategy requires a fresh instance and matching input binding")
        self.started = True

    def decide(self, decision_ns):
        if not self.started:
            raise ValueError("strategy not bound to an execution")
        if self.last_ns is not None and decision_ns < self.last_ns:
            raise ValueError("strategy decision time regressed")
        if decision_ns == self.last_ns:
            return self.current
        # Stale/absent context is an error, not a missing-value fallback.
        frame = self.cursor.at(decision_ns, max_age_ns=self.contract["max_age_ns"])
        row = self.cursor.row(decision_ns, columns=self.contract["feature_cols"],
                              missing_policy="native_nan",
                              max_age_ns=self.contract["max_age_ns"])
        missing = any(not isfinite(float(value)) for value in row.values())
        if missing and self.contract["missing_policy"] == "reject":
            raise ValueError("missing strategy feature")
        actions = {side: {"action": "default", "spread_mult": 1.0} for side in ("BUY", "SELL")}
        if not missing:
            for side, rules in self.contract["rules"].items():
                for rule in rules:
                    if {"ge": ge, "le": le}[rule["op"]](row[rule["feature"]], float(rule["threshold"])):
                        actions[side] = {"action": rule["action"], "spread_mult": float(rule["spread_mult"])}
                        break
        self.counts["decisions"] += 1
        self.counts["missing_decisions"] += int(missing)
        self.counts["requested_sides"] += sum(a["action"] != "default" for a in actions.values())
        self.last_ns, self.current = decision_ns, actions
        self.current_cutoff = frame.cutoff_ns
        return actions

    @staticmethod
    def continuation(action, *, enabled, updated, has_order, force_update):
        if action == "cancel":
            return False, True
        if action == "keep" and enabled and has_order and not force_update:
            return enabled, False
        return enabled, updated

    def resolved(self, decision_ns, side, *, action, price, quantity, route_due):
        self.counts["resolved_sides"] += 1
        if len(self.receipts) >= self.contract["trace_limit"]:
            self.counts["trace_dropped"] += 1
            return
        self.receipts.append({"decision_ns": decision_ns, "feature_cutoff_ns": self.current_cutoff,
                              "side": side, "requested": dict(self.current[side]),
                              "resolved_intent": action, "price": price, "quantity": quantity,
                              "route_due": route_due})

    def report(self):
        return {"contract": deepcopy(self.contract), "counts": dict(self.counts),
                "decisions": deepcopy(self.receipts),
                "receipt_semantics": "resolved_intent_not_ack_or_fill",
                "native_queue_parity": "not_proven"}


def replay_configured_strategy(root, *, contract, params, initial_capital,
                               max_mark_age_ns, funding=None):
    """Run one continuous account; settle independently from feature timing."""
    from models.backtest_tick import simulate_public_inputs
    from models.replay.public_accounting import settle_public_replay

    if (params.get("initial_live_state") or params.get("initial_inventory", 0) != 0
            or params.get("replay_initial_state_mode", "fresh_start") != "fresh_start"):
        raise ValueError("new strategy requires an empty initial account")
    if params.get("trace_fills_max", 0) <= 0:
        raise ValueError("complete fill trace required for accounting")
    policy = PublicStrategy(root, contract)
    result = simulate_public_inputs(root, params, public_strategy=policy)
    account = settle_public_replay(root, result, initial_capital=initial_capital,
                                  max_mark_age_ns=max_mark_age_ns, funding=funding)
    result["accounting"] = account
    for key in ("economic_complete", "all_in_net_pnl", "funding_cashflow"):
        result[key] = account[key]
    return result
