"""Offline E/C opportunities, interventions, learned policies, and controls.

This collector owns no simulator state and authorizes no live actions. The
existing replay applies each selected action through its normal order path.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from strategy.risk_selection import (
    PendingExposure,
    RiskSelectionCandidate,
    RiskSelectionObservation,
    RiskSelectionPolicy,
    evaluate_risk_selection,
)


def opportunity_id(symbol: str, ts_ms: int, decision_sequence: int,
                   side: str, kind: str, order_id: str = "") -> str:
    return f"{symbol}:{ts_ms}:{decision_sequence}:{side}:{kind}:{order_id or '-'}"


def visible_feature_snapshot(values: Mapping[str, Any]) -> dict[str, float | None]:
    """Preserve unavailable model inputs as unknown, never fabricated zeros."""
    snapshot = {}
    for name, value in values.items():
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            number = math.nan
        snapshot[name] = number if math.isfinite(number) else None
    return snapshot


def feature_ready_time(source_ready_ns: Mapping[str, int], prediction_ready_ns: int,
                       decision_ts_ns: int) -> int:
    """Include the prediction fallback alongside individually scheduled sources."""
    return max(prediction_ready_ns, max(source_ready_ns.values(), default=decision_ts_ns))


def random_control_draw(seed: int, scope: str, identity: str) -> float:
    """Order-independent control noise; never advance execution/latency RNGs.

    This digest is a deterministic PRF, not an artifact identity or permission.
    The experiment freezes seed/scope before its evaluation outcomes are read.
    """
    key = json.dumps(["risk-selection-random-v1", seed, scope, identity],
                     ensure_ascii=True, separators=(",", ":")).encode()
    bits = int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big") >> 11
    return bits / 2**53


class ReplayRiskSelection:
    """An uncapped collector, or an explicit streaming sink that must succeed."""

    def __init__(self, *, intervention: Mapping[str, Any] | None = None,
                 sink: Callable[[dict[str, Any]], None] | None = None,
                 max_rows: int = 0, mode: str = "B",
                 policy: RiskSelectionPolicy | Mapping[str, Any] | None = None,
                 control: str = "learned", random_rates: Mapping[str, float] | None = None,
                 random_seed: int | None = None, random_scope: str = "") -> None:
        if not isinstance(mode, str) or mode not in {"B", "E", "C", "EC"}:
            raise ValueError("risk_selection_mode must be B, E, C, or EC")
        if not isinstance(control, str) or control not in {"learned", "random", "flat"}:
            raise ValueError("risk_selection_control must be learned, random, or flat")
        if control == "flat" and mode != "E":
            raise ValueError("Flat uses mode E and skips every new opener")
        if control == "random" and mode == "B":
            raise ValueError("random control requires an E/C selection mode")
        self.target = dict(intervention or {})
        if self.target and (
            set(self.target) != {"opportunity_id", "action"}
            or not str(self.target["opportunity_id"]).strip()
            or self.target["action"] not in {"WAIT", "CANCEL"}
        ):
            raise ValueError("risk_selection_intervention requires opportunity_id and WAIT/CANCEL")
        if self.target and (mode != "B" or control != "learned"):
            raise ValueError("risk-selection policy cannot share a single intervention")
        if control == "flat":
            # A shared experiment may load one policy for its other arms. Flat
            # neither parses nor evaluates it and cannot inherit its coverage.
            policy = None
        if isinstance(policy, Mapping):
            policy = RiskSelectionPolicy.from_dict(policy)
        if policy is not None and not isinstance(policy, RiskSelectionPolicy):
            raise ValueError("risk_selection_policy requires a parsed policy or JSON object")
        self.random_rates: dict[str, float] = {}
        if control == "random":
            if policy is None:
                raise ValueError("random control requires the reference policy's support")
            if (type(random_seed) is not int or not isinstance(random_scope, str)
                    or not random_scope.strip()):
                raise ValueError("random control requires an explicit integer seed and scope")
            if not isinstance(random_rates, Mapping) or not random_rates:
                raise ValueError("random control requires frozen per-surface veto rates")
            for surface, rate in random_rates.items():
                if surface not in {f"{k}:{s}" for k in "EC" for s in ("BUY", "SELL")}:
                    raise ValueError("unknown random-control surface")
                if isinstance(rate, bool):
                    raise ValueError("random-control rates must be numeric probabilities, not bool")
                rate = float(rate)
                if not math.isfinite(rate) or not 0 <= rate <= 1:
                    raise ValueError("random-control rates must be finite and within [0, 1]")
                self.random_rates[surface] = rate
        elif random_rates is not None or random_seed is not None or random_scope:
            raise ValueError("random-control parameters require control random")
        if sink is not None and not callable(sink):
            raise ValueError("risk-selection opportunity sink must be callable")
        if max_rows < 0:
            raise ValueError("risk-selection opportunity limit cannot be negative")
        self.sink = sink
        self.max_rows = max_rows
        self.rows: list[dict[str, Any]] = []
        self.counts = {"E": 0, "C": 0}
        self.intervention_count = 0
        self.mode = mode
        self.policy = policy
        self.control = control
        self.random_seed = random_seed
        self.random_scope = random_scope
        self.control_action_counts = {action: 0 for action in ("POST", "WAIT", "KEEP", "CANCEL")}
        self.control_fallback_counts: dict[str, int] = {}
        self.control_change_count = 0
        self.reference_evaluation_count = 0
        self.policy_action_counts = {action: 0 for action in ("POST", "WAIT", "KEEP", "CANCEL")}
        self.policy_fallback_counts: dict[str, int] = {}
        self.policy_change_count = 0

    def observe(self, row: dict[str, Any]) -> str:
        return self.observe_batch([row])[0]

    def observe_batch(self, rows: Sequence[dict[str, Any]]) -> tuple[str, ...]:
        """Score both sides before any reservation or execution-side mutation."""
        if not rows:
            return ()
        decisions = {}
        if self.mode != "B":
            first = rows[0]
            shared = ("decision_ts_ns", "feature_ready_ts_ns", "inventory_btc", "pending_orders")
            if any(any(row[field] != first[field] for field in shared) for row in rows):
                raise ValueError("risk-selection batch must share one visible observation")
            observation = RiskSelectionObservation(
                decision_ts_ns=int(first["decision_ts_ns"]),
                feature_ready_ts_ns=int(first["feature_ready_ts_ns"]),
                inventory_btc=float(first["inventory_btc"]),
                pending_orders=tuple(PendingExposure(**order) for order in first["pending_orders"]),
            )
            candidates = tuple(
                RiskSelectionCandidate(
                    opportunity_id=row["opportunity_id"], kind=row["kind"], side=row["side"],
                    quantity_btc=row["quantity_btc"], baseline_action=row["baseline_action"],
                    baseline_allowed=row["baseline_allowed"], order_id=row["order_id"],
                    features=row["features"],
                )
                for row in rows if row["kind"] in self.mode
            )
            if self.control == "flat" and (
                abs(observation.inventory_btc) > 1e-10 or observation.pending_orders
            ):
                raise ValueError("Flat requires a known flat account without pending ownership")
            decisions = {decision.opportunity_id: decision for decision in
                         evaluate_risk_selection(observation, candidates, self.policy)}
            if self.control == "random":
                self.reference_evaluation_count += len(decisions)
        actions = []
        for row in rows:
            decision = decisions.get(row["opportunity_id"])
            draw = rate = None
            if decision is not None and self.control == "flat":
                decision = replace(decision, action="WAIT", value_delta_usdc=None,
                                   reason="flat_no_new_risk", out_of_scope=False)
            elif decision is not None and self.control == "random":
                # Reuse the exact reference eligibility, including missing or
                # nonfinite predictions, but discard its value before drawing.
                # This computes the reference model; report that cost honestly.
                reason = decision.reason if decision.out_of_scope else "random_veto_rate_missing"
                action = row["baseline_action"]
                rate = self.random_rates.get(f"{row['kind']}:{row['side']}")
                eligible = not decision.out_of_scope and rate is not None
                if eligible:
                    draw = random_control_draw(self.random_seed, self.random_scope,
                                               row["opportunity_id"])
                    action = ("WAIT" if row["kind"] == "E" else "CANCEL") if draw < rate else action
                    reason = "random_veto" if draw < rate else "random_baseline"
                decision = replace(decision, action=action, value_delta_usdc=None,
                                   reason=reason, out_of_scope=not eligible)
            if self.mode != "B":
                row.update(
                    policy_mode=self.mode,
                    policy_id=(self.policy.policy_id
                               if self.policy and self.control == "learned" else ""),
                    value_delta_usdc=decision.value_delta_usdc if decision else None,
                    policy_reason=decision.reason if decision else "mode_disabled",
                )
                if self.control != "learned":
                    row.update(control=self.control, control_reason=row["policy_reason"],
                               reference_policy_id=self.policy.policy_id if self.policy else "",
                               random_veto_rate=rate, random_draw=draw)
            baseline_action = row["baseline_action"]
            action = self._observe(row, decision.action if decision else None)
            if decision is not None:
                counts = (self.policy_action_counts if self.control == "learned"
                          else self.control_action_counts)
                fallbacks = (self.policy_fallback_counts if self.control == "learned"
                             else self.control_fallback_counts)
                counts[action] += 1
                if self.control == "learned":
                    self.policy_change_count += int(action != baseline_action)
                else:
                    self.control_change_count += int(action != baseline_action)
                if decision.out_of_scope:
                    fallbacks[decision.reason] = fallbacks.get(decision.reason, 0) + 1
            actions.append(action)
        return tuple(actions)

    def _observe(self, row: dict[str, Any], policy_action: str | None) -> str:
        if self.max_rows and sum(self.counts.values()) >= self.max_rows:
            raise RuntimeError("complete risk-selection opportunity collector exceeded max_rows")
        action = str(row["baseline_action"]) if policy_action is None else policy_action
        if self.target.get("opportunity_id") == row["opportunity_id"]:
            expected = "WAIT" if row["kind"] == "E" else "CANCEL"
            if self.target["action"] != expected:
                raise ValueError(
                    "risk-selection intervention action does not match opportunity kind"
                )
            if self.intervention_count:
                raise RuntimeError("risk-selection target opportunity occurred more than once")
            action = expected
            self.intervention_count += 1
        row["action"] = action
        self.counts[row["kind"]] += 1
        if self.sink is None:
            self.rows.append(row)
        else:
            self.sink(row)
        return action

    def finish(self) -> dict[str, Any]:
        if self.target and self.intervention_count != 1:
            raise RuntimeError("risk-selection target opportunity was not reached")
        return {
            "_risk_selection_opportunities": self.rows,
            "risk_selection_opportunity_counts": dict(self.counts),
            "risk_selection_intervention_count": self.intervention_count,
            "risk_selection_opportunities_streamed": self.sink is not None,
            "risk_selection_mode": self.mode,
            "risk_selection_policy_id": (
                self.policy.policy_id if self.policy and self.control == "learned" else ""
            ),
            "risk_selection_policy_decision_count": sum(self.policy_action_counts.values()),
            "risk_selection_policy_action_counts": dict(self.policy_action_counts),
            "risk_selection_policy_change_count": self.policy_change_count,
            "risk_selection_policy_fallback_counts": dict(self.policy_fallback_counts),
            **({
                "risk_selection_control": self.control,
                "risk_selection_control_action_counts": dict(self.control_action_counts),
                "risk_selection_control_decision_count": sum(self.control_action_counts.values()),
                "risk_selection_control_change_count": self.control_change_count,
                "risk_selection_control_fallback_counts": dict(self.control_fallback_counts),
                "risk_selection_reference_evaluation_count": self.reference_evaluation_count,
                "risk_selection_reference_policy_id": self.policy.policy_id if self.policy else "",
                "risk_selection_random_rates": dict(self.random_rates),
                "risk_selection_random_seed": self.random_seed,
                "risk_selection_random_scope": self.random_scope,
            } if self.control != "learned" else {}),
        }


def assemble_paired_label(
    baseline: Mapping[str, Any], alternative: Mapping[str, Any], *,
    intervention: Mapping[str, Any], start_ts_ms: int, end_ts_ms: int,
    baseline_funding_usdc: float, alternative_funding_usdc: float,
) -> dict[str, Any]:
    """One POST-WAIT / KEEP-CANCEL label at a common terminal market mark.

    The caller must rerun the same inputs, parameters and initial state, changing
    only this intervention. Opportunity-prefix equality is a useful replay
    comparability check, not a complete physical checkpoint or live fill proof.
    Values include signed fees through replay cash and funding exactly once.
    Overlapping labels are training targets, never additive portfolio returns.
    """
    target = ReplayRiskSelection(intervention=intervention).target
    if not target or end_ts_ms <= start_ts_ms:
        raise ValueError("paired label needs one target and an explicit nonempty window")
    prefixes = []
    values = []
    for result, expected_count, funding in (
        (baseline, 0, baseline_funding_usdc), (alternative, 1, alternative_funding_usdc),
    ):
        if (result.get("risk_selection_mode", "B") != "B"
                or result.get("risk_selection_control", "learned") != "learned"):
            raise ValueError("single-intervention labels cannot contain a full-path learned policy")
        if (result["risk_selection_start_ts_ms"] != start_ts_ms
                or result["risk_selection_end_ts_ms"] != end_ts_ms):
            raise ValueError("paired replay did not execute the complete common window")
        rows = result["_risk_selection_opportunities"]
        if result.get("risk_selection_opportunities_streamed"):
            raise ValueError("paired label needs the complete opportunity rows, not a sink summary")
        if result["risk_selection_intervention_count"] != expected_count:
            raise ValueError("paired label requires baseline and exactly one intervention")
        counts = {kind: sum(row["kind"] == kind for row in rows) for kind in ("E", "C")}
        if (counts != result["risk_selection_opportunity_counts"]
                or sum(counts.values()) != len(rows)):
            raise ValueError("paired opportunity rows are incomplete")
        identities = [row["opportunity_id"] for row in rows]
        if len(set(identities)) != len(rows) or identities.count(target["opportunity_id"]) != 1:
            raise ValueError(
                "paired target must occur exactly once in each complete opportunity tape"
            )
        index = identities.index(target["opportunity_id"])
        for row_index, row in enumerate(rows):
            decision_ns = int(row["decision_ts_ns"])
            if not start_ts_ms * 1_000_000 <= decision_ns <= end_ts_ms * 1_000_000:
                raise ValueError("paired opportunity lies outside the common replay window")
            if int(row["feature_ready_ts_ns"]) > decision_ns:
                raise ValueError("paired opportunity contains future-visible features")
            if row_index and decision_ns < int(rows[row_index - 1]["decision_ts_ns"]):
                raise ValueError("paired opportunity tape is not chronological")
            expected_action = (
                target["action"] if expected_count and row_index == index
                else row["baseline_action"]
            )
            if row["action"] != expected_action:
                raise ValueError("paired replay changed an action outside the single target")
        expected_actions = ("POST", "WAIT") if rows[index]["kind"] == "E" else ("KEEP", "CANCEL")
        if (rows[index]["baseline_action"], target["action"]) != expected_actions:
            raise ValueError("paired target action does not match E/C opportunity kind")
        prefixes.append([{key: value for key, value in row.items() if key != "action"}
                         for row in rows[:index + 1]])
        if result.get("economic_pnl_complete") is False or result.get(
            "private_fill_pending_visibility_count", 0,
        ):
            raise ValueError("paired replay has an incomplete economic ledger")
        if result["terminal_liquidation_applied"]:
            raise ValueError("paired label requires the shared mark-to-market terminal rule")
        price, inventory, cash, pnl = (float(result[name]) for name in (
            "terminal_mark_price", "final_inventory", "cash_before_terminal", "pnl",
        ))
        if not all(math.isfinite(number) for number in (price, inventory, cash, pnl, funding)):
            raise ValueError("paired value contains a nonfinite amount")
        if price <= 0 or not math.isclose(
            cash + inventory * price, pnl, abs_tol=1e-8, rel_tol=1e-10,
        ):
            raise ValueError("paired replay cash, inventory and terminal PnL do not reconcile")
        values.append(pnl + funding)
    if prefixes[0] != prefixes[1]:
        raise ValueError(
            "paired replay opportunity prefix or target state differs before intervention"
        )
    if baseline["terminal_mark_price"] != alternative["terminal_mark_price"]:
        raise ValueError("paired replay terminal market mark differs")
    row = prefixes[0][-1]
    return {
        "opportunity_id": target["opportunity_id"], "kind": row["kind"],
        "side": row["side"], "role": row["role"], "order_id": row["order_id"],
        "decision_ts_ns": row["decision_ts_ns"],
        "feature_ready_ts_ns": row["feature_ready_ts_ns"], "features": row["features"],
        "baseline_action": row["baseline_action"], "alternative_action": target["action"],
        "quantity_btc": row["quantity_btc"], "price": row["price"],
        "replay_start_ts_ms": start_ts_ms, "terminal_mark_ts_ms": end_ts_ms,
        "terminal_mark_price": baseline["terminal_mark_price"],
        "baseline_value_usdc": values[0], "alternative_value_usdc": values[1],
        "value_difference_usdc": values[0] - values[1],
        "baseline_funding_usdc": baseline_funding_usdc,
        "alternative_funding_usdc": alternative_funding_usdc,
        "matched_opportunity_prefix_count": len(prefixes[0]),
        "comparability_scope": "same_inputs_rerun_and_opportunity_prefix_not_full_checkpoint",
        "value_scope": "modeled_single_intervention_common_terminal_mtm_including_fees_funding",
        "additive_portfolio_return": False,
    }
