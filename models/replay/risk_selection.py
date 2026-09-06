"""Complete offline E/C opportunities, one intervention, or a frozen policy.

This collector owns no simulator state and authorizes no live actions. The
existing replay applies each selected action through its normal order path.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
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


class ReplayRiskSelection:
    """An uncapped collector, or an explicit streaming sink that must succeed."""

    def __init__(self, *, intervention: Mapping[str, Any] | None = None,
                 sink: Callable[[dict[str, Any]], None] | None = None,
                 max_rows: int = 0, mode: str = "B",
                 policy: RiskSelectionPolicy | Mapping[str, Any] | None = None) -> None:
        if not isinstance(mode, str) or mode not in {"B", "E", "C", "EC"}:
            raise ValueError("risk_selection_mode must be B, E, C, or EC")
        self.target = dict(intervention or {})
        if self.target and (
            set(self.target) != {"opportunity_id", "action"}
            or not str(self.target["opportunity_id"]).strip()
            or self.target["action"] not in {"WAIT", "CANCEL"}
        ):
            raise ValueError("risk_selection_intervention requires opportunity_id and WAIT/CANCEL")
        if self.target and mode != "B":
            raise ValueError("risk-selection policy cannot share a single intervention")
        if isinstance(policy, Mapping):
            policy = RiskSelectionPolicy.from_dict(policy)
        if policy is not None and not isinstance(policy, RiskSelectionPolicy):
            raise ValueError("risk_selection_policy requires a parsed policy or JSON object")
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
            decisions = {decision.opportunity_id: decision for decision in
                         evaluate_risk_selection(observation, candidates, self.policy)}
        actions = []
        for row in rows:
            decision = decisions.get(row["opportunity_id"])
            if self.mode != "B":
                row.update(
                    policy_mode=self.mode,
                    policy_id=self.policy.policy_id if self.policy else "",
                    value_delta_usdc=decision.value_delta_usdc if decision else None,
                    policy_reason=decision.reason if decision else "mode_disabled",
                )
            baseline_action = row["baseline_action"]
            action = self._observe(row, decision.action if decision else None)
            if decision is not None:
                self.policy_action_counts[action] += 1
                self.policy_change_count += int(action != baseline_action)
                if decision.out_of_scope:
                    self.policy_fallback_counts[decision.reason] = (
                        self.policy_fallback_counts.get(decision.reason, 0) + 1
                    )
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
            "risk_selection_policy_id": self.policy.policy_id if self.policy else "",
            "risk_selection_policy_decision_count": sum(self.policy_action_counts.values()),
            "risk_selection_policy_action_counts": dict(self.policy_action_counts),
            "risk_selection_policy_change_count": self.policy_change_count,
            "risk_selection_policy_fallback_counts": dict(self.policy_fallback_counts),
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
        if result.get("risk_selection_mode", "B") != "B":
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
