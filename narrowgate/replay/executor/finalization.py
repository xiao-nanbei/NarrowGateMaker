"""Replay terminal settlement, diagnostics and complete result projection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class ReplayFinalization:
    def finalize(self):
        if self.context.phase_callback is not None:
            self.context.phase_callback('finalization')
        for loop_item in self.state.pending_external_market_release_rows:
            self.state.trace_row, self.state._ = loop_item
            self.state.trace_row["external_epoch_support_valid"] = 0
            self.state.trace_row["external_epoch_support_reason"] = (
                "missing_next_ready_market_generation"
            )

        self.state.replay_end_ts = int(self.state.trade_ts[self.state.last_processed_event_idx])
        self.state.last_px = float(self.state.trade_price[self.state.last_processed_event_idx])
        self.state.replay_end_mark_px = float(
            self._book_snapshot_at(self.state.replay_end_ts, self.state.last_px)[4]
        )
        self.state.ema_add_wait_fork_trace: dict[str, self.services.Any] = {}
        if self.state.ema_add_wait_fork_enabled:
            if not self.state.ema_add_wait_fork_assigned:
                raise RuntimeError("EMA ADD/WAIT fork target was never reached")
            self.state.end_bid, self.state.end_ask, self.state._, self.state._, self.state.end_mid, self.state.end_book_source = self._book_snapshot_at(
                self.state.replay_end_ts,
                self.state.last_px,
            )
            self.state.mid_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_mid)
            if float(self.state.q) > 0.0 and float(self.state.end_bid) > 0.0:
                self.state.executable_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_bid)
            elif float(self.state.q) < 0.0 and float(self.state.end_ask) > float(self.state.end_bid) > 0.0:
                self.state.executable_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_ask)
            else:
                self.state.executable_equity = float(self.state.mid_equity)
            self.state.value_mid = float(
                self.state.mid_equity - self.state.ema_add_wait_fork_assignment_equity_usdc
            )
            self.state.value_executable = float(
                self.state.executable_equity - self.state.ema_add_wait_fork_assignment_equity_usdc
            )
            self.state.right_censored = not bool(self.state.ema_add_wait_fork_terminal)
            if self.state.right_censored:
                self.state.ema_add_wait_fork_reason = "data_boundary_right_censored"
            self.state.remaining_orders = [*self.state.bid_orders, *self.state.ask_orders]
            self.state.pending_submit_count = sum(
                int(order.get("state") == self.state.ORDER_PENDING_NEW)
                for order in self.state.remaining_orders
            )
            self.state.pending_cancel_count = sum(
                int(order.get("state") == self.state.ORDER_PENDING_CANCEL)
                for order in self.state.remaining_orders
            )
            self.state.descendant_unterminal_count = sum(
                int(bool(order.get("ema_add_wait_fork_id", "")))
                for order in self.state.remaining_orders
            )
            self.state.ema_add_wait_fork_trace = {
                "schema_version": "multiscale_ema_add_wait_fork_trace.v1",
                "action": str(self.state.ema_add_wait_fork_action),
                "side": str(self.state.ema_add_wait_fork_target_side),
                "inventory_lifecycle_id": int(self.state.ema_add_wait_fork_target_inventory_lifecycle_id),
                "assignment_ts_ms": int(self.state.ema_add_wait_fork_assignment_ts_ms),
                "assignment_inventory_btc": float(
                    self.state.ema_add_wait_fork_assignment_inventory_btc
                ),
                "assignment_equity_usdc": float(
                    self.state.ema_add_wait_fork_assignment_equity_usdc
                ),
                "frozen_baseline_action": str(
                    self.state.ema_add_wait_fork_target_baseline_action
                ),
                "target_generation": {
                    str(key): int(value)
                    for key, value in self.state.ema_add_wait_fork_target_generation.items()
                },
                "target_market_event_index": int(
                    self.state.ema_add_wait_fork_target_market_event_index
                ),
                "frozen_release_ts_ms": int(
                    self.state.ema_add_wait_fork_frozen_release_ts_ms
                ),
                "frozen_release_market_event_index": int(
                    self.state.ema_add_wait_fork_frozen_release_event_index
                ),
                "frozen_release_generation": {
                    str(key): int(value)
                    for key, value in self.state.ema_add_wait_fork_frozen_release_generation.items()
                },
                "release_clock": str(self.state.ema_add_wait_fork_release_clock),
                "target_actual_action": str(
                    self.state.ema_add_wait_fork_target_actual_action
                ),
                "target_order_id": int(self.state.ema_add_wait_fork_target_order_id),
                "external_epoch_observed": bool(
                    self.state.ema_add_wait_fork_external_epoch_observed
                ),
                "wait_released": bool(self.state.ema_add_wait_fork_released),
                "wait_release_ts_ms": int(self.state.ema_add_wait_fork_release_ts_ms),
                "wait_release_generation": (
                    dict(self.state.ema_add_wait_fork_release_generation)
                    if self.state.ema_add_wait_fork_release_generation is not None
                    else None
                ),
                "quarantine_entered": bool(self.state.ema_add_wait_fork_quarantine),
                "quarantine_ts_ms": int(self.state.ema_add_wait_fork_quarantine_ts_ms),
                "arm_washout_complete": bool(self.state.ema_add_wait_fork_terminal),
                "terminal_ts_ms": int(
                    self.state.ema_add_wait_fork_terminal_ts_ms
                    if self.state.ema_add_wait_fork_terminal
                    else self.state.replay_end_ts
                ),
                "terminal_reason": str(self.state.ema_add_wait_fork_reason),
                "right_censored": bool(self.state.right_censored),
                "terminal_inventory_btc": float(self.state.q),
                "terminal_mid_usdc_per_btc": float(self.state.end_mid),
                "terminal_book_source": str(self.state.end_book_source),
                "boundary_mid_value_usdc": float(self.state.value_mid),
                "boundary_executable_value_usdc": float(self.state.value_executable),
                "decision_to_terminal_value_usdc": (
                    float(self.state.value_mid) if not self.state.right_censored else None
                ),
                "censor_time_marking_lower_usdc": (
                    float(min(self.state.value_mid, self.state.value_executable))
                    if self.state.right_censored
                    else None
                ),
                "censor_time_marking_upper_usdc": (
                    float(max(self.state.value_mid, self.state.value_executable))
                    if self.state.right_censored
                    else None
                ),
                "censor_time_marking_semantics": (
                    "contemporaneous_mid_and_executable_marks_not_terminal_bounds"
                    if self.state.right_censored
                    else "not_applicable"
                ),
                "descendant_submit_count": int(
                    self.state.ema_add_wait_fork_descendant_submit_count
                ),
                "second_assignment_count": int(
                    self.state.ema_add_wait_fork_second_assignment_count
                ),
                "active_or_pending_order_count": int(
                    len(self.state.remaining_orders)
                ),
                "pending_submit_count": int(self.state.pending_submit_count),
                "pending_cancel_count": int(self.state.pending_cancel_count),
                "pending_ack_count": int(self.state.pending_cancel_count),
                "descendant_unterminal_count": int(
                    self.state.descendant_unterminal_count
                ),
                "inventory_lifecycle_active": bool(self.state.inventory_lifecycle_active),
                "cursor_owner_count": int(len(self.state.dynamic_fill_hazard_paths)),
                "hazard_owner_count": int(len(self.state.dynamic_fill_hazard_paths))
                + int(self.state.dynamic_fill_hazard_hold is not None),
                "hazard_path_count": int(len(self.state.dynamic_fill_hazard_paths)),
                "hazard_hold_active": bool(self.state.dynamic_fill_hazard_hold is not None),
            }

        self.state.cooldown_duration_fork_trace: dict[str, self.services.Any] = {}
        if self.state.cooldown_duration_fork_enabled:
            if not self.state.cooldown_duration_fork_assigned:
                raise RuntimeError("cooldown duration fork target was never reached")
            self._cooldown_duration_fork_path_update(self.state.replay_end_ts, self.state.replay_end_mark_px)
            self.state.end_bid, self.state.end_ask, self.state._, self.state._, self.state.end_mid, self.state.end_book_source = self._book_snapshot_at(
                self.state.replay_end_ts,
                self.state.last_px,
            )
            self.state.mid_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_mid)
            if float(self.state.q) > 0.0 and float(self.state.end_bid) > 0.0:
                self.state.executable_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_bid)
            elif float(self.state.q) < 0.0 and float(self.state.end_ask) > float(self.state.end_bid) > 0.0:
                self.state.executable_equity = float(self.state.cash) + float(self.state.q) * float(self.state.end_ask)
            else:
                self.state.executable_equity = self.state.mid_equity
            self.state.value_mid = (
                self.state.mid_equity - self.state.cooldown_duration_fork_assignment_equity_usdc
            )
            self.state.value_executable = (
                self.state.executable_equity - self.state.cooldown_duration_fork_assignment_equity_usdc
            )
            self.state.right_censored = not bool(self.state.cooldown_duration_fork_terminal)
            if self.state.right_censored:
                self.state.cooldown_duration_fork_reason = "data_boundary_right_censored"
            self.state.remaining_orders = [*self.state.bid_orders, *self.state.ask_orders]
            self.state.pending_submit_count = sum(
                int(order.get("state") == self.state.ORDER_PENDING_NEW)
                for order in self.state.remaining_orders
            )
            self.state.pending_cancel_count = sum(
                int(order.get("state") == self.state.ORDER_PENDING_CANCEL)
                for order in self.state.remaining_orders
            )
            self.state.assignment_state = {
                "assignment_ts_ms": self.state.cooldown_duration_fork_assignment_ts_ms,
                "assignment_inventory_btc": (
                    self.state.cooldown_duration_fork_assignment_inventory_btc
                ),
                "assignment_equity_usdc": (
                    self.state.cooldown_duration_fork_assignment_equity_usdc
                ),
                "baseline_duration_ms": self.state.cooldown_duration_fork_baseline_ms,
                "target_ordinal": self.state.cooldown_duration_fork_target_ordinal,
                "target_order_id": self.state.cooldown_duration_fork_target_order_id,
                "target_inventory_lifecycle_id": self.state.cooldown_duration_fork_target_inventory_lifecycle_id,
                "target_side": self.state.cooldown_duration_fork_target_side,
            }
            if self.state.cooldown_duration_fork_baseline_policy_enabled:
                self.state.assignment_state.update(
                    {
                        "exact_owner_baseline_duration_ms": (
                            self.state.cooldown_duration_fork_exact_owner_baseline_ms
                        ),
                        "exact_owner_action": self.state.cooldown_duration_fork_exact_owner_action,
                        "exact_owner_baseline_policy_enabled": True,
                        "exact_owner_policy_sha256": (
                            self.state.cooldown_duration_fork_expected_owner_policy_sha256
                        ),
                    }
                )
            self.state.assignment_state_sha256 = self.services.hashlib.sha256(
                self.services.json.dumps(
                    self.state.assignment_state,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("ascii")
            ).hexdigest()
            self.state.cooldown_duration_fork_trace = {
                "schema_version": (
                    "multiscale_ema_boolean_cooldown_duration_fork_trace.v3"
                    if self.state.cooldown_duration_fork_baseline_policy_enabled
                    else "multiscale_ema_boolean_cooldown_duration_fork_trace.v2"
                ),
                "action": self.state.cooldown_duration_fork_action,
                "side": self.state.cooldown_duration_fork_target_side,
                "inventory_lifecycle_id": self.state.cooldown_duration_fork_target_inventory_lifecycle_id,
                "target_exposure_fill_ordinal": (
                    self.state.cooldown_duration_fork_target_ordinal
                ),
                "target_order_id": self.state.cooldown_duration_fork_target_order_id,
                "assignment_ts_ms": self.state.cooldown_duration_fork_assignment_ts_ms,
                "assignment_inventory_btc": (
                    self.state.cooldown_duration_fork_assignment_inventory_btc
                ),
                "assignment_equity_usdc": (
                    self.state.cooldown_duration_fork_assignment_equity_usdc
                ),
                "assignment_state_sha256": self.state.assignment_state_sha256,
                "baseline_duration_ms": self.state.cooldown_duration_fork_baseline_ms,
                "applied_duration_ms": self.state.cooldown_duration_fork_applied_ms,
                "applied_deadline_ts_ms": self.state.cooldown_duration_fork_deadline_ts_ms,
                "quarantine_entered": bool(self.state.cooldown_duration_fork_quarantine),
                "quarantine_ts_ms": self.state.cooldown_duration_fork_quarantine_ts_ms,
                "washout_protocol": (
                    "first_flat_exposure_quarantine_scheduler_drained_v2"
                ),
                "control_path_exact_until_quarantine": True,
                "exposure_permission_change_count": int(
                    self.state.cooldown_duration_exposure_permission_change_count
                ),
                "reducing_permission_control_checks": int(
                    self.state.cooldown_duration_reducing_permission_control_checks
                ),
                "reducing_quote_change_count": int(
                    self.state.cooldown_duration_reducing_quote_change_count
                ),
                "second_assignment_count": int(
                    self.state.cooldown_duration_second_assignment_count
                ),
                "arm_washout_complete": bool(self.state.cooldown_duration_fork_terminal),
                "terminal_ts_ms": (
                    self.state.cooldown_duration_fork_terminal_ts_ms
                    if self.state.cooldown_duration_fork_terminal
                    else self.state.replay_end_ts
                ),
                "terminal_reason": self.state.cooldown_duration_fork_reason,
                "right_censored": self.state.right_censored,
                "terminal_inventory_btc": float(self.state.q),
                "terminal_mid_usdc_per_btc": float(self.state.end_mid),
                "terminal_book_source": str(self.state.end_book_source),
                "boundary_mid_value_usdc": float(self.state.value_mid),
                "boundary_executable_value_usdc": float(self.state.value_executable),
                "assignment_to_washout_value_usdc": (
                    float(self.state.value_mid) if not self.state.right_censored else None
                ),
                "accounting_residual_usdc": (
                    float(
                        (float(self.state.cash) - self.state.cooldown_duration_fork_assignment_equity_usdc)
                        - self.state.value_mid
                    )
                    if not self.state.right_censored
                    else 0.0
                ),
                "censor_time_mid_mark_usdc": (
                    float(self.state.value_mid) if self.state.right_censored else None
                ),
                "censor_time_executable_mark_usdc": (
                    float(self.state.value_executable) if self.state.right_censored else None
                ),
                "censor_marks_are_terminal_bounds": False,
                "post_assignment_buy_fill_count": int(
                    self.state.nfb - self.state.cooldown_duration_fork_assignment_buy_fills
                ),
                "post_assignment_sell_fill_count": int(
                    self.state.nfa - self.state.cooldown_duration_fork_assignment_sell_fills
                ),
                "inventory_time_btc_s": float(
                    self.state.cooldown_duration_fork_inventory_time_btc_s
                ),
                "mae_usdc": float(self.state.cooldown_duration_fork_mae_usdc),
                "max_abs_inventory_btc": float(
                    self.state.cooldown_duration_fork_max_abs_inventory_btc
                ),
                "active_or_pending_order_count": len(self.state.remaining_orders),
                "pending_submit_count": self.state.pending_submit_count,
                "pending_cancel_count": self.state.pending_cancel_count,
                "pending_ack_count": self.state.pending_cancel_count,
                "inventory_lifecycle_active": bool(self.state.inventory_lifecycle_active),
                "cursor_owner_count": len(self.state.dynamic_fill_hazard_paths),
                "hazard_owner_count": len(self.state.dynamic_fill_hazard_paths)
                + int(self.state.dynamic_fill_hazard_hold is not None),
            }
            if self.state.cooldown_duration_fork_baseline_policy_enabled:
                self.state.cooldown_duration_fork_trace.update(
                    {
                        "exact_owner_baseline_duration_ms": (
                            self.state.cooldown_duration_fork_exact_owner_baseline_ms
                        ),
                        "exact_owner_action": self.state.cooldown_duration_fork_exact_owner_action,
                        "exact_owner_baseline_policy_enabled": True,
                        "exact_owner_policy_sha256": (
                            self.state.cooldown_duration_fork_expected_owner_policy_sha256
                        ),
                    }
                )

        if self.state.post_cooldown_inventory_budget_enabled:
            for loop_item in ("BUY", "SELL"):
                self.state.budget_side = loop_item
                self.state.pending = self.state.post_cooldown_inventory_budget_pending_release.get(
                    self.state.budget_side
                )
                self._post_cooldown_budget_finalize(
                    self.state.budget_side,
                    now_ts=self.state.replay_end_ts,
                    reason=(
                        "sync_adjust_unsupported_censor"
                        if self.state.sync_adjust_censored
                        else "day_end_censor"
                    ),
                    censored=True,
                )
                if self.state.pending is not None:
                    if self.state.trace_post_cooldown_inventory_budget is None:
                        raise RuntimeError(
                            "inventory-budget trace is unexpectedly disabled"
                        )
                    if (
                        len(self.state.trace_post_cooldown_inventory_budget)
                        >= self.state.trace_post_cooldown_inventory_budget_max
                    ):
                        raise RuntimeError(
                            "post-cooldown inventory-budget trace limit exceeded"
                        )
                    self.state.trace_post_cooldown_inventory_budget.append(
                        {
                            "schema_version": (
                                "post_cooldown_incremental_inventory_budget_trace.v1"
                            ),
                            "episode_id": -1,
                            "arm_id": self.state.post_cooldown_inventory_budget_arm_id,
                            "side": self.state.budget_side,
                            "budget_units": float(
                                self.state.post_cooldown_inventory_budget_units
                            ),
                            "scheduled_release_ts_ms": int(
                                self.state.pending["ready_ts_ms"]
                            ),
                            "release_observed_delay_ms": int(
                                max(
                                    0,
                                    self.state.replay_end_ts - int(self.state.pending["ready_ts_ms"]),
                                )
                            ),
                            "trigger_fill_ts_ms": int(
                                self.state.pending["last_fill_ts_ms"]
                            ),
                            "assignment_consecutive_same_side_fill_units": float(
                                self.state.pending["consecutive_same_side_fill_units"]
                            ),
                            "masked_decision_count_before_assignment": int(
                                self.state.pending["masked_decision_count"]
                            ),
                            "masked_reason_counts_before_assignment": dict(
                                self.state.pending["masked_reason_counts"]
                            ),
                            "terminal_ts_ms": self.state.replay_end_ts,
                            "terminal_reason": (
                                "release_never_baseline_eligible_day_end"
                            ),
                            "censored": 1,
                            "supported": 0,
                            "budget_hit": 0,
                            "final_quote_action_changed": 0,
                        }
                    )
                    self.state.post_cooldown_inventory_budget_pending_release[
                        self.state.budget_side
                    ] = None
            if self.state.post_cooldown_inventory_budget_prepared_orders:
                raise RuntimeError(
                    "inventory-budget admissions remained prepared at day end: "
                    f"{self.state.post_cooldown_inventory_budget_prepared_orders}"
                )

        if self.state.variance_time_lineage_randomized_enabled:
            for loop_item in list(self.state.variance_time_lineage_open_by_side):
                self.state.lineage_side = loop_item
                self._finalize_cooldown_lineage(
                    self.state.lineage_side,
                    outcome_ts_ms=self.state.replay_end_ts,
                    mark_px=self.state.replay_end_mark_px,
                    terminal_reason=(
                        "sync_adjust_unsupported_censor"
                        if self.state.sync_adjust_censored
                        else "day_end_censor"
                    ),
                    censored=True,
                )
            self.state.unexpected_inventory_lifecycles = sorted(
                inventory_lifecycle_id
                for inventory_lifecycle_id in self.state.variance_time_lineage_pending_inventory_lifecycle
                if inventory_lifecycle_id != int(self.state.inventory_lifecycle_count)
            )
            if self.state.unexpected_inventory_lifecycles:
                raise RuntimeError(
                    "cooldown lineage inventory_lifecycle diagnostics were not finalized: "
                    f"{self.state.unexpected_inventory_lifecycles}"
                )
            self._finalize_cooldown_lineage_inventory_lifecycle(
                self.state.inventory_lifecycle_count,
                outcome_ts_ms=self.state.replay_end_ts,
                mark_px=self.state.replay_end_mark_px,
                closed=False,
                terminal_reason=(
                    "sync_adjust_unsupported_censor"
                    if self.state.sync_adjust_censored
                    else "day_end_mtm_censored"
                ),
            )
            if self.state.lineage_outcome_contract_version == "v2":
                self.state.assigned_count = int(sum(self.state.variance_time_lineage_assignment_counts.values()))
                self.state.emitted_count = int(len(self.state.trace_variance_time_lineage or ()))
                self.state.inventory_lifecycle_terminalized_count = int(
                    sum(
                        int(row.get("inventory_lifecycle_terminal_ts_ms", 0) or 0) > 0
                        for row in (self.state.trace_variance_time_lineage or ())
                    )
                )
                self.state.unique_lineage_count = int(
                    len(
                        {
                            str(row.get("lineage_uid", ""))
                            for row in (self.state.trace_variance_time_lineage or ())
                        }
                    )
                )
                self.state.event_type_counts: dict[str, int] = {}
                for loop_item in self.state.trace_variance_time_lineage_events or ():
                    self.state.event = loop_item
                    self.state.event_type = str(self.state.event.get("event_type", ""))
                    self.state.event_type_counts[self.state.event_type] = (
                        int(self.state.event_type_counts.get(self.state.event_type, 0)) + 1
                    )
                self.state.open_count = int(len(self.state.variance_time_lineage_open_by_side))
                self.state.pending_record_count = int(
                    sum(
                        len(records)
                        for records in self.state.variance_time_lineage_pending_inventory_lifecycle.values()
                    )
                )
                self.state.denominator_counts = {
                    "assigned": self.state.assigned_count,
                    "lineage_finalized": self.state.emitted_count,
                    "inventory_lifecycle_terminalized": self.state.inventory_lifecycle_terminalized_count,
                    "emitted": self.state.emitted_count,
                    "producer_validated": self.state.emitted_count,
                    "unique_lineage_uid": self.state.unique_lineage_count,
                    "assignment_events": int(self.state.event_type_counts.get("assignment", 0)),
                    "lineage_terminal_events": int(
                        self.state.event_type_counts.get("lineage_terminal", 0)
                    ),
                    "inventory_lifecycle_terminal_events": int(
                        self.state.event_type_counts.get("inventory_lifecycle_terminal", 0)
                    ),
                }
                self.state.coverage_complete = bool(
                    len(set(self.state.denominator_counts.values())) == 1
                    and self.state.open_count == 0
                    and self.state.pending_record_count == 0
                )
                self.state.lineage_randomized_trace_audit = {
                    "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
                    "event_schema_version": "narrowgate_lineage_event_journal.v2",
                    "denominator_counts": self.state.denominator_counts,
                    "open_lineage_count": self.state.open_count,
                    "pending_inventory_lifecycle_record_count": self.state.pending_record_count,
                    "event_type_counts": self.state.event_type_counts,
                    "coverage_complete": self.state.coverage_complete,
                }
                if not self.state.coverage_complete:
                    raise RuntimeError(
                        "lineage outcome contract v2 producer coverage failed: "
                        f"{self.state.lineage_randomized_trace_audit}"
                    )

        if self.state.sell_add_price_penalty_enabled:
            if self.state.sell_add_price_penalty_open_record is not None:
                self._finalize_sell_add_price_penalty_inventory_lifecycle(
                    int(self.state.sell_add_price_penalty_open_record["inventory_lifecycle_id"]),
                    self.state.replay_end_ts,
                    self.state.replay_end_mark_px,
                    closed=False,
                    terminal_reason=(
                        "sync_adjust_unsupported_censor"
                        if self.state.sync_adjust_censored
                        else "day_end_mtm_censored"
                    ),
                )
            self.state.assigned_count = int(sum(self.state.sell_add_price_penalty_assignment_counts.values()))
            self.state.emitted_count = int(len(self.state.trace_sell_add_price_penalty or ()))
            self.state.unique_lineage_count = int(
                len(
                    {
                        str(row.get("lineage_uid", ""))
                        for row in (self.state.trace_sell_add_price_penalty or ())
                    }
                )
            )
            self.state.event_type_counts: dict[str, int] = {}
            for loop_item in self.state.trace_sell_add_price_penalty_events or ():
                self.state.event = loop_item
                self.state.event_type = str(self.state.event.get("event_type", ""))
                self.state.event_type_counts[self.state.event_type] = int(
                    self.state.event_type_counts.get(self.state.event_type, 0)
                ) + 1
            self.state.denominator_counts = {
                "assigned": self.state.assigned_count,
                "lineage_finalized": self.state.emitted_count,
                "inventory_lifecycle_terminalized": int(
                    sum(
                        int(row.get("inventory_lifecycle_terminal_ts_ms", 0) or 0) > 0
                        for row in (self.state.trace_sell_add_price_penalty or ())
                    )
                ),
                "emitted": self.state.emitted_count,
                "producer_validated": self.state.emitted_count,
                "unique_lineage_uid": self.state.unique_lineage_count,
                "assignment_events": int(self.state.event_type_counts.get("assignment", 0)),
                "lineage_terminal_events": int(
                    self.state.event_type_counts.get("lineage_terminal", 0)
                ),
                "inventory_lifecycle_terminal_events": int(
                    self.state.event_type_counts.get("inventory_lifecycle_terminal", 0)
                ),
            }
            self.state.coverage_complete = bool(
                len(set(self.state.denominator_counts.values())) == 1
                and self.state.sell_add_price_penalty_open_record is None
            )
            self.state.sell_add_price_penalty_trace_audit = {
                "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
                "event_schema_version": "narrowgate_lineage_event_journal.v2",
                "denominator_counts": self.state.denominator_counts,
                "open_lineage_count": int(
                    self.state.sell_add_price_penalty_open_record is not None
                ),
                "pending_inventory_lifecycle_record_count": 0,
                "event_type_counts": self.state.event_type_counts,
                "coverage_complete": self.state.coverage_complete,
            }
            if not self.state.coverage_complete:
                raise RuntimeError(
                    "SELL-add price-penalty producer coverage failed: "
                    f"{self.state.sell_add_price_penalty_trace_audit}"
                )

        if self.state.multi_short_repair_enabled:
            self.state.pending_inventory_lifecycle_ids = set(self.state.multi_short_repair_pending_inventory_lifecycle)
            if self.state.multi_short_repair_open_record is not None:
                self.state.pending_inventory_lifecycle_ids.add(
                    int(self.state.multi_short_repair_open_record["inventory_lifecycle_id"])
                )
            for loop_item in sorted(self.state.pending_inventory_lifecycle_ids):
                self.state.pending_inventory_lifecycle_id = loop_item
                self._finalize_multi_short_repair_inventory_lifecycle(
                    int(self.state.pending_inventory_lifecycle_id),
                    self.state.replay_end_ts,
                    self.state.replay_end_mark_px,
                    closed=False,
                    terminal_reason=(
                        "sync_adjust_unsupported_censor"
                        if self.state.sync_adjust_censored
                        else "day_end_mtm_censored"
                    ),
                )
            self.state.assigned_count = int(sum(self.state.multi_short_repair_assignment_counts.values()))
            self.state.emitted_count = int(len(self.state.trace_multi_short_repair or ()))
            self.state.unique_lineage_count = int(
                len(
                    {
                        str(row.get("lineage_uid", ""))
                        for row in (self.state.trace_multi_short_repair or ())
                    }
                )
            )
            self.state.event_type_counts: dict[str, int] = {}
            for loop_item in self.state.trace_multi_short_repair_events or ():
                self.state.event = loop_item
                self.state.event_type = str(self.state.event.get("event_type", ""))
                self.state.event_type_counts[self.state.event_type] = int(
                    self.state.event_type_counts.get(self.state.event_type, 0)
                ) + 1
            self.state.denominator_counts = {
                "assigned": self.state.assigned_count,
                "lineage_finalized": self.state.emitted_count,
                "inventory_lifecycle_terminalized": int(
                    sum(
                        int(row.get("inventory_lifecycle_terminal_ts_ms", 0) or 0) > 0
                        for row in (self.state.trace_multi_short_repair or ())
                    )
                ),
                "emitted": self.state.emitted_count,
                "producer_validated": self.state.emitted_count,
                "unique_lineage_uid": self.state.unique_lineage_count,
                "assignment_events": int(self.state.event_type_counts.get("assignment", 0)),
                "lineage_terminal_events": int(
                    self.state.event_type_counts.get("lineage_terminal", 0)
                ),
                "inventory_lifecycle_terminal_events": int(
                    self.state.event_type_counts.get("inventory_lifecycle_terminal", 0)
                ),
            }
            self.state.coverage_complete = bool(
                len(set(self.state.denominator_counts.values())) == 1
                and self.state.multi_short_repair_open_record is None
                and not self.state.multi_short_repair_pending_inventory_lifecycle
            )
            self.state.multi_short_repair_trace_audit = {
                "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
                "event_schema_version": "narrowgate_lineage_event_journal.v2",
                "denominator_counts": self.state.denominator_counts,
                "open_lineage_count": int(self.state.multi_short_repair_open_record is not None),
                "pending_inventory_lifecycle_record_count": int(
                    len(self.state.multi_short_repair_pending_inventory_lifecycle)
                ),
                "event_type_counts": self.state.event_type_counts,
                "coverage_complete": self.state.coverage_complete,
            }
            if not self.state.coverage_complete:
                raise RuntimeError(
                    "multi-SHORT reducing-BUY producer coverage failed: "
                    f"{self.state.multi_short_repair_trace_audit}"
                )

        if self.state.fair_center_shift_enabled:
            if self.state.fair_center_open_record is not None:
                self.state.inventory_lifecycle_started = bool(
                    int(self.state.fair_center_open_record.get("inventory_lifecycle_started", 0) or 0)
                )
                self._finalize_fair_center_assignment(
                    self.state.replay_end_ts,
                    self.state.replay_end_mark_px,
                    terminal_reason=(
                        "sync_adjust_unsupported_censor"
                        if self.state.sync_adjust_censored
                        else (
                            "day_end_mtm_censored"
                            if self.state.inventory_lifecycle_started
                            else "no_fill_day_end_censored"
                        )
                    ),
                    censored=True,
                )
            self.state.assigned_count = int(sum(self.state.fair_center_assignment_counts.values()))
            self.state.emitted_count = int(len(self.state.trace_fair_center_shift or ()))
            self.state.unique_lineage_count = int(
                len(
                    {
                        str(row.get("lineage_uid", ""))
                        for row in (self.state.trace_fair_center_shift or ())
                    }
                )
            )
            self.state.event_type_counts: dict[str, int] = {}
            for loop_item in self.state.trace_fair_center_shift_events or ():
                self.state.event = loop_item
                self.state.event_type = str(self.state.event.get("event_type", ""))
                self.state.event_type_counts[self.state.event_type] = int(
                    self.state.event_type_counts.get(self.state.event_type, 0)
                ) + 1
            self.state.denominator_counts = {
                "assigned": self.state.assigned_count,
                "lineage_finalized": self.state.emitted_count,
                "inventory_lifecycle_terminalized": int(
                    sum(
                        int(row.get("inventory_lifecycle_terminal_ts_ms", 0) or 0) > 0
                        for row in (self.state.trace_fair_center_shift or ())
                    )
                ),
                "emitted": self.state.emitted_count,
                "producer_validated": self.state.emitted_count,
                "unique_lineage_uid": self.state.unique_lineage_count,
                "assignment_events": int(self.state.event_type_counts.get("assignment", 0)),
                "lineage_terminal_events": int(
                    self.state.event_type_counts.get("lineage_terminal", 0)
                ),
                "inventory_lifecycle_terminal_events": int(
                    self.state.event_type_counts.get("inventory_lifecycle_terminal", 0)
                ),
            }
            self.state.coverage_complete = bool(
                len(set(self.state.denominator_counts.values())) == 1
                and self.state.fair_center_open_record is None
            )
            self.state.fair_center_trace_audit = {
                "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
                "event_schema_version": "narrowgate_lineage_event_journal.v2",
                "denominator_counts": self.state.denominator_counts,
                "open_lineage_count": int(self.state.fair_center_open_record is not None),
                "pending_inventory_lifecycle_record_count": 0,
                "event_type_counts": self.state.event_type_counts,
                "coverage_complete": self.state.coverage_complete,
            }
            if not self.state.coverage_complete:
                raise RuntimeError(
                    "cross-venue fair-center producer coverage failed: "
                    f"{self.state.fair_center_trace_audit}"
                )

        self.state.end_ts = self.state.replay_end_ts
        for loop_item in self.state.bid_orders:
            self.state.order = loop_item
            self._append_order_outcome(self.state.order, self.state.end_ts, "open_end", "end_of_window")
        for loop_item in self.state.ask_orders:
            self.state.order = loop_item
            self._append_order_outcome(self.state.order, self.state.end_ts, "open_end", "end_of_window")

        if self.state.trace_safe_add_rearm is not None and self.state.safe_add_rearm_orders:
            self.state.end_ts = self.state.replay_end_ts
            for loop_item in list(self.state.safe_add_rearm_orders):
                self.state.order = loop_item
                self._safe_add_rearm_finalize(
                    self.state.order,
                    "day_end_censored",
                    self.state.end_ts,
                    activation_ts_ms=int(self.state.order["activate_ts"]),
                    fill_ts_ms=0,
                    fill_qty=0.0,
                    opportunity_value_30s_bps=0.0,
                )
            self.state.safe_add_rearm_orders.clear()

        # Window end is a mark-to-market boundary, not a synthetic taker close.
        # Real taker exits above still pay taker_fee; residual inventory is only
        # marked here. Keep the hypothetical liquidation fee as a diagnostic.
        if self.state.ranked_toxicity_guard_binding_active:
            for loop_item in (*self.state.bid_orders, *self.state.ask_orders):
                self.state.open_order = loop_item
                self._ranked_guard_exchange_terminal(
                    self.state.open_order,
                    reason="shutdown",
                    visibility_ts_ms=int(self.state.replay_end_ts),
                    exchange_ts_ms=int(self.state.replay_end_ts),
                )
            self.state.ranked_toxicity_guard_binding_audit = (
                self.context.ranked_toxicity_guard_binding.finish_replay(
                    event_ts_ns=int(self.state.replay_end_ts) * 1_000_000,
                )
            )
        self.state.local_order_lifecycle.censor_all(self.state.replay_end_ts)
        if self.state.order_lifecycle_journal_v2_adapter is not None:
            self.state.order_lifecycle_journal_v2_adapter.shutdown_censor_all(self.state.replay_end_ts)
            self.state.order_lifecycle_journal_v2_health = self.state.order_lifecycle_journal_v2_adapter.close()
        self.state.trace_local_order_lifecycle = self.state.local_order_lifecycle.events(copy_rows=False)
        for loop_item in list(self.state.local_order_value_open_records):
            self.state.open_order_id = loop_item
            self._finalize_local_order_value_observation(
                int(self.state.open_order_id),
                "censored",
                self.state.replay_end_ts,
            )
        for loop_item in list(self.state.local_action_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_local_action_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.queue_value_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_queue_value_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.sell_add_skip_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_sell_add_skip_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.state_conditioned_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_state_conditioned_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.safe_add_rearm_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_safe_add_rearm_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.state_conditioned_rearm_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_state_conditioned_rearm_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                float(self.state.last_px),
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.first_add_decision_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_first_add_decision_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                self.state.replay_end_mark_px,
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        for loop_item in list(self.state.first_opener_decision_open_records):
            self.state.open_inventory_lifecycle_id = loop_item
            self._finalize_first_opener_decision_inventory_lifecycle(
                self.state.open_inventory_lifecycle_id,
                self.state.replay_end_ts,
                self.state.replay_end_mark_px,
                closed=False,
                terminal_reason="day_end_mtm_censored",
            )
        if self.state.trace_first_add_decision_to_terminal is not None:
            self.state.selected_count = int(len(self.state.first_add_decision_recorded_inventory_lifecycles))
            self.state.emitted_count = int(len(self.state.trace_first_add_decision_to_terminal))
            self.state.unique_inventory_lifecycle_count = int(
                len(
                    {
                        int(row["inventory_lifecycle_id"])
                        for row in self.state.trace_first_add_decision_to_terminal
                    }
                )
            )
            self.state.exact_join_count = int(
                sum(
                    int(row.get("exact_decision_order_fill_join", 0) or 0) == 1
                    for row in self.state.trace_first_add_decision_to_terminal
                )
            )
            self.state.feature_clock_violation_count = int(
                sum(
                    int(row["decision_visible_feature_ready_ts_max_ms"])
                    > int(row["decision_ts_ms"])
                    for row in self.state.trace_first_add_decision_to_terminal
                )
            )
            self.state.coverage_complete = bool(
                self.state.selected_count
                == self.state.emitted_count
                == self.state.unique_inventory_lifecycle_count
                == self.state.exact_join_count
                and not self.state.first_add_decision_open_records
                and self.state.feature_clock_violation_count == 0
            )
            self.state.first_add_decision_trace_audit = {
                "trace_schema_version": "first_add_decision_to_terminal_trace.v1",
                "selected_inventory_lifecycle_count": self.state.selected_count,
                "emitted_row_count": self.state.emitted_count,
                "unique_inventory_lifecycle_count": self.state.unique_inventory_lifecycle_count,
                "exact_join_count": self.state.exact_join_count,
                "feature_clock_violation_count": self.state.feature_clock_violation_count,
                "open_record_count": int(len(self.state.first_add_decision_open_records)),
                "coverage_complete": self.state.coverage_complete,
            }
            if not self.state.coverage_complete:
                raise RuntimeError(
                    "first-add decision-to-terminal native trace coverage failed: "
                    f"{self.state.first_add_decision_trace_audit}"
                )
        if self.state.trace_first_opener_decision_to_terminal is not None:
            self.state.selected_count = int(len(self.state.first_opener_decision_recorded_inventory_lifecycles))
            self.state.emitted_count = int(len(self.state.trace_first_opener_decision_to_terminal))
            self.state.unique_inventory_lifecycle_count = int(
                len(
                    {
                        int(row["inventory_lifecycle_id"])
                        for row in self.state.trace_first_opener_decision_to_terminal
                    }
                )
            )
            self.state.exact_join_count = int(
                sum(
                    int(row.get("exact_decision_order_fill_join", 0) or 0) == 1
                    for row in self.state.trace_first_opener_decision_to_terminal
                )
            )

            self.state.feature_clock_violation_count = int(
                sum(
                    self._first_opener_feature_clock_invalid(row)
                    for row in self.state.trace_first_opener_decision_to_terminal
                )
            )
            self.state.unsupported_count = int(len(self.state.first_opener_decision_unsupported_inventory_lifecycles))
            self.state.eligible_true_opener_count = int(self.state.inventory_lifecycle_count) - self.state.unsupported_count
            self.state.true_opener_coverage = float(self.state.eligible_true_opener_count) / max(
                int(self.state.inventory_lifecycle_count), 1
            )
            self.state.coverage_complete = bool(
                self.state.selected_count
                == self.state.emitted_count
                == self.state.unique_inventory_lifecycle_count
                == self.state.exact_join_count
                == self.state.eligible_true_opener_count
                and self.state.selected_count + self.state.unsupported_count == int(self.state.inventory_lifecycle_count)
                and not self.state.first_opener_decision_open_records
                and self.state.feature_clock_violation_count == 0
            )
            self.state.first_opener_decision_trace_audit = {
                "trace_schema_version": self.state.first_opener_trace_schema_version,
                "inventory_lifecycle_count": int(self.state.inventory_lifecycle_count),
                "eligible_true_opener_inventory_lifecycle_count": self.state.eligible_true_opener_count,
                "true_opener_inventory_lifecycle_coverage": self.state.true_opener_coverage,
                "unsupported_nonopener_open_inventory_lifecycle_count": self.state.unsupported_count,
                "unsupported_nonopener_open_inventory_lifecycles": list(
                    self.state.first_opener_decision_unsupported_inventory_lifecycles
                ),
                "selected_inventory_lifecycle_count": self.state.selected_count,
                "emitted_row_count": self.state.emitted_count,
                "unique_inventory_lifecycle_count": self.state.unique_inventory_lifecycle_count,
                "exact_join_count": self.state.exact_join_count,
                "feature_clock_violation_count": self.state.feature_clock_violation_count,
                "open_record_count": int(len(self.state.first_opener_decision_open_records)),
                "coverage_complete": self.state.coverage_complete,
            }
            if not self.state.coverage_complete:
                raise RuntimeError(
                    "first-opener decision-to-terminal native trace coverage failed: "
                    f"{self.state.first_opener_decision_trace_audit}"
                )
        self.state.mtm_before_terminal_fee, self.state.terminal_fee_drag, self.state.final_pnl = self.services.terminal_pnl_decomposition(
            self.state.cash, self.state.q, self.state.last_px, self.state.taker_fee
        )
        self.state.terminal_liquidation_fee_estimate = abs(self.state.q) * self.state.last_px * max(0.0, self.state.taker_fee)

        # Trim arrays
        self.state.pnl_arr = self.state.pnl_arr[:self.state.si]
        self.state.inv_arr = self.state.inv_arr[:self.state.si]
        self.state.ts_arr = self.state.ts_arr[:self.state.si]

        self.state.elapsed_s = max(0.0, float(self.state.replay_end_ts - self.state.replay_origin_ts_ms) / 1000.0)
        self.state.n_days = self.services.replay_elapsed_days(self.services.np.asarray([
            self.state.replay_origin_ts_ms, self.state.replay_end_ts,
        ]))
        self.state.nf = self.state.nfb + self.state.nfa

        # Sharpe
        if self.state.si > 1:
            self.state.pnl_diff = self.services.np.diff(self.state.pnl_arr)
            self.state.dt_s = self.services.np.diff(self.state.ts_arr).astype(self.services.np.float64) / 1000.0
            self.state.valid = self.state.dt_s > 0.0
            if self.services.np.any(self.state.valid):
                self.state.dp = self.state.pnl_diff[self.state.valid]
                self.state.dt = self.state.dt_s[self.state.valid]
                self.state.mu_sec = self.state.dp.sum() / self.state.dt.sum()
                self.state.diff_norm = self.state.dp / self.services.np.sqrt(self.state.dt)
                self.state.sigma_sec = self.state.diff_norm.std()
                if self.state.sigma_sec > 0:
                    self.state.sharpe = self.state.mu_sec / self.state.sigma_sec * self.services.math.sqrt(365.25 * 86400.0)
                else:
                    self.state.sharpe = 0.0
            else:
                self.state.sharpe = 0.0
        else:
            self.state.sharpe = 0.0

        # Max drawdown
        if self.state.si > 0:
            self.state.cum_max = self.services.np.maximum.accumulate(self.state.pnl_arr)
            self.state.dd = self.state.cum_max - self.state.pnl_arr
            self.state.max_dd = float(self.state.dd.max())
        else:
            self.state.max_dd = 0.0

        self.state.avg_markout = self.state.mo_sum_all / self.state.mo_qty_all if self.state.mo_qty_all > 0 else 0.0
        self.state.avg_markout_bid = self.state.mo_sum_bid / self.state.mo_qty_bid if self.state.mo_qty_bid > 0 else 0.0
        self.state.avg_markout_ask = self.state.mo_sum_ask / self.state.mo_qty_ask if self.state.mo_qty_ask > 0 else 0.0
        self.state.avg_delta_pre_cap = self.state.delta_pre_cap_sum / max(self.state.nrq, 1)
        self.state.avg_delta_after_cap = self.state.delta_after_cap_sum / max(self.state.n_final_spread, 1)
        self.state.cap_hit_rate = self.state.cap_hit_count / max(self.state.nrq, 1)
        self.state.delta_cap_hit_rate = self.state.delta_cap_hit_count / max(self.state.nrq, 1)
        self.state.final_cap_compress_rate = self.state.final_cap_compress_count / max(self.state.nrq, 1)
        self.state.final_cap_rounding_rate = self.state.final_cap_rounding_count / max(self.state.nrq, 1)
        self.state.final_cap_mid_guard_rate = self.state.final_cap_mid_guard_count / max(self.state.nrq, 1)
        self.state.final_cap_post_only_rate = self.state.final_cap_post_only_count / max(self.state.nrq, 1)
        self.state.final_cap_delta_rate = self.state.final_cap_delta_count / max(self.state.nrq, 1)
        self.state.avg_final_cap_excess = self.state.final_cap_excess_sum / max(self.state.final_cap_compress_count, 1)
        self.state.post_policy_cap_rate = self.state.post_policy_cap_count / max(self.state.nrq, 1)
        self.state.avg_post_policy_cap_excess = self.state.post_policy_cap_excess_sum / max(self.state.post_policy_cap_count, 1)
        self.state.mid_guard_bid_rate = self.state.mid_guard_bid_hits / max(self.state.nrq, 1)
        self.state.mid_guard_ask_rate = self.state.mid_guard_ask_hits / max(self.state.nrq, 1)
        self.state.post_only_bid_rate = self.state.post_only_bid_hits / max(self.state.nrq, 1)
        self.state.post_only_ask_rate = self.state.post_only_ask_hits / max(self.state.nrq, 1)
        self.state.avg_cap_bps = self.state.cap_bps_sum / max(self.state.nrq, 1)
        self.state.avg_bid_dist = self.state.bid_dist_sum / max(self.state.n_final_spread, 1)
        self.state.avg_ask_dist = self.state.ask_dist_sum / max(self.state.n_final_spread, 1)
        self.state.avg_quote_skew_pct = 100.0 * self.state.quote_skew_sum / max(self.state.n_final_spread, 1)
        self.state.avg_abs_quote_skew_pct = 100.0 * self.state.quote_abs_skew_sum / max(self.state.n_final_spread, 1)
        self.state.quote_abs_skew_gt_10_rate = self.state.quote_abs_skew_gt_10_count / max(self.state.n_final_spread, 1)
        self.state.quote_abs_skew_gt_25_rate = self.state.quote_abs_skew_gt_25_count / max(self.state.n_final_spread, 1)
        self.state.bid_tighter_rate = self.state.bid_tighter_count / max(self.state.n_final_spread, 1)
        self.state.ask_tighter_rate = self.state.ask_tighter_count / max(self.state.n_final_spread, 1)
        self.state.quote_balanced_rate = self.state.quote_balanced_count / max(self.state.n_final_spread, 1)
        self.state.avg_markout_final_compressed = self.state.mo_sum_final_compressed / max(self.state.mo_qty_final_compressed, 1e-12)
        self.state.avg_markout_not_final_compressed = self.state.mo_sum_not_final_compressed / max(self.state.mo_qty_not_final_compressed, 1e-12)
        self.state.quote_attempts = self.state.nrq + self.state.stale_book_skip_count
        self.state.stale_book_skip_rate = self.state.stale_book_skip_count / max(self.state.quote_attempts, 1)
        self.state.quote_attempts_per_day = self.state.quote_attempts / max(self.state.n_days, 0.01)
        self.state.requotes_per_day = self.state.nrq / max(self.state.n_days, 0.01)
        self.state.avg_stale_book_age_ms = self.state.stale_book_age_sum / max(self.state.stale_book_skip_count, 1)
        # InvAdj = raw PnL minus inventory-path price drift.  It helps separate
        # spread/queue accounting from passive market drift, but it can also mask
        # adverse selection.  Always read together with raw PnL, maker-signed
        # markout, tail loss, inventory-time, false-block, and daily stability.
        self.state.inventory_adjusted_pnl = self.state.final_pnl - self.state.inventory_pnl
        self.state.inventory_cost = -self.state.inventory_pnl
        self.state.inv_time_hours = self.state.abs_inventory_time_s / 3600.0
        self.state.time_avg_abs_inventory = self.state.abs_inventory_time_s / max(self.state.elapsed_s, 1e-9)
        self.state.time_avg_signed_inventory = self.state.signed_inventory_time_s / max(self.state.elapsed_s, 1e-9)
        self.state.time_avg_sq_inventory = self.state.sq_inventory_time_s / max(self.state.elapsed_s, 1e-9)
        self.state.time_avg_notional_inventory = self.state.notional_inventory_time_s / max(self.state.elapsed_s, 1e-9)
        self.state.pnl_per_abs_inventory_hour = self.state.final_pnl / self.state.inv_time_hours if self.state.inv_time_hours > 1e-12 else 0.0
        self.state.inv_adj_pnl_per_abs_inventory_hour = (
            self.state.inventory_adjusted_pnl / self.state.inv_time_hours if self.state.inv_time_hours > 1e-12 else 0.0
        )
        if self.state.dynamic_cap_enabled and self.state.dynamic_cap_base_bps > 0.0:
            self.state.cap_mode = "dynamic"
            self.state.cap_bps = self.state.dynamic_cap_base_bps
        elif self.state.max_spread_bps > 0.0:
            self.state.cap_mode = "fixed"
            self.state.cap_bps = self.state.max_spread_bps
        else:
            self.state.cap_mode = "off"
            self.state.cap_bps = 0.0
        self.state.cap_label = self.context.params.get("cap_label", self.services._cap_label_from_fields(self.state.cap_mode, self.state.cap_bps))
        self.state.inventory_lifecycle_open_count = max(0, self.state.inventory_lifecycle_count - self.state.inventory_lifecycle_closed_count)
        self.state.reference_scheduler_stats = (
            self.state.reference_scheduler.stats() if self.state.reference_scheduler is not None else None
        )
        self.state.exchange_book_scheduler_stats = (
            self.state.exchange_book_scheduler.stats()
            if self.state.exchange_book_scheduler is not None
            else None
        )
        self.state.dynamic_fill_hazard_visibility_book_stats = (
            self.state.dynamic_fill_hazard_visibility_scheduler.book_stats()
            if self.state.dynamic_fill_hazard_visibility_scheduler is not None
            else None
        )
        self.state.dynamic_fill_hazard_python_book_stats = (
            self.state.dynamic_fill_hazard_visibility_book_stats
            if self.state.dynamic_fill_hazard_visibility_book_stats is not None
            else self.state.exchange_book_scheduler_stats
        )
        self.state.final_replay_ts_ms = int(self.state.trade_ts[-1]) if self.state.n_trades > 0 else 0
        if self.state.variance_time_enabled and self.state.final_replay_ts_ms > 0:
            self._advance_variance_episode("BUY", self.state.final_replay_ts_ms)
            self._advance_variance_episode("SELL", self.state.final_replay_ts_ms)
        self.state.variance_time_buy_snapshot = (
            self.state.variance_episode_buy.snapshot()
            if self.state.variance_episode_buy is not None
            else None
        )
        self.state.variance_time_sell_snapshot = (
            self.state.variance_episode_sell.snapshot()
            if self.state.variance_episode_sell is not None
            else None
        )
        self.state.dynamic_fill_hazard_cpp_counters: dict[str, int] = {}
        self.state.dynamic_fill_hazard_cpp_sequence_stats: dict[str, int] = {}
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            self.state.dynamic_fill_hazard_cpp_counters = (
                self.state.dynamic_fill_hazard_cpp_runtime.counters()
            )
            self.state.dynamic_fill_hazard_cpp_sequence_stats = (
                self.state.dynamic_fill_hazard_cpp_runtime.sequence_stats()
            )
            self.state.python_counters = {
                "eval_count": int(self.state.dynamic_fill_hazard_eval_count),
                "valid_eval_count": int(self.state.dynamic_fill_hazard_valid_eval_count),
                "invalid_eval_count": int(self.state.dynamic_fill_hazard_invalid_eval_count),
                "keep_count": int(self.state.dynamic_fill_hazard_keep_count),
                "cancel_request_count": int(
                    self.state.dynamic_fill_hazard_cancel_request_count
                ),
                "cancel_ack_count": int(self.state.dynamic_fill_hazard_cancel_ack_count),
                "pre_ack_fill_count": int(
                    self.state.dynamic_fill_hazard_pre_ack_fill_count
                ),
                "recovery_count": int(self.state.dynamic_fill_hazard_recovery_count),
                "reentry_count": int(self.state.dynamic_fill_hazard_reentry_count),
                "retain_invalid_count": int(
                    self.state.dynamic_fill_hazard_retain_invalid_count
                ),
            }
            self._dynamic_fill_hazard_cpp_check(
                self.state.dynamic_fill_hazard_cpp_counters == self.state.python_counters,
                "final_counters",
                python=self.state.python_counters,
                cpp=self.state.dynamic_fill_hazard_cpp_counters,
            )
            self.state.cpp_prospective_counters = (
                self.state.dynamic_fill_hazard_cpp_runtime.prospective_counters()
            )
            self.state.python_prospective_counters = {
                "prospective_eval_count": int(
                    self.state.dynamic_fill_hazard_prospective_eval_count
                ),
                "prospective_valid_count": int(
                    self.state.dynamic_fill_hazard_prospective_valid_count
                ),
                "prospective_invalid_count": int(
                    self.state.dynamic_fill_hazard_prospective_invalid_count
                ),
            }
            self._dynamic_fill_hazard_cpp_check(
                self.state.cpp_prospective_counters == self.state.python_prospective_counters,
                "final_prospective_counters",
                python=self.state.python_prospective_counters,
                cpp=self.state.cpp_prospective_counters,
            )
            self._dynamic_fill_hazard_cpp_check(
                bool(self.state.dynamic_fill_hazard_cpp_runtime.hold_active)
                == bool(self.state.dynamic_fill_hazard_hold is not None)
                and (
                    not self.state.dynamic_fill_hazard_cpp_runtime.hold_active
                    or self.state.dynamic_fill_hazard_cpp_runtime.hold_order_id
                    == str(self.state.dynamic_fill_hazard_hold["client_order_id"])
                ),
                "final_hold_state",
                python_hold=(
                    str(self.state.dynamic_fill_hazard_hold["client_order_id"])
                    if self.state.dynamic_fill_hazard_hold is not None
                    else ""
                ),
                cpp_hold=self.state.dynamic_fill_hazard_cpp_runtime.hold_order_id,
            )
            if self.state.dynamic_fill_hazard_python_book_stats is not None:
                self.state.sequence_pairs = {
                    "snapshot_messages": (
                        int(self.state.dynamic_fill_hazard_python_book_stats.snapshot_events),
                        int(
                            self.state.dynamic_fill_hazard_cpp_sequence_stats.get(
                                "snapshot_messages",
                                -1,
                            )
                        ),
                    ),
                    "accepted_updates": (
                        int(self.state.dynamic_fill_hazard_python_book_stats.delta_events),
                        int(
                            self.state.dynamic_fill_hazard_cpp_sequence_stats.get(
                                "accepted_updates",
                                -1,
                            )
                        ),
                    ),
                    "sequence_gaps": (
                        int(self.state.dynamic_fill_hazard_python_book_stats.sequence_gaps),
                        int(
                            self.state.dynamic_fill_hazard_cpp_sequence_stats.get(
                                "sequence_gaps",
                                -1,
                            )
                        ),
                    ),
                    "invalid_sequence_messages": (
                        int(
                            self.state.dynamic_fill_hazard_python_book_stats.invalid_sequence_messages
                        ),
                        int(
                            self.state.dynamic_fill_hazard_cpp_sequence_stats.get(
                                "invalid_sequence_messages",
                                -1,
                            )
                        ),
                    ),
                    "message_time_reversals": (
                        int(
                            self.state.dynamic_fill_hazard_python_book_stats.message_time_reversals
                        ),
                        int(
                            self.state.dynamic_fill_hazard_cpp_sequence_stats.get(
                                "message_time_reversals",
                                -1,
                            )
                        ),
                    ),
                }
                self.state.bad_sequence = {
                    key: {"python": values[0], "cpp": values[1]}
                    for key, values in self.state.sequence_pairs.items()
                    if values[0] != values[1]
                }
                self._dynamic_fill_hazard_cpp_check(
                    self.state.dynamic_fill_hazard_cpp_book_event_count
                    == int(self.state.dynamic_fill_hazard_python_book_stats.consumed_events)
                    and not self.state.bad_sequence,
                    "final_native_book_stats",
                    python_consumed=int(
                        self.state.dynamic_fill_hazard_python_book_stats.consumed_events
                    ),
                    cpp_consumed=int(self.state.dynamic_fill_hazard_cpp_book_event_count),
                    stat_mismatches=self.state.bad_sequence,
                )

        self.state.cooldown_v2_snapshot_emitter_audit: dict[str, self.services.Any] = {}
        if self.state.cooldown_v2_snapshot_emitter is not None:
            self.state.raw_emitter_audit = self.state.cooldown_v2_snapshot_emitter.audit()
            if hasattr(self.state.raw_emitter_audit, "__dataclass_fields__"):
                self.state.cooldown_v2_snapshot_emitter_audit = self.services.asdict(self.state.raw_emitter_audit)
            elif isinstance(self.state.raw_emitter_audit, self.services.Mapping):
                self.state.cooldown_v2_snapshot_emitter_audit = dict(self.state.raw_emitter_audit)
            else:
                raise RuntimeError("cooldown v2 emitter returned an invalid audit")
        self.state.cooldown_duration_shared_prefix_audit: dict[str, self.services.Any] = {}
        if self.state.cooldown_duration_shared_prefix_executor is not None:
            self.state.raw_shared_prefix_audit = self.state.cooldown_duration_shared_prefix_executor.audit()
            if hasattr(self.state.raw_shared_prefix_audit, "__dataclass_fields__"):
                self.state.cooldown_duration_shared_prefix_audit = self.services.asdict(
                    self.state.raw_shared_prefix_audit
                )
            elif isinstance(self.state.raw_shared_prefix_audit, self.services.Mapping):
                self.state.cooldown_duration_shared_prefix_audit = dict(
                    self.state.raw_shared_prefix_audit
                )
            else:
                raise RuntimeError(
                    "cooldown shared-prefix executor returned an invalid audit"
                )
        self.state.cooldown_duration_policy_audit: dict[str, self.services.Any] = {}
        if self.state.cooldown_duration_policy_evaluator is not None:
            self.state.raw_policy_audit = self.state.cooldown_duration_policy_evaluator.audit()
            if hasattr(self.state.raw_policy_audit, "__dataclass_fields__"):
                self.state.cooldown_duration_policy_audit = self.services.asdict(self.state.raw_policy_audit)
            elif isinstance(self.state.raw_policy_audit, self.services.Mapping):
                self.state.cooldown_duration_policy_audit = dict(self.state.raw_policy_audit)
            else:
                raise RuntimeError(
                    "cooldown duration policy evaluator returned an invalid audit"
                )

        self.state.planned_shutdown_open_order_count = sum(
            order.get("state") == self.state.ORDER_OPEN
            and float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE
            for order in (*self.state.bid_orders, *self.state.ask_orders)
        )
        self.state.planned_shutdown_pending_new_order_count = sum(
            order.get("state") == self.state.ORDER_PENDING_NEW
            and float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE
            for order in (*self.state.bid_orders, *self.state.ask_orders)
        )
        self.state.planned_shutdown_pending_cancel_order_count = sum(
            order.get("state") == self.state.ORDER_PENDING_CANCEL
            and float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE
            for order in (*self.state.bid_orders, *self.state.ask_orders)
        )

        self.state.result = {
            "eta_inventory": self.state.eta_inventory,
            "risk_per_order": self.state.risk_per_order,
            "planned_quote_stop_ts_ms": int(self.state.planned_quote_stop_ts_ms),
            "planned_quote_stop_triggered": bool(self.state.planned_quote_stop_triggered),
            "planned_quote_stop_trigger_ts_ms": int(
                self.state.planned_quote_stop_trigger_ts_ms
            ),
            "planned_shutdown_orders_at_trigger": int(
                self.state.planned_shutdown_orders_at_trigger
            ),
            "planned_shutdown_open_order_count": int(
                self.state.planned_shutdown_open_order_count
            ),
            "planned_shutdown_pending_new_order_count": int(
                self.state.planned_shutdown_pending_new_order_count
            ),
            "planned_shutdown_pending_cancel_order_count": int(
                self.state.planned_shutdown_pending_cancel_order_count
            ),
            "ema_add_wait_fork_enabled": bool(self.state.ema_add_wait_fork_enabled),
            "ema_add_wait_fork_action": str(self.state.ema_add_wait_fork_action),
            "ema_add_wait_fork_assigned": bool(self.state.ema_add_wait_fork_assigned),
            "ema_add_wait_fork_terminal": bool(self.state.ema_add_wait_fork_terminal),
            "ema_add_wait_fork_right_censored": bool(
                self.state.ema_add_wait_fork_enabled and not self.state.ema_add_wait_fork_terminal
            ),
            "buy_soft_widen_release_probe_enabled": bool(
                self.state.buy_soft_widen_release_probe_enabled
            ),
            "buy_soft_widen_release_probe_apply_candidate": bool(
                self.state.buy_soft_widen_release_probe_apply_candidate
            ),
            "buy_soft_widen_release_target_ts_ms": int(
                self.state.buy_soft_widen_release_target_ts_ms
            ),
            "buy_soft_widen_release_target_role": str(
                self.state.buy_soft_widen_release_target_role
            ),
            "buy_soft_widen_release_target_reached_count": int(
                self.state.buy_soft_widen_release_target_reached_count
            ),
            "buy_soft_widen_release_eligible_count": int(
                self.state.buy_soft_widen_release_eligible_count
            ),
            "buy_soft_widen_release_requested_count": int(
                self.state.buy_soft_widen_release_requested_count
            ),
            "buy_soft_widen_release_effective_mult_count": int(
                self.state.buy_soft_widen_release_effective_mult_count
            ),
            "buy_soft_widen_release_effective_price_count": int(
                self.state.buy_soft_widen_release_effective_price_count
            ),
            "buy_soft_widen_release_role_observed": str(
                self.state.buy_soft_widen_release_role_observed
            ),
            "buy_soft_widen_release_reason": str(self.state.buy_soft_widen_release_reason),
            "buy_soft_widen_release_baseline_spread_mult": float(
                self.state.buy_soft_widen_release_baseline_spread_mult
            ),
            "buy_soft_widen_release_selected_spread_mult": float(
                self.state.buy_soft_widen_release_selected_spread_mult
            ),
            "buy_soft_widen_release_baseline_bid_price": float(
                self.state.buy_soft_widen_release_baseline_bid_price
            ),
            "buy_soft_widen_release_candidate_bid_price": float(
                self.state.buy_soft_widen_release_candidate_bid_price
            ),
            "conditional_p3_reach_gate_enabled": bool(
                self.state.conditional_p3_reach_gate_enabled
            ),
            "conditional_p3_reach_gate_outward_ticks": int(
                self.state.conditional_p3_reach_gate_outward_ticks
            ),
            "conditional_p3_reach_gate_grid_min_ticks": int(
                self.state.conditional_p3_reach_gate_grid_min_ticks
            ),
            "conditional_p3_reach_gate_eval_count": int(
                self.state.conditional_p3_reach_gate_eval_count
            ),
            "conditional_p3_reach_gate_toxicity_trigger_count": int(
                self.state.conditional_p3_reach_gate_toxicity_trigger_count
            ),
            "conditional_p3_reach_gate_supported_count": int(
                self.state.conditional_p3_reach_gate_supported_count
            ),
            "conditional_p3_reach_gate_pass_count": int(
                self.state.conditional_p3_reach_gate_pass_count
            ),
            "conditional_p3_reach_gate_price_change_count": int(
                self.state.conditional_p3_reach_gate_price_change_count
            ),
            "conditional_p3_reach_gate_spread_cap_noop_count": int(
                self.state.conditional_p3_reach_gate_spread_cap_noop_count
            ),
            "conditional_p3_reach_gate_buy_price_change_count": int(
                self.state.conditional_p3_reach_gate_buy_price_change_count
            ),
            "conditional_p3_reach_gate_sell_price_change_count": int(
                self.state.conditional_p3_reach_gate_sell_price_change_count
            ),
            "ranked_toxicity_guard_binding_enabled": bool(
                self.state.ranked_toxicity_guard_binding_active
            ),
            "ranked_toxicity_guard_binding_audit": (
                self.state.ranked_toxicity_guard_binding_audit
            ),
            "strict_calibration": bool(self.context.params.get("strict_calibration", False)),
            "strict_calibration_validated": bool(self.context.params.get("strict_calibration_validated", False)),
            "random_passive_enabled": self.state.random_passive_enabled,
            "random_passive_sampler_version": self.services.RANDOM_PASSIVE_SAMPLER_VERSION,
            "random_passive_seed": self.state.random_passive_seed,
            "random_passive_side_mirror_prob": self.state.random_passive_side_mirror_prob,
            "random_passive_timing_jitter_fraction": self.state.random_passive_timing_jitter_fraction,
            "random_passive_preserve_inventory_skew": self.state.random_passive_preserve_inventory_skew,
            "random_passive_mirror_count": self.state.random_passive_mirror_count,
            "random_passive_mirror_eligible_count": self.state.random_passive_mirror_eligible_count,
            "random_passive_timing_jitter_count": self.state.random_passive_timing_jitter_count,
            "local_action_ope_enabled": bool(self.state.local_action_ope_enabled),
            "local_action_ope_seed": int(self.state.local_action_seed),
            "local_action_ope_assignment_count": int(
                sum(self.state.local_action_assignment_counts.values())
            ),
            "local_action_ope_probabilities": dict(self.state.local_action_probabilities),
            **{
                f"local_action_ope_{action}_count": int(
                    self.state.local_action_assignment_counts[action]
                )
                for action in self.services.LOCAL_ACTIONS
            },
            "local_order_value_observation_enabled": bool(
                self.state.trace_local_order_value is not None
            ),
            "local_order_value_observation_count": int(
                len(self.state.trace_local_order_value)
                if self.state.trace_local_order_value is not None
                else 0
            ),
            "local_order_value_fill_horizon_ms": int(
                self.state.local_order_value_fill_horizon_ms
            ),
            "local_order_value_price_jump_ticks": float(
                self.state.local_order_value_price_jump_ticks
            ),
            "queue_value_keep_cancel_enabled": bool(
                self.state.queue_value_keep_cancel_enabled
            ),
            "queue_value_action_family": str(self.state.queue_value_action_family),
            "queue_value_keep_cancel_sides": sorted(self.state.queue_value_sides),
            "queue_value_keep_cancel_seed": int(self.state.queue_value_seed),
            "queue_value_keep_cancel_assignment_count": int(
                sum(self.state.queue_value_assignment_counts.values())
            ),
            "queue_value_keep_cancel_probabilities": dict(
                self.state.queue_value_probabilities
            ),
            "queue_value_runtime_event_source": dict(
                self.state.queue_value_runtime_event_source
            ),
            "queue_value_runtime_event_counts": {
                side: dict(counts)
                for side, counts in self.state.queue_value_runtime_event_counts.items()
            },
            "queue_value_fill_horizon_ms": int(self.state.queue_value_fill_horizon_ms),
            "queue_value_price_jump_ticks": float(
                self.state.queue_value_price_jump_ticks
            ),
            **{
                f"queue_value_keep_cancel_{action}_count": int(
                    self.state.queue_value_assignment_counts[action]
                )
                for action in self.state.queue_value_actions
            },
            "sell_add_skip_ope_enabled": bool(self.state.sell_add_skip_ope_enabled),
            "sell_add_skip_ope_family_id": self.state.sell_add_skip_family_id,
            "sell_add_skip_ope_mode": self.state.sell_add_skip_mode,
            "sell_add_skip_ope_sides": sorted(self.state.sell_add_skip_sides),
            "sell_add_skip_ope_seed": int(self.state.sell_add_skip_seed),
            "sell_add_skip_ope_assignment_count": int(
                sum(self.state.sell_add_skip_assignment_counts.values())
            ),
            "sell_add_skip_ope_probabilities": dict(
                self.state.sell_add_skip_probabilities
            ),
            **{
                f"sell_add_skip_ope_{action}_count": int(
                    self.state.sell_add_skip_assignment_counts[action]
                )
                for action in self.state.sell_add_skip_actions
            },
            "state_conditioned_policy_mode": self.state.state_conditioned_policy_mode,
            "state_conditioned_policy_id": (
                self.state.state_conditioned_policy.artifact.policy_id
                if self.state.state_conditioned_policy is not None
                else ""
            ),
            "state_conditioned_policy_assignment_count": int(
                sum(self.state.state_conditioned_executed_counts.values())
            ),
            **{
                f"state_conditioned_policy_candidate_{action}_count": int(
                    self.state.state_conditioned_candidate_counts[action]
                )
                for action in self.services.LOCAL_ACTIONS
            },
            **{
                f"state_conditioned_policy_executed_{action}_count": int(
                    self.state.state_conditioned_executed_counts[action]
                )
                for action in self.services.LOCAL_ACTIONS
            },
            "safe_add_rearm_randomized_enabled": bool(
                self.state.safe_add_rearm_randomized_enabled
            ),
            "safe_add_rearm_randomized_seed": int(self.state.safe_add_rearm_randomized_seed),
            "safe_add_rearm_randomized_elapsed_s": float(
                self.state.safe_add_rearm_randomized_elapsed_ms / 1000.0
            ),
            "safe_add_rearm_randomized_assignment_count": int(
                sum(self.state.safe_add_rearm_assignment_counts.values())
            ),
            "safe_add_rearm_randomized_probabilities": dict(
                self.state.safe_add_rearm_randomized_probabilities
            ),
            **{
                f"safe_add_rearm_randomized_{action}_count": int(
                    self.state.safe_add_rearm_assignment_counts[action]
                )
                for action in self.services.SAFE_ADD_REARM_ACTIONS
            },
            "state_conditioned_rearm_enabled": bool(
                self.state.state_conditioned_rearm_enabled
            ),
            "state_conditioned_rearm_sides": sorted(
                self.state.state_conditioned_rearm_sides
            ),
            "state_conditioned_rearm_seed": int(self.state.state_conditioned_rearm_seed),
            "state_conditioned_rearm_min_elapsed_s": float(
                self.state.state_conditioned_rearm_min_elapsed_ms / 1_000.0
            ),
            "state_conditioned_rearm_assignment_count": int(
                sum(self.state.state_conditioned_rearm_assignment_counts.values())
            ),
            "state_conditioned_rearm_probabilities": dict(
                self.state.state_conditioned_rearm_probabilities
            ),
            **{
                f"state_conditioned_rearm_{action}_count": int(
                    self.state.state_conditioned_rearm_assignment_counts[action]
                )
                for action in self.services.STATE_CONDITIONED_REARM_ACTIONS
            },
            "execution_intensity_slope": self.state.execution_intensity_slope,
            "queue_base": self.state.queue_base,
            "queue_decay": self.state.queue_decay,
            "queue_ahead_mode": self.state.queue_ahead_mode,
            "book_source_authority": str(
                self.context.params.get("_book_source_authority", "unclassified")
            ),
            "book_dataset_version": str(
                self.context.params.get("_book_dataset_version", "")
            ),
            "book_exact_queue_policy_eligible": bool(
                self.context.params.get("_book_exact_queue_policy_eligible", False)
            ),
            "queue_price_tolerance": self.state.queue_price_tolerance,
            "queue_regime_calibration_enabled": bool(self.state.queue_regime_lookup or self.state.queue_deplete_lookup or self.state.queue_mo_lookup),
            "maker_fill_prob": self.state.maker_fill_prob,
            "queue_ahead_base_mult": self.state.queue_ahead_base_mult,
            "queue_deplete_base_mult": self.state.queue_deplete_base_mult,
            "queue_l2_cancel_ahead_enabled": self.state.queue_l2_cancel_ahead_enabled,
            "queue_l2_cancel_ahead_event_count": self.state.queue_l2_cancel_ahead_event_count,
            "queue_l2_cancel_ahead_bid_event_count": self.state.queue_l2_cancel_ahead_bid_event_count,
            "queue_l2_cancel_ahead_ask_event_count": self.state.queue_l2_cancel_ahead_ask_event_count,
            "queue_l2_cancel_ahead_qty": self.state.queue_l2_cancel_ahead_qty,
            "active_order_queue_mode": self.state.active_order_queue_mode,
            "active_order_queue_scope": "seed_only_v1",
            "active_order_queue_lookup_count": self.state.active_order_queue_lookup_count,
            "active_order_queue_exact_count": self.state.active_order_queue_exact_count,
            "active_order_queue_known_zero_count": (
                self.state.active_order_queue_known_zero_count
            ),
            "active_order_queue_missing_count": self.state.active_order_queue_missing_count,
            "active_order_queue_unusable_count": self.state.active_order_queue_unusable_count,
            "exchange_book_queue_mode": self.state.exchange_book_queue_mode,
            "exchange_book_queue_scope": (
                self.state.exchange_book_scheduler.evidence_scope
                if self.state.exchange_book_scheduler is not None
                else "disabled"
            ),
            "exchange_book_queue_lookup_count": (
                self.state.exchange_book_queue_lookup_count
            ),
            "exchange_book_queue_exact_count": (
                self.state.exchange_book_queue_exact_count
            ),
            "exchange_book_queue_known_zero_count": (
                self.state.exchange_book_queue_known_zero_count
            ),
            "exchange_book_queue_missing_count": (
                self.state.exchange_book_queue_missing_count
            ),
            "exchange_book_queue_invalidated_order_count": (
                self.state.exchange_book_queue_invalidated_order_count
            ),
            "exchange_book_queue_cancel_ahead_event_count": (
                self.state.exchange_book_queue_cancel_ahead_event_count
            ),
            "exchange_book_queue_cancel_ahead_qty": (
                self.state.exchange_book_queue_cancel_ahead_qty
            ),
            "exchange_book_queue_ambiguous_event_count": (
                self.state.exchange_book_queue_ambiguous_event_count
            ),
            "exchange_book_cancel_trade_ambiguous_order_count": (
                self.state.exchange_book_cancel_trade_ambiguous_order_count
            ),
            "exchange_book_cancel_book_ambiguous_order_count": (
                self.state.exchange_book_cancel_book_ambiguous_order_count
            ),
            "exchange_book_events_consumed": (
                self.state.exchange_book_scheduler_stats.consumed_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_events_accepted": (
                self.state.exchange_book_scheduler_stats.accepted_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_events_rejected": (
                self.state.exchange_book_scheduler_stats.rejected_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_source_gap_events": (
                self.state.exchange_book_scheduler_stats.source_gap_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_invalid_sequence_messages": (
                self.state.exchange_book_scheduler_stats.invalid_sequence_messages
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_snapshot_events": (
                self.state.exchange_book_scheduler_stats.snapshot_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_delta_events": (
                self.state.exchange_book_scheduler_stats.delta_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_delta_bootstrap_events": (
                self.state.exchange_book_scheduler_stats.delta_bootstrap_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_sequence_gaps": (
                self.state.exchange_book_scheduler_stats.sequence_gaps
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_message_time_reversals": (
                self.state.exchange_book_scheduler_stats.message_time_reversals
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_transaction_timestamp_events": (
                self.state.exchange_book_scheduler_stats.transaction_timestamp_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_event_timestamp_fallback_events": (
                self.state.exchange_book_scheduler_stats.event_timestamp_fallback_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_receive_timestamp_fallback_events": (
                self.state.exchange_book_scheduler_stats.receive_timestamp_fallback_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "exchange_book_unknown_timestamp_source_events": (
                self.state.exchange_book_scheduler_stats.unknown_timestamp_source_events
                if self.state.exchange_book_scheduler_stats is not None
                else 0
            ),
            "queue_ahead_buy_exposure_mult": self.state.queue_ahead_buy_exposure_mult,
            "queue_ahead_buy_reducing_mult": self.state.queue_ahead_buy_reducing_mult,
            "queue_ahead_sell_exposure_mult": self.state.queue_ahead_sell_exposure_mult,
            "queue_ahead_sell_reducing_mult": self.state.queue_ahead_sell_reducing_mult,
            "new_order_latency_ms": self.state.new_order_latency_ms,
            "cancel_order_latency_ms": self.state.cancel_order_latency_ms,
            "latency_jitter_ms": self.state.latency_jitter_ms,
            "replace_min_price_change_ticks": self.state.replace_min_price_change_ticks,
            "replace_price_threshold_mode": self.state.replace_price_threshold.mode,
            **({"replace_outward_r": self.state.replace_price_threshold.outward_r,
                "replace_outward_static_ticks": self.state.replace_price_threshold.static_ticks}
               if self.state.replace_price_threshold.mode in ("dynamic_outward", "static_outward") else {}),
            "replace_min_price_change_ticks_outward": self.state.replace_price_threshold.outward,
            "replace_min_price_change_ticks_inward": self.state.replace_price_threshold.inward,
            "directional_replace_counts": self.state.directional_replace_counts,
            "directional_replace_samples": self.state.directional_replace_samples,
            "replace_min_price_change_ticks_reducing": self.state.replace_min_price_change_ticks_reducing,
            "replace_min_interval_ms": self.state.replace_min_interval_ms,
            "replace_min_interval_ms_reducing": self.state.replace_min_interval_ms_reducing,
            "replace_pending_coalesce": bool(self.state.replace_pending_coalesce),
            "replace_pending_coalesce_replayed": True,
            **(
                {
                    "replace_terminal_continuation": True,
                    "replace_terminal_continuation_terminal_count": (
                        self.state.replacement_terminal_terminal_count
                    ),
                    "replace_terminal_continuation_decision_count": (
                        self.state.replacement_terminal_decision_count
                    ),
                    "replace_terminal_continuation_bid_decision_count": (
                        self.state.replacement_terminal_bid_decision_count
                    ),
                    "replace_terminal_continuation_ask_decision_count": (
                        self.state.replacement_terminal_ask_decision_count
                    ),
                    "replace_terminal_continuation_decision_latency_sum_ms": (
                        self.state.replacement_terminal_decision_latency_sum_ms
                    ),
                    "replace_terminal_continuation_decision_latency_max_ms": (
                        self.state.replacement_terminal_decision_latency_max_ms
                    ),
                }
                if self.state.replace_terminal_continuation
                else {}
            ),
            "replace_cancel_first_exposure_increasing": bool(self.state.replace_cancel_first_exposure_increasing),
            **({'quote_price_policy': self.state.quote_price_runtime.audit()}
               if self.state.quote_price_runtime is not None else {}),
            "replace_cancel_first_exposure_increasing_replayed": True,
            "replace_throttle_count": self.state.replace_throttle_count,
            "replace_throttle_bid_count": self.state.replace_throttle_bid_count,
            "replace_throttle_ask_count": self.state.replace_throttle_ask_count,
            "replace_throttle_price_count": self.state.replace_throttle_price_count,
            **({"target_observation": self.state.target_observation.snapshot()}
               if self.state.target_observation is not None else {}),
            **({"quote_schedule": {
                "mode": self.state.quote_schedule_mode,
                "counters": dict(self.state.quote_schedule.counters),
                "published": dict(self.state.quote_schedule.published),
                "handled": {side: dict(versions) for side, versions in self.state.quote_schedule.handled.items()},
            }} if self.state.quote_schedule is not None else {}),
            **({"target_observation_delivery": self.state.target_production.receipt()}
               if self.state.target_journal is not None else {}),
            "replace_throttle_age_count": self.state.replace_throttle_age_count,
            "decision_place_count": self.state.decision_place_count,
            "decision_replace_count": self.state.decision_replace_count,
            "decision_keep_count": self.state.decision_keep_count,
            "decision_pause_count": self.state.decision_pause_count,
            "decision_none_count": self.state.decision_none_count,
            "decision_skip_filter_count": self.state.decision_skip_filter_count,
            "decision_pending_coalesce_count": self.state.decision_pending_coalesce_count,
            "decision_cancel_first_count": self.state.decision_cancel_first_count,
            **self.state.decision_side_role_action_counts,
            "new_order_latency_sample_count": int(self.state.new_latency_samples_ms.size),
            "cancel_order_latency_sample_count": int(self.state.cancel_latency_samples_ms.size),
            **(
                {
                    "decision_to_gateway_latency_sample_count": int(
                        self.state.decision_to_gateway_latency_samples_ms.size
                    ),
                    "decision_to_gateway_latency_seed": int(
                        self.state.decision_to_gateway_latency_seed
                    ),
                    "decision_to_gateway_latency_authority": "diagnostic_only",
                    "decision_market_snapshot_clock": (
                        "after_pre_snapshot_compute" if self.state.pre_snapshot_compute_enabled
                        else "decision_time_frozen"
                    ),
                }
                if self.state.decision_to_gateway_latency_enabled
                else {}
            ),
            **({
                "pre_snapshot_compute_sample_count": int(self.state.pre_snapshot_compute_samples_ms.size),
                "pre_snapshot_compute_count": self.state.pre_snapshot_compute_count,
                "pre_snapshot_compute_completed_count": self.state.pre_snapshot_compute_completed_count,
                "pre_snapshot_compute_abandoned_count": (
                    self.state.pre_snapshot_compute_count - self.state.pre_snapshot_compute_completed_count
                ),
                "pre_snapshot_compute_sum_ms": self.state.pre_snapshot_compute_sum_ms,
                "pre_snapshot_compute_pending_count": int(self.state.pending_quote_compute is not None),
                "pre_snapshot_compute_discarded_count": self.state.pre_snapshot_compute_discarded_count,
                "pre_snapshot_prediction_clock": "requote_entry_visible_inputs",
                "pre_snapshot_inventory_variance_clock": "capture_visible_state",
            } if self.state.pre_snapshot_compute_enabled else {}),
            **({
                "requote_tail_work_sample_count": int(self.state.requote_tail_work_samples_ms.size),
                "requote_tail_work_count": self.state.requote_tail_work_count,
                "requote_tail_work_sum_ms": self.state.requote_tail_work_sum_ms,
                "requote_tail_work_clock": (
                    "after_request_enqueue_or_no_request_compute_before_loop_sleep"
                    if self.state.async_rest_gateway else "after_last_rest_return_or_compute_before_loop_sleep"
                ),
            } if self.state.requote_tail_work_enabled else {}),
            **({
                "runtime_compute_path_counts": self.state.runtime_compute_path_counts,
                "runtime_compute_last_bucket_end_ms": self.state.runtime_compute_last_bucket_end_ms,
                "runtime_compute_sample_counts": {
                    path: len(rows) for path, rows in self.state.runtime_compute_by_path.items()
                },
                "runtime_compute_phase_placement": self.context.params["_runtime_compute_sample_semantics"],
                "runtime_compute_clock": self.context.params["runtime_compute_clock"],
            } if self.state.runtime_compute_by_path else {}),
            **({'cooldown_rearm': self.state.cooldown_rearm_runtime.audit()}
               if self.state.cooldown_rearm_runtime is not None else {}),
            **({
                "signal_startup": "cold_300_completed_aggtrade_bars",
                "signal_process_origin_ts_ms": self.state.replay_origin_ts_ms,
                "signal_price_available_ts_ms": self.state.signal_price_available_ts_ms,
                "signal_first_bucket_ms": self.state.signal_first_bucket_ms,
                "signal_completed_bars_at_warmup": self.state.signal_completed_bars,
                "signal_warmup_observed_ts_ms": self.state.signal_warmup_observed_ts_ms,
                "signal_warmup_skip_count": self.state.signal_warmup_skip_count,
            } if self.state.signal_cold_start else {}),
            **({
                "main_loop_work_sample_count": int(self.state.main_loop_work_samples_ms.shape[0]),
                "main_loop_work_pre_sum_ms": self.state.main_loop_work_pre_sum_ms,
                "main_loop_work_post_sum_ms": self.state.main_loop_work_post_sum_ms,
                "main_loop_work_clock": "paired_before_tick_and_after_tick_before_sleep",
            } if self.state.main_loop_work_enabled else {}),
            **(
                {
                    "rest_gateway_timing_mode": str(self.state.rest_gateway_timing_mode),
                    "rest_gateway_timing_profile_path": str(
                        self.state.rest_gateway_timing_profile["path"]
                    ),
                    "rest_gateway_timing_profile_row_count": int(
                        self.state.rest_gateway_timing_profile["request_present_mask"].shape[0]
                    ),
                    "rest_gateway_timing_sampled_row_count": int(
                        self.state.rest_gateway_sampled_row_count
                    ),
                    "rest_gateway_timing_authority": "diagnostic_only",
                }
                if self.state.rest_gateway_timing_enabled
                and self.state.rest_gateway_timing_profile is not None
                else {}
            ),
            **(
                {
                    "rest_gateway_timing_mode": self.state.rest_gateway_timing_mode,
                    "rest_gateway_timing_authority": "diagnostic_only",
                    "rest_gateway_sampling_assumption": (
                        "independent_requests_with_paired_effective_private_response_upper_bound"
                        if self.state.serial_rest_return_enabled else
                        "independent_request_service_times_with_paired_effective_ack"
                    ),
                    "rest_gateway_response_clock_semantics": (
                        self.state.direct_return_semantics if self.state.direct_return_samples else
                        "paired_observed_upper_bound" if self.state.serial_rest_return_enabled else
                        "private_callback_bound_service_proxy"
                    ),
                    "rest_gateway_request_count": self.state.rest_gateway_request_count,
                    "rest_gateway_busy_ms": self.state.rest_gateway_busy_ms,
                    "rest_gateway_request_wait_ms": self.state.rest_gateway_request_wait_ms,
                    "rest_gateway_busy_until_ms": self.state.rest_gateway_busy_until_ms,
                    "rest_gateway_decision_deferral_count": self.state.rest_gateway_decision_deferral_count,
                    "rest_gateway_return_pending_coalesce_count": self.state.rest_return_pending_coalesce_count,
                    "rest_gateway_pending_decision_count": int(self.state.serial_rest_decision is not None),
                    **({
                        "rest_gateway_lane": "GLOBAL_FIFO",
                        "rest_gateway_max_inflight": min(1, self.state.rest_gateway_request_count),
                        "rest_gateway_queue_capacity": self.state.async_lane_capacity,
                        "rest_gateway_queue_high_watermark": self.state.async_rest_queue_high_watermark,
                        "rest_gateway_pending_request_count": (
                            len(self.state.async_rest_queue) + int(self.state.async_rest_active is not None)
                        ),
                        "rest_gateway_unmodeled_paths": [
                            *(
                                [] if self.state.bulk_cancel_samples is not None
                                else ["bulk_safety_cancel_barrier"]
                            ),
                            "bulk_barrier_callback_admission_or_synchronous_close_interruption",
                            "completion_dispatcher_queue_and_callback_duration: HTTP RESULT "
                            "authority and admission-slot release use HTTP return, not observed "
                            "Future callback delivery/completion",
                            "IOC_account_trades_lookup_latency",
                            "gateway_failure_and_unknown_response_tape",
                            *(["emergency_ownership_conflict_fatal_cancel_and_reconciliation"]
                              if self.state.risk_emergency_ownership_conflict_count else []),
                        ],
                        "bulk_cancel_request_count": self.state.bulk_cancel_request_count,
                        "bulk_cancel_phase_model": (
                            "shared_effective_private_http_for_batch" if self.state.bulk_cancel_samples is not None
                            else "not_configured"
                        ),
                        "rest_gateway_http_result_status_by_operation": self.state.http_result_status_by_operation,
                        "replay_main_loop_synchronous_request_count": (
                            self.state.main_loop_synchronous_request_count
                        ),
                        "replay_main_loop_synchronous_wait_ms": self.state.main_loop_synchronous_wait_ms,
                        "replay_main_loop_synchronous_request_pending": (
                            self.state.main_loop_waiting_request is not None
                        ),
                        "ioc_close_completion_assumption": (
                            "max_http_return_and_private_fill_proof_no_account_trades_timing"
                        ),
                    } if self.state.async_rest_gateway else {}),
                    **({"rest_gateway_response_sample_counts": {
                        slot: int(rows.shape[0]) for slot, rows in self.state.rest_return_samples.items()
                    }} if self.state.serial_rest_return_enabled else {}),
                }
                if self.state.sampled_serial_gateway else {}
            ),
            **(
                {
                    "replay_main_loop_sleep_ms": self.state.main_loop_sleep_ms,
                    "replay_main_loop_tick_count": self.state.main_loop_tick_count,
                    "replay_main_loop_clock": (
                        "actual_tick_then_sleep_or_private_terminal_wake"
                        if self.state.async_rest_gateway else "actual_tick_and_rest_return_then_sleep"
                    ),
                    "replay_main_loop_requote_anchor": "actual_requote_start",
                    "replay_main_loop_dynamic_rq_clock": "delivered_1s_bars_before_due_check",
                    "replay_main_loop_dynamic_rq_bar_index": self.state.main_loop_rq_var_idx + (
                        self.state.input_clock_offsets["variance"] if self.state.main_loop_rq_input_active else 0
                    ),
                    "replay_main_loop_unmodeled_work": "periodic_position_sync_and_health_io",
                }
                if self.state.main_loop_enabled else {}
            ),
            **(
                {
                    "new_order_exchange_effective_latency_sample_count": int(
                        self.state.new_exchange_effective_latency_samples_ms.size
                    ),
                    "cancel_exchange_effective_latency_sample_count": int(
                        self.state.cancel_exchange_effective_latency_samples_ms.size
                    ),
                    "cancel_ack_visibility_latency_sample_count": int(
                        self.state.cancel_ack_visibility_latency_samples_ms.size
                    ),
                    "cancel_latency_split_enabled": bool(self.state.cancel_latency_split_enabled),
                }
                if (
                    self.state.new_exchange_effective_latency_samples_ms.size > 0
                    or self.state.cancel_latency_split_enabled
                )
                else {}
            ),
            **(
                {
                    "private_fill_visibility_latency_sample_count": int(
                        self.state.private_fill_visibility_latency_samples_ms.size
                    ),
                    "private_fill_visibility_enabled": True,
                    "private_fill_exchange_match_count": int(
                        self.state.private_fill_exchange_match_count
                    ),
                    "private_fill_visible_count": int(self.state.private_fill_visible_count),
                    "private_fill_pending_visibility_count": int(
                        self.state.private_fill_exchange_match_count
                        - self.state.private_fill_visible_count
                    ),
                    **({
                        "economic_pnl_complete": (
                            self.state.private_fill_exchange_match_count == self.state.private_fill_visible_count
                        ),
                        "economic_pnl_status": (
                            "complete_local_fill_ledger"
                            if self.state.private_fill_exchange_match_count == self.state.private_fill_visible_count
                            else "incomplete_pending_private_fills"
                        ),
                        "pnl_clock_scope": "local_fill_visibility_at_window_end",
                        "exchange_pending_quantity": float(self.state.exchange_inventory - self.state.q),
                    } if self.state.serial_rest_return_enabled else {}),
                }
                if self.state.private_fill_visibility_enabled
                else {}
            ),
            "exec_book_visibility_delay_enabled": bool(
                self.state.exec_book_visibility_delay_enabled
            ),
            "exec_book_visibility_delay_profile_id": str(
                self.context.params.get("exec_book_visibility_delay_profile_id", "")
            ),
            "exec_book_visibility_delay_profile_path": str(
                self.context.params.get("exec_book_visibility_delay_profile_path", "")
            ),
            **(
                {
                    "exec_source_stratified_profile_id": str(
                        self.context.params.get("exec_source_stratified_profile_id", "")
                    ),
                    "exec_source_stratified_profile_path": str(
                        self.context.params.get("exec_source_stratified_profile_path", "")
                    ),
                    "exec_source_stratified_profile_sha256": (
                        self.state.exec_source_profile_sha256
                    ),
                    "exec_source_stratified_profile_market_id": (
                        self.state.exec_source_profile_market_id
                    ),
                    "exec_source_stratified_profile_transport": (
                        self.state.exec_source_profile_transport
                    ),
                    "exec_source_stratified_authority": (
                        "diagnostic_non_authoritative"
                    ),
                }
                if self.state.exec_source_profile is not None
                else {}
            ),
            "exec_book_visibility_mode": self.state.exec_book_visibility_mode,
            **(
                {
                    "exec_message_delivery_semantics": "per_message_assigned_ready_with_channel_head_of_line",
                    "exec_message_delivery_authority": "diagnostic_non_authoritative",
                    "exec_message_delivery_sources": {
                        feed: schedule.stats_dict()
                        for feed, schedule in self.state.exec_message_schedules.items()
                    },
                    "exec_message_missing_source_skip_count": self.state.exec_message_missing_source_skip_count,
                    "exec_message_delivery_input_semantics": dict(
                        self.context.params.get("exec_message_delivery_input_semantics", {})
                    ),
                }
                if self.state.exec_message_schedules else {}
            ),
            "exec_book_visibility_delay_sample_count": int(
                self.state.exec_book_visibility_delay_samples_ms.size
            ),
            "exec_book_visibility_delay_mean_ms": float(
                self.state.exec_book_visibility_delay_mean_ms
            ),
            "exec_book_visibility_delay_jitter_ms": float(
                self.state.exec_book_visibility_delay_jitter_ms
            ),
            "exec_book_visibility_delay_seed": int(
                self.state.exec_book_visibility_delay_seed
            ),
            "exec_book_visibility_delay_applied_count": int(
                self.state.exec_book_visibility_delay_applied_count
            ),
            "exec_book_visibility_delay_applied_avg_ms": (
                self.state.exec_book_visibility_delay_sum_ms
                / max(self.state.exec_book_visibility_delay_applied_count, 1)
            ),
            "exec_book_visibility_delay_applied_max_ms": int(
                self.state.exec_book_visibility_delay_max_ms
            ),
            "exec_depth_visibility_delay_applied_avg_ms": (
                self.state.exec_depth_visibility_delay_sum_ms
                / max(self.state.exec_book_visibility_delay_applied_count, 1)
            ),
            "exec_depth_visibility_delay_applied_max_ms": int(
                self.state.exec_depth_visibility_delay_max_ms
            ),
            "exec_trade_visibility_delay_applied_avg_ms": (
                self.state.exec_trade_visibility_delay_sum_ms
                / max(self.state.exec_book_visibility_delay_applied_count, 1)
            ),
            "exec_trade_visibility_delay_applied_max_ms": int(
                self.state.exec_trade_visibility_delay_max_ms
            ),
            "exec_book_visibility_paired_hit_count": int(
                self.state.exec_book_visibility_paired_hit_count
            ),
            "exec_book_visibility_paired_miss_count": int(
                self.state.exec_book_visibility_paired_miss_count
            ),
            "exec_book_visibility_mid_override_count": int(
                self.state.exec_book_visibility_mid_override_count
            ),
            "exec_depth_visibility_source_offset_ms": int(
                self.state.exec_depth_visibility_source_offset_ms
            ),
            "live_perf_telemetry_path": str(self.context.params.get("live_perf_telemetry_path", "")),
            "live_perf_latency_mode": str(self.context.params.get("live_perf_latency_mode", "")),
            "replay_contract_sha256": str(self.context.params.get("replay_contract_sha256", "")),
            "replay_purpose": str(self.context.params.get("replay_purpose", "exploratory")),
            "replay_initial_state_mode": str(
                self.context.params.get("replay_initial_state_mode", "fresh_start")
            ),
            "replay_promotion_eligible": bool(
                self.context.params.get("replay_promotion_eligible", False)
            ),
            "replay_evidence_scope": str(self.context.params.get("replay_evidence_scope", "")),
            "latency_sampler_version": self.state.latency_sampler_version,
            "latency_profile_id": str(self.context.params.get("latency_profile_id", "")),
            "latency_environment": self.context.params.get("latency_environment", ""),
            "latency_scenario": str(self.context.params.get("latency_scenario", "baseline")),
            "latency_seed": int(self.state.latency_seed),
            "latency_stress_enabled": bool(self.state.latency_stress_enabled),
            "latency_stress_spike_probability": float(
                self.state.latency_stress_spike_probability
            ),
            "latency_stress_spike_multiplier": float(
                self.state.latency_stress_spike_multiplier
            ),
            "sync_adjust_degrade_config_enabled": bool(
                self.context.params.get("sync_adjust_degrade_enabled", False)
            ),
            "sync_adjust_degrade_replayed": bool(
                self.state.sync_degrade_events.mode != "disabled"
            ),
            "sync_adjust_replay_mode": str(self.state.sync_degrade_events.mode),
            "sync_adjust_semantics": self.services.SYNC_DEGRADE_SEMANTICS,
            "sync_adjust_event_tape_path": str(
                self.state.sync_degrade_events.artifact_path
            ),
            "sync_adjust_event_tape_sha256": str(
                self.state.sync_degrade_events.artifact_sha256
            ),
            "sync_adjust_event_environment": str(
                self.state.sync_degrade_events.environment
            ),
            "sync_adjust_event_coverage_start_ts_ms": int(
                self.state.sync_degrade_events.coverage_start_ts_ms
            ),
            "sync_adjust_event_coverage_end_ts_ms": int(
                self.state.sync_degrade_events.coverage_end_ts_ms
            ),
            "sync_adjust_event_count": int(
                self.state.sync_degrade_events.timestamps_ms.size
            ),
            "sync_adjust_degrade_trigger_count": int(
                self.state.sync_adjust_degrade_trigger_count
            ),
            "sync_adjust_degrade_block_bid_count": int(
                self.state.sync_adjust_degrade_block_bid_count
            ),
            "sync_adjust_degrade_block_ask_count": int(
                self.state.sync_adjust_degrade_block_ask_count
            ),
            "sync_adjust_degrade_until_ms": int(self.state.sync_adjust_degrade_until_ms),
            "sync_adjust_censored": bool(self.state.sync_adjust_censored),
            "sync_adjust_censor_ts_ms": int(self.state.sync_adjust_censor_ts_ms),
            "sync_adjust_promotion_eligible": bool(
                self.state.sync_degrade_events.promotion_eligible
            ),
            "user_stream_mismatch_replayed": False,
            "exchange_order_ack_replayed": bool(
                self.state.new_order_latency_split_enabled or self.state.cancel_latency_split_enabled
            ),
            "multi_market_network_arrival_replayed": False,
            "replay_semantics_note": (
                "Python replay approximates queue, latency and pending order lifecycle. "
                "Frozen sync-degrade events are replayed after same-ms market events. "
                "The replay still does not synthesize unrecorded user-stream/REST mismatches "
                "or live multi-market network arrival/reconnect state."
            ),
            "book_source": self.state.book_source,
            "book_observation_clock_status": {"bbo": self.services.book_observation_status(self.context.bbo_data), "l2": self.services.book_observation_status(self.context.l2_data)},
            "max_sprd_bps": self.state.cap_bps,
            "cap_mode": self.state.cap_mode,
            "cap_base_bps": self.state.cap_bps,
            "cap_alpha": self.state.dynamic_cap_alpha,
            "cap_max_mult": self.state.dynamic_cap_max_mult,
            "cap_var_baseline": self.state.dynamic_cap_var_baseline,
            "cap_liq_beta": self.state.dynamic_cap_liq_beta,
            "cap_liq_baseline": self.state.dynamic_cap_liq_baseline,
            "cap_min_mult": self.state.dynamic_cap_min_mult,
            "depth_liquidity_ratio_floor": self.state.depth_liquidity_ratio_floor,
            "initial_inventory": self.state.initial_inventory,
            "initial_entry_price": self.state.initial_entry_price,
            "initial_live_state_enabled": bool(self.state.initial_live_state_enabled),
            "initial_live_state_orders_requested": self.state.initial_live_state_orders_requested,
            "initial_live_state_orders_restored": self.state.initial_live_state_orders_restored,
            "initial_live_state_bid_orders_restored": self.state.initial_live_state_bid_orders_restored,
            "initial_live_state_ask_orders_restored": self.state.initial_live_state_ask_orders_restored,
            "initial_live_state_warning": self.state.initial_live_state_warning,
            **({
                "initial_live_state_queue_estimated_count": self.state.initial_live_state_queue_estimated_count,
                "initial_live_state_queue_scope": "inherited_queue_history_unknown_diagnostic_only",
                "initial_live_state_last_requote_ts_ms": self.state.restored_last_requote_ts,
                "initial_live_state_requote_clock_scope": (
                    "caller_supplied_recorded_local_clock"
                    if self.state.restored_last_requote_ts is not None else "not_restored"
                ),
            } if self.state.initial_live_state_enabled else {}),
            "cap_label": self.state.cap_label,
            "dynamic_cap_enabled": self.state.dynamic_cap_enabled,
            "eta": self.state.eta,
            "kappa_ratio": self.state.kappa_ratio,
            "vol_blend": self.state.vol_blend,
            "skew": self.state.skew_strength,
            "asym": self.state.asym_strength,
            "gdir": self.state.inventory_direction_alignment_strength,
            "ret_skew": self.state.ret_skew,
            "inventory_asym_strength": self.state.inventory_asym_strength,
            "inventory_signal_fade_strength": self.state.inventory_signal_fade_strength,
            "book_imb_strength": self.state.book_imb_strength,
            "book_imb_source": self.state.book_imb_source,
            "book_imb_levels": self.state.book_imb_levels,
            "vol_power": self.state.vol_power,
            "rq_thresh_bps": self.state.requote_threshold_bps,
            "requote_clock": self.state.requote_clock,
            "use_bar_pricing": self.state.use_bar_pricing,
            "adverse_guard_enabled": self.state.adverse_guard_enabled,
            "adverse_toxicity_threshold": self.state.adverse_toxicity_threshold,
            "adverse_markout_threshold": self.state.adverse_markout_threshold,
            "adverse_markout_pause_threshold": self.state.adverse_markout_pause_threshold,
            "adverse_markout_pause_hybrid": self.state.adverse_markout_pause_hybrid,
            "adverse_markout_pause_base_s": self.state.adverse_markout_pause_base_s,
            "adverse_markout_pause_min_s": self.state.adverse_markout_pause_min_s,
            "adverse_markout_pause_max_s": self.state.adverse_markout_pause_max_s,
            "adverse_markout_decay_tau_s": self.state.adverse_markout_decay_tau_s,
            "markout_horizon_s": self.state.markout_horizon_s,
            "adverse_markout_max_resolve_gap_s": self.state.adverse_markout_max_resolve_gap_s,
            "markout_semantics": {**self.services.markout_semantics(
                consumer=("message_schedule" if self.state.exec_message_schedules else self.state.exec_book_visibility_mode),
                minimum_age_s=self.state.markout_horizon_ms / 1000.0,
                configured_max_age_s=float(self.context.params.get("adverse_markout_max_resolve_gap_s", 30.0)),
                effective_max_age_s=self.state.adverse_markout_max_resolve_gap_ms / 1000.0,
                max_age_applied=not bool(self.state.exec_message_schedules),
                price_selector="visible_depth_mid_or_current_replay_mid_fallback",
                clock_policy="modeled_exchange_fill_to_original_replay_processing_opportunity"),
                "configured_minimum_fill_age_s": float(self.context.params.get("markout_horizon_s", 10.0)),
                "ema_span_fills_effective": self.state.markout_ema_span_fills,
                "ema_alpha_effective": self.state.mo_alpha,
                "pause_threshold_effective": self.state.adverse_markout_pause_threshold_eff,
                "feedback_enabled": self.state.markout_ema_span_fills > 0,
            },
            "diagnostic_markout_semantics": {
                "purpose": "offline_diagnostic", "price_selector": "first_replay_price_at_or_after_target",
                "price_source": "replay_trade_price_array_may_include_forward_filled_clock_rows",
                "exact_target_time_query": False, "price_age_known": False,
                "value_unit": "quote_currency_per_base_unit", "not_online_feedback": True,
            },
            "adverse_dir_threshold": self.state.adverse_dir_threshold,
            "adverse_ret_bps_threshold": self.state.adverse_ret_bps_threshold,
            "adverse_weighted_mid_proxy_shift_bps": self.state.adverse_weighted_mid_proxy_shift_bps,
            "adverse_spread_mult": self.state.adverse_spread_mult,
            "thin_depth_threshold": self.state.thin_depth_threshold,
            "adverse_thin_depth_threshold": self.state.adverse_thin_depth_threshold,
            "adverse_thin_depth_mult": self.state.adverse_thin_depth_mult,
            "adverse_pause": self.state.adverse_pause,
            "local_extreme_guard_enabled": self.state.local_extreme_guard_enabled,
            "local_extreme_window_s": self.state.local_extreme_window_ms / 1000.0,
            "local_extreme_rank_threshold": self.state.local_extreme_rank_threshold,
            "local_extreme_require_thin_depth": self.state.local_extreme_require_thin_depth,
            "local_extreme_thin_depth_threshold": self.state.local_extreme_thin_depth_threshold,
            "local_extreme_spread_mult": self.state.local_extreme_spread_mult,
            "local_extreme_pause": self.state.local_extreme_pause,
            "fragile_order_ttl_s": self.state.fragile_order_ttl_ms / 1000.0,
            "adaptive_add_cooldown_enabled": self.state.adaptive_add_cooldown_cfg.enabled,
            "adaptive_add_cooldown_min_mult": self.state.adaptive_add_cooldown_cfg.min_mult,
            "adaptive_add_cooldown_max_mult": self.state.adaptive_add_cooldown_cfg.max_mult,
            "adaptive_add_cooldown_w_markout": self.state.adaptive_add_cooldown_cfg.w_markout,
            "adaptive_add_cooldown_w_flow": self.state.adaptive_add_cooldown_cfg.w_flow,
            "adaptive_add_cooldown_w_inventory_lifecycle": self.state.adaptive_add_cooldown_cfg.w_inventory_lifecycle,
            "adaptive_add_cooldown_w_trend": self.state.adaptive_add_cooldown_cfg.w_trend,
            "adaptive_add_cooldown_w_refill_weak": self.state.adaptive_add_cooldown_cfg.w_refill_weak,
            "adaptive_add_cooldown_w_refill_good": self.state.adaptive_add_cooldown_cfg.w_refill_good,
            "adaptive_add_cooldown_w_reversion": self.state.adaptive_add_cooldown_cfg.w_reversion,
            "adaptive_add_cooldown_gate_enabled": self.state.adaptive_add_cooldown_cfg.gate_enabled,
            "adaptive_add_cooldown_gate_mult": self.state.adaptive_add_cooldown_cfg.gate_mult,
            "adaptive_add_cooldown_gate_inventory_lifecycle_score": self.state.adaptive_add_cooldown_cfg.gate_inventory_lifecycle_score,
            "adaptive_add_cooldown_gate_trend_score": self.state.adaptive_add_cooldown_cfg.gate_trend_score,
            "adaptive_add_cooldown_gate_refill_edge_max": self.state.adaptive_add_cooldown_cfg.gate_refill_edge_max,
            "adaptive_add_cooldown_gate_reversion_max": self.state.adaptive_add_cooldown_cfg.gate_reversion_max,
            "adaptive_add_cooldown_gate_side": self.state.adaptive_add_cooldown_cfg.gate_side,
            "adaptive_add_cooldown_hit_count": self.state.adaptive_add_cooldown_hit_count,
            "adaptive_add_cooldown_bid_hit_count": self.state.adaptive_add_cooldown_bid_hit_count,
            "adaptive_add_cooldown_ask_hit_count": self.state.adaptive_add_cooldown_ask_hit_count,
            "defense_guard_enabled": self.state.defense_guard_enabled,
            "defense_markout_threshold": self.state.defense_markout_threshold,
            "defense_dir_threshold": self.state.defense_dir_threshold,
            "defense_ret_bps_threshold": self.state.defense_ret_bps_threshold,
            "defense_weighted_mid_proxy_shift_bps": self.state.defense_weighted_mid_proxy_shift_bps,
            "defense_spread_mult": self.state.defense_spread_mult,
            "defense_pause": self.state.defense_pause,
            "defense_emergency_inventory_ratio": self.state.defense_emergency_inventory_ratio,
            "defense_emergency_loss": self.state.defense_emergency_loss,
            "flat_unilateral_max_s": self.state.flat_unilateral_max_s,
            "inventory_lifecycle_stop_add_enabled": self.state.inventory_lifecycle_stop_add_enabled,
            "inventory_lifecycle_stop_add_inv_threshold": self.state.inventory_lifecycle_stop_add_inv_threshold,
            "inventory_lifecycle_stop_add_age_s": self.state.inventory_lifecycle_stop_add_age_s,
            "cooldown_duration_parent_stop_ts_ms": int(
                self.state.cooldown_duration_parent_stop_ts_ms
            ),
            "cooldown_duration_parent_stop_triggered": bool(
                self.state.cooldown_duration_parent_stop_triggered
            ),
            "cooldown_duration_parent_stop_trigger_ts_ms": int(
                self.state.cooldown_duration_parent_stop_trigger_ts_ms
            ),
            "inventory_lifecycle_soft_control_enabled": self.state.inventory_lifecycle_soft_control_enabled,
            "inventory_lifecycle_soft_inv_threshold": self.state.inventory_lifecycle_soft_inv_threshold,
            "inventory_lifecycle_soft_age_s": self.state.inventory_lifecycle_soft_age_s,
            "inventory_lifecycle_soft_spread_mult": self.state.inventory_lifecycle_soft_spread_mult,
            "inventory_lifecycle_soft_gate_enabled": self.state.inventory_lifecycle_soft_gate_cfg.enabled,
            "inventory_lifecycle_soft_gate_inventory_lifecycle_inv_ref": self.state.inventory_lifecycle_soft_gate_cfg.inventory_lifecycle_inv_ref,
            "inventory_lifecycle_soft_gate_inventory_lifecycle_age_ref_s": self.state.inventory_lifecycle_soft_gate_cfg.inventory_lifecycle_age_ref_s,
            "inventory_lifecycle_soft_gate_trend_ret_ref": self.state.inventory_lifecycle_soft_gate_cfg.trend_ret_ref,
            "inventory_lifecycle_soft_gate_refill_ref": self.state.inventory_lifecycle_soft_gate_cfg.refill_ref,
            "inventory_lifecycle_soft_gate_inventory_lifecycle_score": self.state.inventory_lifecycle_soft_gate_cfg.gate_inventory_lifecycle_score,
            "inventory_lifecycle_soft_gate_trend_score": self.state.inventory_lifecycle_soft_gate_cfg.gate_trend_score,
            "inventory_lifecycle_soft_gate_refill_edge_max": self.state.inventory_lifecycle_soft_gate_cfg.gate_refill_edge_max,
            "inventory_lifecycle_soft_gate_reversion_max": self.state.inventory_lifecycle_soft_gate_cfg.gate_reversion_max,
            "inventory_lifecycle_soft_gate_side": self.state.inventory_lifecycle_soft_gate_cfg.gate_side,
            "post_fill_quote_response_enabled": self.state.post_fill_quote_response_cfg.enabled,
            "post_fill_quote_response_mode": self.state.post_fill_quote_response_cfg.mode,
            "post_fill_inventory_ticks_per_order_unit": (
                self.state.post_fill_quote_response_cfg.inventory_ticks_per_order_unit
            ),
            "post_fill_inventory_max_ticks": (
                self.state.post_fill_quote_response_cfg.inventory_max_ticks
            ),
            "post_fill_flow_ticks_per_excitation": (
                self.state.post_fill_quote_response_cfg.flow_ticks_per_excitation
            ),
            "post_fill_flow_max_ticks": self.state.post_fill_quote_response_cfg.flow_max_ticks,
            "post_fill_response_half_life_s": (
                self.state.post_fill_quote_response_cfg.response_half_life_s
            ),
            "post_fill_response_half_life_min_s": (
                self.state.post_fill_quote_response_cfg.response_half_life_min_s
            ),
            "post_fill_response_half_life_max_s": (
                self.state.post_fill_quote_response_cfg.response_half_life_max_s
            ),
            "post_fill_response_volatility_weight": (
                self.state.post_fill_quote_response_cfg.volatility_weight
            ),
            "post_fill_response_refill_weight": (
                self.state.post_fill_quote_response_cfg.refill_weight
            ),
            "post_fill_response_repair_probability_weight": (
                self.state.post_fill_quote_response_cfg.repair_probability_weight
            ),
            "multi_market_policy_enabled": self.state.multi_market_policy_cfg.enabled,
            "multi_market_policy_mode": self.state.multi_market_policy_cfg.mode,
            "multi_market_policy_horizon_ms": self.state.multi_market_policy_cfg.horizon_ms,
            "multi_market_policy_min_abs_inventory": self.state.multi_market_policy_cfg.min_abs_inventory,
            "multi_market_policy_min_inventory_lifecycle_age_s": self.state.multi_market_policy_cfg.min_inventory_lifecycle_age_s,
            "multi_market_policy_min_inventory_lifecycle_fills": self.state.multi_market_policy_cfg.min_inventory_lifecycle_fills,
            "multi_market_policy_min_venue_agreement": self.state.multi_market_policy_cfg.min_venue_agreement,
            "multi_market_policy_min_external_move_bps": self.state.multi_market_policy_cfg.min_external_move_bps,
            "multi_market_policy_min_global_flow_pressure": self.state.multi_market_policy_cfg.min_global_flow_pressure,
            "multi_market_policy_min_bridge_move_bps": self.state.multi_market_policy_cfg.min_bridge_move_bps,
            "multi_market_policy_max_repair_probability": self.state.multi_market_policy_cfg.max_repair_probability,
            "multi_market_policy_min_repair_probability_drop": self.state.multi_market_policy_cfg.min_repair_probability_drop,
            "multi_market_reference_replay_enabled": (
                self.state.reference_scheduler is not None or self.state.historical_global_flow_cursor is not None
            ),
            "multi_market_reference_replay_source": (
                "receive_time_event_tapes"
                if self.state.reference_scheduler is not None
                else str(getattr(self.context.historical_global_flow_data, "source", ""))
                if self.state.historical_global_flow_cursor is not None
                else ""
            ),
            "inventory_lifecycle_repair_model_enabled": self.context.inventory_lifecycle_repair_model is not None,
            "inventory_lifecycle_repair_model_id": str(
                getattr(self.context.inventory_lifecycle_repair_model, "model_id", "")
            ),
            "inventory_lifecycle_repair_model_training_end_day": str(
                getattr(self.context.inventory_lifecycle_repair_model, "training_end_day", "")
            ),
            "inventory_lifecycle_repair_trace_count": len(self.state.trace_inventory_lifecycle_repair or []),
            "multi_market_reference_events_consumed": (
                self.state.reference_scheduler_stats.consumed_events
                if self.state.reference_scheduler_stats is not None
                else 0
            ),
            "multi_market_reference_events_accepted": (
                self.state.reference_scheduler_stats.accepted_events
                if self.state.reference_scheduler_stats is not None
                else 0
            ),
            "multi_market_reference_events_rejected": (
                self.state.reference_scheduler_stats.rejected_events
                if self.state.reference_scheduler_stats is not None
                else 0
            ),
            "multi_market_reference_gap_events": (
                self.state.reference_scheduler_stats.gap_events
                if self.state.reference_scheduler_stats is not None
                else 0
            ),
            "buy_fill_selection_live_enabled": bool(self.state.buy_fill_selection_live_enabled),
            "buy_fill_selection_live_replayed": bool(self.state.buy_fill_selection_live_enabled),
            "buy_fill_selection_live_score_threshold": self.state.buy_fill_selection_live_score_threshold,
            "buy_fill_selection_live_spread_mult_cap": self.state.buy_fill_selection_live_spread_mult_cap,
            "buy_fill_selection_live_apply_reducing": bool(self.state.buy_fill_selection_live_apply_reducing),
            "buy_fill_selection_live_eval_count": self.state.buy_fill_selection_eval_count,
            "buy_fill_selection_live_hit_count": self.state.buy_fill_selection_hit_count,
            "buy_fill_selection_live_hit_rate": (
                self.state.buy_fill_selection_hit_count / self.state.buy_fill_selection_eval_count
                if self.state.buy_fill_selection_eval_count > 0 else 0.0
            ),
            "buy_fill_selection_live_score_mean": (
                self.state.buy_fill_selection_score_sum / self.state.buy_fill_selection_eval_count
                if self.state.buy_fill_selection_eval_count > 0 else 0.0
            ),
            "buy_fill_selection_live_score_max": self.state.buy_fill_selection_score_max,
            "buy_fill_selection_live_score_p10": (
                float(self.services.np.quantile(self.state.buy_fill_selection_scores, 0.10))
                if self.state.buy_fill_selection_scores else 0.0
            ),
            "buy_fill_selection_live_score_median": (
                float(self.services.np.quantile(self.state.buy_fill_selection_scores, 0.50))
                if self.state.buy_fill_selection_scores else 0.0
            ),
            "buy_fill_selection_live_score_p90": (
                float(self.services.np.quantile(self.state.buy_fill_selection_scores, 0.90))
                if self.state.buy_fill_selection_scores else 0.0
            ),
            "buy_fill_selection_live_score_ge_042": self.state.buy_fill_selection_score_ge_042,
            "buy_fill_selection_live_score_ge_043": self.state.buy_fill_selection_score_ge_043,
            "buy_fill_selection_live_score_ge_044": self.state.buy_fill_selection_score_ge_044,
            "buy_fill_selection_live_score_ge_045": self.state.buy_fill_selection_score_ge_045,
            "selection_score": 0.0,
            "pnl": self.state.final_pnl,
            "cash_before_terminal": self.state.cash,
            "terminal_mark_price": self.state.last_px,
            "terminal_mtm_pnl": self.state.mtm_before_terminal_fee,
            "mtm_before_terminal_fee": self.state.mtm_before_terminal_fee,
            "terminal_fee_drag": self.state.terminal_fee_drag,
            "terminal_liquidation_fee_estimate": self.state.terminal_liquidation_fee_estimate,
            "terminal_liquidation_applied": False,
            "maker_fee_rate": self.state.maker_fee,
            "inventory_pnl": self.state.inventory_pnl,
            "inventory_cost": self.state.inventory_cost,
            "inventory_adjusted_pnl": self.state.inventory_adjusted_pnl,
            "signed_inventory_time_s": self.state.signed_inventory_time_s,
            "abs_inventory_time_s": self.state.abs_inventory_time_s,
            "sq_inventory_time_s": self.state.sq_inventory_time_s,
            "signed_notional_inventory_time_s": self.state.signed_notional_inventory_time_s,
            "notional_inventory_time_s": self.state.notional_inventory_time_s,
            "time_avg_abs_inventory": self.state.time_avg_abs_inventory,
            "time_avg_signed_inventory": self.state.time_avg_signed_inventory,
            "time_avg_sq_inventory": self.state.time_avg_sq_inventory,
            "time_avg_notional_inventory": self.state.time_avg_notional_inventory,
            "pnl_per_abs_inventory_hour": self.state.pnl_per_abs_inventory_hour,
            "inv_adj_pnl_per_abs_inventory_hour": self.state.inv_adj_pnl_per_abs_inventory_hour,
            "inventory_lifecycle_count": self.state.inventory_lifecycle_count,
            "inventory_lifecycle_accounting_semantics": self.services.INVENTORY_LIFECYCLE_ACCOUNTING_SEMANTICS,
            "inventory_lifecycle_closed_count": self.state.inventory_lifecycle_closed_count,
            "inventory_lifecycle_open_count": self.state.inventory_lifecycle_open_count,
            "inventory_lifecycle_max_abs_inventory": self.state.inventory_lifecycle_max_abs_inventory,
            "inventory_lifecycle_max_duration_s": self.state.inventory_lifecycle_max_duration_s,
            "inventory_lifecycle_max_adverse_excursion": self.state.inventory_lifecycle_max_adverse_excursion,
            "inventory_lifecycle_exposure_increasing_fills": self.state.inventory_lifecycle_run_exposure_increasing_fills,
            "inventory_lifecycle_reducing_fills": self.state.inventory_lifecycle_run_reducing_fills,
            "inventory_lifecycle_buy_fills": self.state.inventory_lifecycle_run_buy_fills,
            "inventory_lifecycle_sell_fills": self.state.inventory_lifecycle_run_sell_fills,
            "inventory_lifecycle_shadow_inv_006_blocks": self.state.inventory_lifecycle_shadow_inv_006_blocks,
            "inventory_lifecycle_shadow_inv_008_blocks": self.state.inventory_lifecycle_shadow_inv_008_blocks,
            "inventory_lifecycle_shadow_inv_010_blocks": self.state.inventory_lifecycle_shadow_inv_010_blocks,
            "inventory_lifecycle_shadow_age_20m_blocks": self.state.inventory_lifecycle_shadow_age_20m_blocks,
            "inventory_lifecycle_shadow_age_40m_blocks": self.state.inventory_lifecycle_shadow_age_40m_blocks,
            "inventory_lifecycle_shadow_age_60m_blocks": self.state.inventory_lifecycle_shadow_age_60m_blocks,
            "inventory_lifecycle_shadow_reducing_only_blocks": self.state.inventory_lifecycle_shadow_reducing_only_blocks,
            "inventory_lifecycle_stop_add_count": self.state.inventory_lifecycle_stop_add_count,
            "bid_inventory_lifecycle_stop_add_count": self.state.bid_inventory_lifecycle_stop_add_count,
            "ask_inventory_lifecycle_stop_add_count": self.state.ask_inventory_lifecycle_stop_add_count,
            "inventory_lifecycle_soft_control_count": self.state.inventory_lifecycle_soft_control_count,
            "bid_inventory_lifecycle_soft_control_count": self.state.bid_inventory_lifecycle_soft_control_count,
            "ask_inventory_lifecycle_soft_control_count": self.state.ask_inventory_lifecycle_soft_control_count,
            "post_fill_quote_response_eval_count": self.state.post_fill_quote_response_eval_count,
            "post_fill_quote_response_active_count": self.state.post_fill_quote_response_active_count,
            "post_fill_quote_response_inventory_count": (
                self.state.post_fill_quote_response_inventory_count
            ),
            "post_fill_quote_response_flow_count": self.state.post_fill_quote_response_flow_count,
            "post_fill_quote_response_cap_limited_count": (
                self.state.post_fill_quote_response_cap_limited_count
            ),
            "post_fill_quote_response_post_only_clamp_count": (
                self.state.post_fill_quote_response_post_only_clamp_count
            ),
            "post_fill_quote_response_add_fill_count": (
                self.state.post_fill_quote_response_add_fill_count
            ),
            "post_fill_quote_response_bid_add_count": (
                self.state.post_fill_quote_response_bid_add_count
            ),
            "post_fill_quote_response_ask_add_count": (
                self.state.post_fill_quote_response_ask_add_count
            ),
            "post_fill_quote_response_avg_inventory_ticks": (
                self.state.post_fill_quote_response_inventory_ticks_sum
                / max(self.state.post_fill_quote_response_eval_count, 1)
            ),
            "post_fill_quote_response_avg_add_ticks": (
                self.state.post_fill_quote_response_add_ticks_sum
                / max(self.state.post_fill_quote_response_eval_count, 1)
            ),
            "post_fill_quote_response_max_add_ticks": (
                self.state.post_fill_quote_response_add_ticks_max
            ),
            "post_fill_quote_response_avg_half_life_s": (
                self.state.post_fill_quote_response_half_life_sum
                / max(self.state.post_fill_quote_response_eval_count, 1)
            ),
            "multi_market_policy_eval_count": self.state.multi_market_policy_eval_count,
            "multi_market_policy_hit_count": self.state.multi_market_policy_hit_count,
            "multi_market_policy_effective_block_count": self.state.multi_market_policy_effective_block_count,
            "bid_multi_market_policy_hit_count": self.state.bid_multi_market_policy_hit_count,
            "ask_multi_market_policy_hit_count": self.state.ask_multi_market_policy_hit_count,
            "bid_multi_market_policy_effective_block_count": (
                self.state.bid_multi_market_policy_effective_block_count
            ),
            "ask_multi_market_policy_effective_block_count": (
                self.state.ask_multi_market_policy_effective_block_count
            ),
            "pnl_per_day": self.state.final_pnl / max(self.state.n_days, 0.01),
            "sharpe": self.state.sharpe,
            "max_drawdown": self.state.max_dd,
            "avg_markout": self.state.avg_markout,
            "avg_markout_bid": self.state.avg_markout_bid,
            "avg_markout_ask": self.state.avg_markout_ask,
            "markout_count": self.state.mo_count_all,
            "markout_qty_btc": self.state.mo_qty_all,
            "markout_qty_bid_btc": self.state.mo_qty_bid,
            "markout_qty_ask_btc": self.state.mo_qty_ask,
            "fills_bid": self.state.nfb,
            "fills_ask": self.state.nfa,
            "fills_total": self.state.nf,
            "buy_fill_qty": self.state.buy_fill_qty_total,
            "sell_fill_qty": self.state.sell_fill_qty_total,
            "buy_fill_notional": self.state.buy_fill_notional,
            "sell_fill_notional": self.state.sell_fill_notional,
            "buy_avg_fill_price": self.state.buy_fill_notional / self.state.buy_fill_qty_total if self.state.buy_fill_qty_total > 1e-12 else 0.0,
            "sell_avg_fill_price": self.state.sell_fill_notional / self.state.sell_fill_qty_total if self.state.sell_fill_qty_total > 1e-12 else 0.0,
            "fills_per_day": self.state.nf / max(self.state.n_days, 0.01),
            "fill_rate": self.state.nf / max(self.state.nrq, 1),
            "quote_attempts": self.state.quote_attempts,
            "quote_attempts_per_day": self.state.quote_attempts_per_day,
            "requotes_per_day": self.state.requotes_per_day,
            "avg_spread": self.state.tsprd / max(self.state.nrq, 1),
            "avg_final_spread": self.state.tsprd_final / max(self.state.n_final_spread, 1),
            "n_final_spread": self.state.n_final_spread,
            "quote_spread_lt_100_rate": self.state.quote_spread_lt_100_count / max(self.state.nrq, 1),
            "quote_spread_lt_150_rate": self.state.quote_spread_lt_150_count / max(self.state.nrq, 1),
            "final_spread_lt_100_rate": self.state.final_spread_lt_100_count / max(self.state.n_final_spread, 1),
            "final_spread_lt_150_rate": self.state.final_spread_lt_150_count / max(self.state.n_final_spread, 1),
            "post_only_guard_hits": self.state.post_only_guard_hits,
            "post_only_bid_hits": self.state.post_only_bid_hits,
            "post_only_ask_hits": self.state.post_only_ask_hits,
            "adverse_guard_count": self.state.adverse_guard_count,
            "adverse_pause_count": self.state.adverse_pause_count,
            "adverse_guard_rate": self.state.adverse_guard_count / max(self.state.nrq * 2, 1),
            "adverse_pause_rate": self.state.adverse_pause_count / max(self.state.nrq * 2, 1),
            "adverse_markout_stale_drop_count": self.state.mo_stale_drop_count,
            "bid_adverse_markout_pause_extend_count": self.state.mo_pause_extend_bid_count,
            "ask_adverse_markout_pause_extend_count": self.state.mo_pause_extend_ask_count,
            "mid_guard_bid_hits": self.state.mid_guard_bid_hits,
            "mid_guard_ask_hits": self.state.mid_guard_ask_hits,
            "gtx_rejects": self.state.gtx_rejects,
            "fills_while_pending_cancel": self.state.fills_while_pending_cancel,
            "fills_bid_final_compressed": self.state.fills_bid_final_compressed,
            "fills_ask_final_compressed": self.state.fills_ask_final_compressed,
            "fills_final_compressed": self.state.fills_bid_final_compressed + self.state.fills_ask_final_compressed,
            "fills_bid_not_final_compressed": self.state.fills_bid_not_final_compressed,
            "fills_ask_not_final_compressed": self.state.fills_ask_not_final_compressed,
            "fills_not_final_compressed": self.state.fills_bid_not_final_compressed + self.state.fills_ask_not_final_compressed,
            "fills_bid_mid_guard": self.state.fills_bid_mid_guard,
            "fills_ask_mid_guard": self.state.fills_ask_mid_guard,
            "fills_bid_post_only": self.state.fills_bid_post_only,
            "fills_ask_post_only": self.state.fills_ask_post_only,
            "bid_adverse_guard_count": self.state.bid_adverse_guard_count,
            "bid_adverse_toxicity_count": self.state.bid_adverse_toxicity_count,
            "bid_adverse_markout_count": self.state.bid_adverse_markout_count,
            "bid_adverse_direction_count": self.state.bid_adverse_direction_count,
            "bid_adverse_ret_count": self.state.bid_adverse_ret_count,
            "bid_adverse_weighted_mid_proxy_count": self.state.bid_adverse_weighted_mid_proxy_count,
            "bid_adverse_thin_depth_count": self.state.bid_adverse_thin_depth_count,
            "bid_adverse_pause_count": self.state.bid_adverse_pause_count,
            "bid_adverse_guard_rate": self.state.bid_adverse_guard_count / max(self.state.nrq, 1),
            "fills_bid_adverse_guard": self.state.fills_bid_adverse_guard,
            "ask_adverse_guard_count": self.state.ask_adverse_guard_count,
            "ask_adverse_toxicity_count": self.state.ask_adverse_toxicity_count,
            "ask_adverse_markout_count": self.state.ask_adverse_markout_count,
            "ask_adverse_direction_count": self.state.ask_adverse_direction_count,
            "ask_adverse_ret_count": self.state.ask_adverse_ret_count,
            "ask_adverse_weighted_mid_proxy_count": self.state.ask_adverse_weighted_mid_proxy_count,
            "ask_adverse_thin_depth_count": self.state.ask_adverse_thin_depth_count,
            "ask_adverse_pause_count": self.state.ask_adverse_pause_count,
            "ask_adverse_guard_rate": self.state.ask_adverse_guard_count / max(self.state.nrq, 1),
            "fills_ask_adverse_guard": self.state.fills_ask_adverse_guard,
            "defense_guard_count": self.state.defense_guard_count,
            "defense_pause_count": self.state.defense_pause_count,
            "defense_guard_rate": self.state.defense_guard_count / max(self.state.nrq * 2, 1),
            "defense_pause_rate": self.state.defense_pause_count / max(self.state.nrq * 2, 1),
            "bid_defense_guard_count": self.state.bid_defense_guard_count,
            "bid_defense_pause_count": self.state.bid_defense_pause_count,
            "bid_defense_guard_rate": self.state.bid_defense_guard_count / max(self.state.nrq, 1),
            "fills_bid_defense_guard": self.state.fills_bid_defense_guard,
            "ask_defense_guard_count": self.state.ask_defense_guard_count,
            "ask_defense_pause_count": self.state.ask_defense_pause_count,
            "ask_defense_guard_rate": self.state.ask_defense_guard_count / max(self.state.nrq, 1),
            "fills_ask_defense_guard": self.state.fills_ask_defense_guard,
            "local_extreme_guard_count": self.state.local_extreme_guard_count,
            "local_extreme_pause_count": self.state.local_extreme_pause_count,
            "local_extreme_guard_rate": self.state.local_extreme_guard_count / max(self.state.nrq * 2, 1),
            "bid_local_extreme_guard_count": self.state.bid_local_extreme_guard_count,
            "ask_local_extreme_guard_count": self.state.ask_local_extreme_guard_count,
            "fills_bid_local_extreme_guard": self.state.fills_bid_local_extreme_guard,
            "fills_ask_local_extreme_guard": self.state.fills_ask_local_extreme_guard,
            "fragile_ttl_cancel_count": self.state.fragile_ttl_cancel_count,
            "flat_unilateral_release_count": self.state.flat_unilateral_release_count,
            "flat_unilateral_bid_release_count": self.state.flat_unilateral_bid_release_count,
            "flat_unilateral_ask_release_count": self.state.flat_unilateral_ask_release_count,
            "stale_book_skip_count": self.state.stale_book_skip_count,
            "stale_book_skip_rate": self.state.stale_book_skip_rate,
            "stale_book_cancel_count": self.state.stale_book_cancel_count,
            "avg_stale_book_age_ms": self.state.avg_stale_book_age_ms,
            "max_stale_book_age_ms": self.state.stale_book_age_max,
            "max_pending_new_orders": self.state.max_pending_new_orders,
            "max_pending_cancel_orders": self.state.max_pending_cancel_orders,
            "cap_hit_rate": self.state.cap_hit_rate,
            "delta_cap_hit_rate": self.state.delta_cap_hit_rate,
            "final_cap_compress_rate": self.state.final_cap_compress_rate,
            "final_cap_rounding_rate": self.state.final_cap_rounding_rate,
            "final_cap_mid_guard_rate": self.state.final_cap_mid_guard_rate,
            "final_cap_post_only_rate": self.state.final_cap_post_only_rate,
            "final_cap_delta_rate": self.state.final_cap_delta_rate,
            "spread_cap_mode": self.state.spread_cap_action_name,
            "cap_exposure_block_count": self.state.cap_exposure_block_count,
            "bid_cap_exposure_block_count": self.state.bid_cap_exposure_block_count,
            "ask_cap_exposure_block_count": self.state.ask_cap_exposure_block_count,
            "cap_exposure_block_rate": self.state.cap_exposure_block_count / max(self.state.nrq * 2, 1),
            "avg_final_cap_excess": self.state.avg_final_cap_excess,
            "max_final_cap_excess": self.state.final_cap_excess_max,
            "post_policy_cap_count": self.state.post_policy_cap_count,
            "post_policy_cap_rate": self.state.post_policy_cap_rate,
            "avg_post_policy_cap_excess": self.state.avg_post_policy_cap_excess,
            "max_post_policy_cap_excess": self.state.post_policy_cap_excess_max,
            "mid_guard_bid_rate": self.state.mid_guard_bid_rate,
            "mid_guard_ask_rate": self.state.mid_guard_ask_rate,
            "post_only_bid_rate": self.state.post_only_bid_rate,
            "post_only_ask_rate": self.state.post_only_ask_rate,
            "avg_delta_pre_cap": self.state.avg_delta_pre_cap,
            "avg_delta_after_cap": self.state.avg_delta_after_cap,
            "avg_cap_bps": self.state.avg_cap_bps,
            "avg_bid_dist": self.state.avg_bid_dist,
            "avg_ask_dist": self.state.avg_ask_dist,
            "avg_quote_skew_pct": self.state.avg_quote_skew_pct,
            "avg_abs_quote_skew_pct": self.state.avg_abs_quote_skew_pct,
            "quote_abs_skew_gt_10_rate": self.state.quote_abs_skew_gt_10_rate,
            "quote_abs_skew_gt_25_rate": self.state.quote_abs_skew_gt_25_rate,
            "bid_tighter_rate": self.state.bid_tighter_rate,
            "ask_tighter_rate": self.state.ask_tighter_rate,
            "quote_balanced_rate": self.state.quote_balanced_rate,
            "avg_markout_final_compressed": self.state.avg_markout_final_compressed,
            "avg_markout_not_final_compressed": self.state.avg_markout_not_final_compressed,
            "markout_count_final_compressed": self.state.mo_count_final_compressed,
            "markout_count_not_final_compressed": self.state.mo_count_not_final_compressed,
            "markout_qty_final_compressed_btc": self.state.mo_qty_final_compressed,
            "markout_qty_not_final_compressed_btc": self.state.mo_qty_not_final_compressed,
            "max_inventory": self.state.mx_inv,
            "avg_inventory": self.state.abs_inventory_time_s / max(self.state.elapsed_s, 1e-9),
            "final_inventory": self.state.q,
            "n_requotes": self.state.nrq,
            "ber_active_count": int(self.state.ber_active_count),
            "ber_active_rate": self.state.ber_active_count / max(self.state.nrq, 1),
            "ber_held_input_end": float(self.state.ber_held_ti),
            "ber_ema_fast_end": float(self.state.ema_ti_fast),
            "ber_ema_slow_end": float(self.state.ema_ti_slow),
            "ber_active_end": bool(self.state.trade_intensity_acceleration_guard_active),
            "ber_feature_publish_count": int(self.state.ber_feature_publish_count),
            **({
                "quote_trade_intensity_end": float(self.state.cur_ti),
                "quote_trade_intensity_publish_count": int(self.state.quote_ti_publish_count),
                "quote_trade_intensity_completed_ts_ms": int(self.state.quote_ti_completed_ts_ms),
                "quote_trade_intensity_unit": "mean_aggregate_trades_per_complete_10s_bar",
                "quote_trade_intensity_clock": "capture_visible_completed_10s_feature",
            } if self.context.var_ti is not None else {}),
            "ber_role_safe_decision_count": int(self.state.ber_role_safe_decision_count),
            "ber_role_safe_buy_add_count": int(self.state.ber_role_safe_buy_add_count),
            "ber_role_safe_sell_add_count": int(self.state.ber_role_safe_sell_add_count),
            "ber_role_safe_flat_bypass_count": int(
                self.state.ber_role_safe_flat_bypass_count
            ),
            "ber_role_safe_mixed_fail_closed_count": int(
                self.state.ber_role_safe_mixed_fail_closed_count
            ),
            "ber_role_safe_pair_change_count": int(
                self.state.ber_role_safe_pair_change_count
            ),
            "ber_role_safe_bid_change_count": int(self.state.ber_role_safe_bid_change_count),
            "ber_role_safe_ask_change_count": int(self.state.ber_role_safe_ask_change_count),
            "ber_role_safe_source_mismatch_count": int(
                self.state.ber_role_safe_source_mismatch_count
            ),
            "ber_role_safe_cap_collision_count": int(
                self.state.ber_role_safe_cap_collision_count
            ),
            "ber_role_safe_cap_infeasible_count": int(
                self.state.ber_role_safe_cap_infeasible_count
            ),
            "n_timeouts": self.state.nto,
            "circuit_breaker_count": self.state.circuit_breaker_count,
            "circuit_breaker_exit_mode": self.state.circuit_breaker_exit_mode,
            "circuit_breaker_closing": bool(self.state.circuit_breaker_closing),
            "circuit_breaker_close_place_count": self.state.circuit_breaker_close_place_count,
            "circuit_breaker_close_keep_count": self.state.circuit_breaker_close_keep_count,
            "circuit_breaker_close_fill_count": self.state.circuit_breaker_close_fill_count,
            "circuit_breaker_close_gtx_reject_count": (
                self.state.circuit_breaker_close_gtx_reject_count
            ),
            "circuit_breaker_close_ioc_place_count": (
                self.state.circuit_breaker_close_ioc_place_count
            ),
            "circuit_breaker_close_ioc_fill_count": (
                self.state.circuit_breaker_close_ioc_fill_count
            ),
            "circuit_breaker_close_ioc_expire_count": (
                self.state.circuit_breaker_close_ioc_expire_count
            ),
            "n_trades": self.state.n_execution_trades,
            "n_clock_events": self.state.n_trades + self.state.input_event_offset,
            "replay_event_clock": self.state.replay_event_clock,
            "replay_clock_interval_ms": self.state.replay_clock_interval_ms,
            "n_days": self.state.n_days,
            "avg_rq_ms": self.state.rq_sum / max(self.state.nrq, 1),
            "consecutive_loss_cooldown_semantics": self.services.LOSS_COOLDOWN_SEMANTICS,
            "risk_daily_loss_block_count": self.state.risk_daily_loss_block_count,
            "risk_position_value_block_count": self.state.risk_position_value_block_count,
            "risk_emergency_close_count": self.state.risk_emergency_close_count,
            "risk_notional_cap_count": self.state.risk_notional_cap_count,
            "risk_emergency_latched": self.state.risk_emergency_latched,
            "risk_emergency_ownership_conflict_count": self.state.risk_emergency_ownership_conflict_count,
            **({
                "risk_emergency_stop_reason": "stop_reconciliation_required",
                "economic_pnl_complete": False,
                "economic_pnl_status": "incomplete_unmodeled_emergency_fatal_recovery",
            } if self.state.risk_emergency_ownership_conflict_count else {}),
            "_final_risk_state": self.services.asdict(self.state.risk_state),
            "_risk_action_trace": self.state.trace_risk_actions,
            "consecutive_loss_cooldown_state": self.state.consecutive_loss_cooldown.snapshot(),
            "max_consecutive_losses": int(
                self.state.consecutive_loss_cooldown.max_consecutive_losses
            ),
            "cooldown_after_loss_s": float(
                self.state.consecutive_loss_cooldown.cooldown_ms / 1_000.0
            ),
            "consecutive_loss_cooldown_trigger_count": int(
                self.state.consecutive_loss_cooldown.trigger_count
            ),
            "consecutive_loss_cooldown_expiry_count": int(
                self.state.consecutive_loss_cooldown.expiry_count
            ),
            "consecutive_loss_cooldown_block_count": int(
                self.state.consecutive_loss_cooldown_block_count
            ),
            "consecutive_loss_cooldown_cancel_count": int(
                self.state.consecutive_loss_cooldown_cancel_count
            ),
            "consecutive_loss_round_trip_loss_count": int(
                self.state.consecutive_loss_cooldown.losing_round_trips
            ),
            "consecutive_loss_round_trip_nonloss_count": int(
                self.state.consecutive_loss_cooldown.winning_or_flat_round_trips
            ),
            "consecutive_loss_count_end": int(
                self.state.consecutive_loss_cooldown.consecutive_losses
            ),
            "consecutive_loss_count_max": int(
                self.state.consecutive_loss_cooldown.max_observed_consecutive_losses
            ),
            "consecutive_loss_cooldown_until_ms": int(
                self.state.consecutive_loss_cooldown.cooldown_until_ms
            ),
            "dynamic_fill_hazard_action_enabled": bool(
                self.state.dynamic_fill_hazard_action_enabled
            ),
            "dynamic_fill_hazard_action_application": str(
                self.state.dynamic_fill_hazard_action_application
            ),
            "dynamic_fill_hazard_shadow_cancel_signal_count": int(
                self.state.dynamic_fill_hazard_shadow_cancel_signal_count
            ),
            "dynamic_fill_hazard_replay_authority": (
                (
                    "python_exchange_truth_feature_ready_visibility"
                    if self.state.dynamic_fill_hazard_visibility_scheduler is not None
                    else "python_native_exchange_book"
                )
                if self.state.dynamic_fill_hazard_action_enabled
                else "disabled"
            ),
            "dynamic_fill_hazard_visibility_clock_mode": str(
                self.state.dynamic_fill_hazard_visibility_clock_mode
            ),
            "dynamic_fill_hazard_visibility_profile_identity": dict(
                self.state.dynamic_fill_hazard_visibility_profile_identity
            ),
            "dynamic_fill_hazard_visibility_stats": (
                self.state.dynamic_fill_hazard_visibility_scheduler.stats_dict()
                if self.state.dynamic_fill_hazard_visibility_scheduler is not None
                else {}
            ),
            "dynamic_fill_hazard_visibility_book_stats": (
                {
                    key: value
                    for key, value in vars(
                        self.state.dynamic_fill_hazard_visibility_book_stats
                    ).items()
                }
                if self.state.dynamic_fill_hazard_visibility_book_stats is not None
                else {}
            ),
            "dynamic_fill_hazard_truth_state_fingerprint": (
                self.state.exchange_book_scheduler.state_fingerprint()
                if self.state.exchange_book_scheduler is not None
                else ""
            ),
            "dynamic_fill_hazard_visibility_state_fingerprint": (
                self.state.dynamic_fill_hazard_visibility_scheduler.state_fingerprint()
                if self.state.dynamic_fill_hazard_visibility_scheduler is not None
                else ""
            ),
            "dynamic_fill_hazard_visible_trade_enqueued_count": int(
                self.state.dynamic_fill_hazard_visible_trade_enqueued_count
            ),
            "dynamic_fill_hazard_visible_trade_delivered_count": int(
                self.state.dynamic_fill_hazard_visible_trade_delivered_count
            ),
            "dynamic_fill_hazard_visible_trade_pending_count": int(
                len(self.state.dynamic_fill_hazard_visible_trade_heap)
            ),
            "dynamic_fill_hazard_visible_book_trade_tie_count": int(
                self.state.dynamic_fill_hazard_visible_book_trade_tie_count
            ),
            "dynamic_fill_hazard_provider_receive_missing_count": int(
                self.state.dynamic_fill_hazard_provider_receive_missing_count
            ),
            "dynamic_fill_hazard_invalid_reason_counts": dict(
                sorted(self.state.dynamic_fill_hazard_invalid_reason_counts.items())
            ),
            "dynamic_fill_hazard_eval_by_role": {
                role: dict(counts)
                for role, counts in sorted(
                    self.state.dynamic_fill_hazard_eval_by_role.items()
                )
            },
            "dynamic_fill_hazard_action_by_role": {
                role: dict(counts)
                for role, counts in sorted(
                    self.state.dynamic_fill_hazard_action_by_role.items()
                )
            },
            "dynamic_fill_hazard_valid_score_quantiles": (
                {
                    name: float(value)
                    for name, value in zip(
                        ("p01", "p10", "p25", "p50", "p75", "p90", "p99"),
                        self.services.np.quantile(
                            self.services.np.asarray(
                                self.state.dynamic_fill_hazard_valid_scores,
                                dtype=self.services.np.float64,
                            ),
                            (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99),
                        ),
                        strict=True,
                    )
                }
                if self.state.dynamic_fill_hazard_valid_scores
                else {}
            ),
            "dynamic_fill_hazard_future_feature_time_count": int(
                self.state.dynamic_fill_hazard_invalid_reason_counts.get(
                    "future_feature_time",
                    0,
                )
            ),
            "dynamic_fill_hazard_invalid_attribution_closed": bool(
                self.state.dynamic_fill_hazard_invalid_eval_count
                == sum(self.state.dynamic_fill_hazard_invalid_reason_counts.values())
                and "unattributed" not in self.state.dynamic_fill_hazard_invalid_reason_counts
            ),
            "dynamic_fill_hazard_cpp_parity_enabled": bool(
                self.state.dynamic_fill_hazard_cpp_runtime is not None
            ),
            "dynamic_fill_hazard_cpp_parity_scope": (
                "native_book_and_buy_q90_parity_kernel_only"
                if self.state.dynamic_fill_hazard_cpp_runtime is not None
                else "disabled"
            ),
            "dynamic_fill_hazard_full_cpp_tick_replay_authority": False,
            "dynamic_fill_hazard_cpp_parity_passed": bool(
                self.state.dynamic_fill_hazard_cpp_runtime is not None
                and self.state.dynamic_fill_hazard_cpp_mismatch_count == 0
            ),
            "dynamic_fill_hazard_cpp_identity": dict(
                self.state.dynamic_fill_hazard_cpp_identity
            ),
            "dynamic_fill_hazard_cpp_book_event_count": int(
                self.state.dynamic_fill_hazard_cpp_book_event_count
            ),
            "dynamic_fill_hazard_cpp_activation_count": int(
                self.state.dynamic_fill_hazard_cpp_activation_count
            ),
            "dynamic_fill_hazard_cpp_evaluation_count": int(
                self.state.dynamic_fill_hazard_cpp_evaluation_count
            ),
            "dynamic_fill_hazard_cpp_lifecycle_count": int(
                self.state.dynamic_fill_hazard_cpp_lifecycle_count
            ),
            "dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count": int(
                self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count
            ),
            "dynamic_fill_hazard_cpp_mismatch_count": int(
                self.state.dynamic_fill_hazard_cpp_mismatch_count
            ),
            "dynamic_fill_hazard_cpp_mismatch_by_stage": dict(
                self.state.dynamic_fill_hazard_cpp_mismatch_by_stage
            ),
            "dynamic_fill_hazard_cpp_mismatch_trace": list(
                self.state.dynamic_fill_hazard_cpp_mismatch_trace
            ),
            "dynamic_fill_hazard_cpp_counters": dict(
                self.state.dynamic_fill_hazard_cpp_counters
            ),
            "dynamic_fill_hazard_cpp_sequence_stats": dict(
                self.state.dynamic_fill_hazard_cpp_sequence_stats
            ),
            "dynamic_fill_hazard_model_family_id": (
                self.state.dynamic_fill_hazard_bundle.family_id
                if self.state.dynamic_fill_hazard_bundle is not None
                else ""
            ),
            "dynamic_fill_hazard_model_sha256": (
                self.state.dynamic_fill_hazard_bundle.file_sha256
                if self.state.dynamic_fill_hazard_bundle is not None
                else ""
            ),
            "dynamic_fill_hazard_policy_id": (
                self.state.dynamic_fill_hazard_policy.policy_id
                if self.state.dynamic_fill_hazard_policy is not None
                else ""
            ),
            "dynamic_fill_hazard_policy_sha256": (
                self.state.dynamic_fill_hazard_policy.file_sha256
                if self.state.dynamic_fill_hazard_policy is not None
                else ""
            ),
            "dynamic_fill_hazard_eval_count": int(
                self.state.dynamic_fill_hazard_eval_count
            ),
            "dynamic_fill_hazard_valid_eval_count": int(
                self.state.dynamic_fill_hazard_valid_eval_count
            ),
            "dynamic_fill_hazard_invalid_eval_count": int(
                self.state.dynamic_fill_hazard_invalid_eval_count
            ),
            "dynamic_fill_hazard_keep_count": int(self.state.dynamic_fill_hazard_keep_count),
            "dynamic_fill_hazard_cancel_request_count": int(
                self.state.dynamic_fill_hazard_cancel_request_count
            ),
            "dynamic_fill_hazard_cancel_ack_count": int(
                self.state.dynamic_fill_hazard_cancel_ack_count
            ),
            "dynamic_fill_hazard_pre_ack_fill_count": int(
                self.state.dynamic_fill_hazard_pre_ack_fill_count
            ),
            "dynamic_fill_hazard_recovery_count": int(
                self.state.dynamic_fill_hazard_recovery_count
            ),
            "dynamic_fill_hazard_reentry_count": int(
                self.state.dynamic_fill_hazard_reentry_count
            ),
            "dynamic_fill_hazard_post_cancel_recovery_count": int(
                self.state.dynamic_fill_hazard_post_cancel_recovery_count
            ),
            "dynamic_fill_hazard_prospective_eval_count": int(
                self.state.dynamic_fill_hazard_prospective_eval_count
            ),
            "dynamic_fill_hazard_prospective_valid_count": int(
                self.state.dynamic_fill_hazard_prospective_valid_count
            ),
            "dynamic_fill_hazard_prospective_invalid_count": int(
                self.state.dynamic_fill_hazard_prospective_invalid_count
            ),
            "dynamic_fill_hazard_post_terminal_hazard_reuse_count": int(
                self.state.dynamic_fill_hazard_post_terminal_hazard_reuse_count
            ),
            "dynamic_fill_hazard_post_terminal_cursor_reuse_count": int(
                self.state.dynamic_fill_hazard_post_terminal_cursor_reuse_count
            ),
            "dynamic_fill_hazard_unsupported_terminal_route_count": int(
                self.state.dynamic_fill_hazard_unsupported_terminal_route_count
            ),
            "dynamic_fill_hazard_blocked_quote_count": int(
                self.state.dynamic_fill_hazard_blocked_quote_count
            ),
            "dynamic_fill_hazard_retain_invalid_count": int(
                self.state.dynamic_fill_hazard_retain_invalid_count
            ),
            "dynamic_fill_hazard_hold_active_end": bool(
                self.state.dynamic_fill_hazard_hold is not None
            ),
            "dynamic_fill_hazard_hold_phase_end": str(
                self.state.dynamic_fill_hazard_hold.get("phase", "NONE")
                if self.state.dynamic_fill_hazard_hold is not None
                else "NONE"
            ),
            "dynamic_fill_hazard_terminal_cursor_retention_end": int(
                bool(
                    self.state.dynamic_fill_hazard_hold is not None
                    and self.state.dynamic_fill_hazard_hold.get("terminal", False)
                    and str(self.state.dynamic_fill_hazard_hold.get("client_order_id", ""))
                    in self.state.dynamic_fill_hazard_paths
                )
            ),
            "fill_cooldown": self.state.fill_cooldown_s,
            "fill_cooldown_clock_mode": self.state.fill_cooldown_clock_mode,
            "fill_cooldown_consecutive_reset_policy": (
                self.state.fill_cooldown_consecutive_reset_policy
            ),
            "fill_cooldown_reset_consec_on_expiry": self.state.fill_cooldown_reset_consec_on_expiry,
            "fill_cooldown_apply_reducing": self.state.fill_cooldown_apply_reducing,
            "fill_cooldown_reducing": self.state.fill_cooldown_reducing_s,
            "fill_cooldown_reducing_inventory_lifecycle_only": self.state.fill_cooldown_reducing_inventory_lifecycle_only,
            "fill_cooldown_reducing_inv_threshold": self.state.fill_cooldown_reducing_inv_threshold,
            "fill_cooldown_reducing_inv_ratio": self.state.fill_cooldown_reducing_inv_ratio,
            "fill_cooldown_reducing_age_s": self.state.fill_cooldown_reducing_age_s,
            "fill_cooldown_reducing_vol_ref": self.state.fill_cooldown_reducing_vol_ref,
            "fill_cooldown_reducing_vol_min_mult": self.state.fill_cooldown_reducing_vol_min_mult,
            "fill_cooldown_reducing_vol_max_mult": self.state.fill_cooldown_reducing_vol_max_mult,
            "variance_time_lineage_randomized_enabled": bool(
                self.state.variance_time_lineage_randomized_enabled
            ),
            "variance_time_lineage_seed": int(self.state.variance_time_lineage_seed),
            "variance_time_lineage_control_assignments": int(
                self.state.variance_time_lineage_assignment_counts[
                    self.services.LINEAGE_CONTROL_ACTION
                ]
            ),
            "variance_time_lineage_candidate_assignments": int(
                self.state.variance_time_lineage_assignment_counts[
                    self.services.LINEAGE_CANDIDATE_ACTION
                ]
            ),
            "variance_time_lineage_full_cpp_tick_replay_authority": False,
            "post_cooldown_incremental_inventory_budget_enabled": bool(
                self.state.post_cooldown_inventory_budget_enabled
            ),
            "post_cooldown_incremental_inventory_budget_arm_id": str(
                self.state.post_cooldown_inventory_budget_arm_id
            ),
            "post_cooldown_incremental_inventory_budget_target_side": str(
                self.state.post_cooldown_inventory_budget_target_side
            ),
            "post_cooldown_incremental_inventory_budget_units": float(
                self.state.post_cooldown_inventory_budget_units
            ),
            "post_cooldown_incremental_inventory_budget_assignment_count": int(
                sum(
                    int(row.get("supported", 0) or 0)
                    for row in (self.state.trace_post_cooldown_inventory_budget or ())
                )
            ),
            "post_cooldown_incremental_inventory_budget_block_count": int(
                self.state.post_cooldown_inventory_budget_block_count
            ),
            "post_cooldown_incremental_inventory_budget_unsupported_count": int(
                self.state.post_cooldown_inventory_budget_unsupported_count
            ),
            "post_cooldown_incremental_inventory_budget_conservation_checks": int(
                self.state.post_cooldown_inventory_budget_conservation_checks
            ),
            "post_cooldown_incremental_inventory_budget_conservation_failures": int(
                self.state.post_cooldown_inventory_budget_conservation_failures
            ),
            "sell_add_inventory_price_penalty_enabled": bool(
                self.state.sell_add_price_penalty_enabled
            ),
            "sell_add_inventory_price_penalty_seed": int(
                self.state.sell_add_price_penalty_seed
            ),
            "sell_add_inventory_price_penalty_control_assignments": int(
                self.state.sell_add_price_penalty_assignment_counts[
                    self.context.SELL_ADD_PRICE_PENALTY_CONTROL
                ]
            ),
            "sell_add_inventory_price_penalty_candidate_assignments": int(
                self.state.sell_add_price_penalty_assignment_counts[
                    self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE
                ]
            ),
            "sell_add_inventory_price_penalty_unit_btc": float(
                self.state.sell_add_price_penalty_unit_btc
            ),
            "sell_add_inventory_price_penalty_step_bps": float(
                self.state.sell_add_price_penalty_step_bps
            ),
            "sell_add_inventory_price_penalty_max_bps": float(
                self.state.sell_add_price_penalty_max_bps
            ),
            "sell_add_inventory_price_penalty_full_cpp_tick_replay_authority": False,
            "multi_short_reducing_buy_aggression_enabled": bool(
                self.state.multi_short_repair_enabled
            ),
            "multi_short_reducing_buy_aggression_seed": int(
                self.state.multi_short_repair_seed
            ),
            "multi_short_reducing_buy_aggression_control_assignments": int(
                self.state.multi_short_repair_assignment_counts[self.context.MULTI_SHORT_REPAIR_CONTROL]
            ),
            "multi_short_reducing_buy_aggression_candidate_assignments": int(
                self.state.multi_short_repair_assignment_counts[self.context.MULTI_SHORT_REPAIR_CANDIDATE]
            ),
            "multi_short_reducing_buy_aggression_trigger_inventory_btc": float(
                self.state.multi_short_repair_trigger_inventory_btc
            ),
            "multi_short_reducing_buy_aggression_release_inventory_btc": float(
                self.state.multi_short_repair_release_inventory_btc
            ),
            "multi_short_reducing_buy_aggression_full_cpp_tick_replay_authority": False,
            "cross_venue_fair_center_shift_enabled": bool(
                self.state.fair_center_shift_enabled
            ),
            "cross_venue_fair_center_shift_seed": int(
                self.state.fair_center_shift_seed
            ),
            "cross_venue_fair_center_shift_family_id": str(
                self.state.fair_center_shift_family_id
            ),
            "cross_venue_fair_center_shift_control_assignments": int(
                self.state.fair_center_assignment_counts[self.context.FAIR_CENTER_CONTROL]
            ),
            "cross_venue_fair_center_shift_candidate_assignments": int(
                self.state.fair_center_assignment_counts[self.context.FAIR_CENTER_CANDIDATE]
            ),
            "cross_venue_fair_center_shift_source_kind": (
                "historical_trade_bar_1s" if self.state.fair_center_shift_enabled else ""
            ),
            "cross_venue_fair_center_shift_transport_supported": False,
            "cross_venue_fair_center_shift_full_cpp_tick_replay_authority": False,
            "lineage_randomized_outcome_contract_version": (
                self.state.lineage_outcome_contract_version
            ),
            "variance_time_reference_rate_buy_bps2_per_s": (
                self.state.variance_time_reference_rate["BUY"]
            ),
            "variance_time_reference_rate_sell_bps2_per_s": (
                self.state.variance_time_reference_rate["SELL"]
            ),
            "variance_time_minimum_wall_time_ms": (
                self.state.variance_time_minimum_wall_time_ms
            ),
            "variance_time_maximum_wall_time_ms": (
                self.state.variance_time_maximum_wall_time_ms
            ),
            "variance_time_max_feature_age_ms": self.state.variance_time_max_feature_age_ms,
            "_final_fill_cooldown_state": {
                "last_buy_fill_ts_ms": int(self.state.last_buy_fill_ts),
                "last_sell_fill_ts_ms": int(self.state.last_sell_fill_ts),
                "consec_buy": float(self.state.consec_buy),
                "consec_sell": float(self.state.consec_sell),
                "last_buy_fill_cooldown_ms": float(self.state.last_buy_fill_cooldown_ms),
                "last_sell_fill_cooldown_ms": float(self.state.last_sell_fill_cooldown_ms),
                "last_buy_fill_cooldown_total_ms": float(
                    self.state.last_buy_fill_cooldown_total_ms
                ),
                "last_sell_fill_cooldown_total_ms": float(
                    self.state.last_sell_fill_cooldown_total_ms
                ),
                "variance_time_buy_episode": self.state.variance_time_buy_snapshot,
                "variance_time_sell_episode": self.state.variance_time_sell_snapshot,
            },
            "_pnl_ts": self.state.pnl_arr,
            "_inv_ts": self.state.inv_arr,
            "_ts": self.state.ts_arr,
            "_fill_trace": self.state.trace_fills if self.state.trace_fills is not None else [],
            **({
                "economic_fill_contract": "match_facts_and_local_notifications.v1",
                "_economic_fill_trace": self.state.economic_fill_facts,
                "economic_match_cash": float(self.state.economic_match_cash),
                "economic_match_inventory": float(self.state.economic_match_inventory),
                "exchange_inventory_at_window_end": float(self.state.exchange_inventory),
            } if self.state.private_fill_visibility_enabled else {}),
            "_fill_diagnostic_counts": self.services.diagnostic_counts(self.state.trace_fills or []),
            "_trace_coverage": {
                "scope": "producer_calls_not_complete_lifecycle_journal",
                "excluded_route_calls": self.state.trace_attempt_counts["excluded_route_calls"],
                **{name: {"attempted": self.state.trace_attempt_counts[name],
                           "recorded": len(rows or []), "enabled": rows is not None,
                           "cap": cap,
                           "unrecorded": self.state.trace_attempt_counts[name] - len(rows or [])}
                   for name, rows, cap in (
                       ("fills", self.state.trace_fills, self.state.trace_fills_max),
                       ("order_outcomes", self.state.trace_orders, self.state.trace_quotes_max),
                       ("routed_decisions", self.state.trace_decisions, self.state.trace_decisions_max))},
            },
            "_utc_accounting_marks": (
                self.state.utc_accounting_marks.finish(self.state.replay_end_ts)
                if self.state.utc_accounting_marks is not None else []
            ),
            "_quote_trace": self.state.trace_orders if self.state.trace_orders is not None else [],
            "_variance_time_lineage_trace": (
                self.state.trace_variance_time_lineage
                if self.state.trace_variance_time_lineage is not None
                else []
            ),
            "_variance_time_lineage_event_journal": (
                self.state.trace_variance_time_lineage_events
                if self.state.trace_variance_time_lineage_events is not None
                else []
            ),
            "_post_cooldown_incremental_inventory_budget_trace": (
                self.state.trace_post_cooldown_inventory_budget
                if self.state.trace_post_cooldown_inventory_budget is not None
                else []
            ),
            "_sell_add_inventory_price_penalty_trace": (
                self.state.trace_sell_add_price_penalty
                if self.state.trace_sell_add_price_penalty is not None
                else []
            ),
            "_sell_add_inventory_price_penalty_event_journal": (
                self.state.trace_sell_add_price_penalty_events
                if self.state.trace_sell_add_price_penalty_events is not None
                else []
            ),
            "_sell_add_inventory_price_penalty_trace_audit": dict(
                self.state.sell_add_price_penalty_trace_audit
            ),
            "_multi_short_reducing_buy_aggression_trace": (
                self.state.trace_multi_short_repair
                if self.state.trace_multi_short_repair is not None
                else []
            ),
            "_multi_short_reducing_buy_aggression_event_journal": (
                self.state.trace_multi_short_repair_events
                if self.state.trace_multi_short_repair_events is not None
                else []
            ),
            "_multi_short_reducing_buy_aggression_trace_audit": dict(
                self.state.multi_short_repair_trace_audit
            ),
            "_cross_venue_fair_center_shift_trace": (
                self.state.trace_fair_center_shift
                if self.state.trace_fair_center_shift is not None
                else []
            ),
            "_cross_venue_fair_center_shift_event_journal": (
                self.state.trace_fair_center_shift_events
                if self.state.trace_fair_center_shift_events is not None
                else []
            ),
            "_cross_venue_fair_center_shift_trace_audit": dict(
                self.state.fair_center_trace_audit
            ),
            "_lineage_randomized_trace_audit": dict(
                self.state.lineage_randomized_trace_audit
            ),
            "_first_add_decision_to_terminal_trace": (
                self.state.trace_first_add_decision_to_terminal
                if self.state.trace_first_add_decision_to_terminal is not None
                else []
            ),
            "_first_add_decision_to_terminal_trace_audit": dict(
                self.state.first_add_decision_trace_audit
            ),
            "_first_opener_decision_to_terminal_trace": (
                self.state.trace_first_opener_decision_to_terminal
                if self.state.trace_first_opener_decision_to_terminal is not None
                else []
            ),
            "_first_opener_decision_to_terminal_trace_audit": dict(
                self.state.first_opener_decision_trace_audit
            ),
            "_adaptive_add_cooldown_trace": self.state.adaptive_add_cooldown_trace,
            "_inventory_lifecycle_repair_trace": (
                self.state.trace_inventory_lifecycle_repair if self.state.trace_inventory_lifecycle_repair is not None else []
            ),
            "_post_fill_quote_response_trace": (
                self.state.trace_post_fill_quote_response
                if self.state.trace_post_fill_quote_response is not None
                else []
            ),
            "_safe_add_rearm_trace": (
                self.state.trace_safe_add_rearm if self.state.trace_safe_add_rearm is not None else []
            ),
            "_local_action_ope_trace": (
                self.state.trace_local_action_ope if self.state.trace_local_action_ope is not None else []
            ),
            "_local_order_value_trace": (
                self.state.trace_local_order_value
                if self.state.trace_local_order_value is not None
                else []
            ),
            "_local_order_lifecycle_trace": self.state.trace_local_order_lifecycle,
            "_inventory_lifecycle_economic_fill_leg_trace": list(
                self.state.inventory_lifecycle_economic_fill_leg_trace
            ),
            "_dynamic_fill_hazard_lifecycle_journal": list(
                self.state.dynamic_fill_hazard_lifecycle_journal
            ),
            "_dynamic_fill_hazard_lifecycle_journal_audit": (
                self.services.audit_order_lifecycle_journal(
                    self.state.dynamic_fill_hazard_lifecycle_journal
                )
            ),
            "_active_order_queue_missing_trace": self.state.active_order_queue_missing_trace,
            "_exchange_book_queue_missing_trace": (
                self.state.exchange_book_queue_missing_trace
            ),
            "_exchange_book_queue_ambiguity_trace": (
                self.state.exchange_book_queue_ambiguity_trace
            ),
            "_queue_value_keep_cancel_trace": (
                self.state.trace_queue_value_keep_cancel
                if self.state.trace_queue_value_keep_cancel is not None
                else []
            ),
            "_sell_add_skip_ope_trace": (
                self.state.trace_sell_add_skip_ope
                if self.state.trace_sell_add_skip_ope is not None
                else []
            ),
            "_state_conditioned_policy_trace": (
                self.state.trace_state_conditioned_policy
                if self.state.trace_state_conditioned_policy is not None
                else []
            ),
            "_safe_add_rearm_intervention_trace": (
                self.state.trace_safe_add_rearm_intervention
                if self.state.trace_safe_add_rearm_intervention is not None
                else []
            ),
            "_state_conditioned_rearm_trace": (
                self.state.trace_state_conditioned_rearm
                if self.state.trace_state_conditioned_rearm is not None
                else []
            ),
            "_decision_trace": self.state.trace_decisions if self.state.trace_decisions is not None else [],
            "_queue_event_trace": self.state.trace_queue_events if self.state.trace_queue_events is not None else [],
            "_ema_add_wait_fork_trace": dict(self.state.ema_add_wait_fork_trace),
            "_cooldown_duration_opportunity_trace": (
                self.state.trace_cooldown_duration_opportunities
                if self.state.trace_cooldown_duration_opportunities is not None
                else []
            ),
            "_cooldown_v2_snapshot_receipts": list(
                self.state.cooldown_v2_snapshot_receipts
            ),
            "_cooldown_v2_snapshot_emitter_audit": dict(
                self.state.cooldown_v2_snapshot_emitter_audit
            ),
            "_cooldown_duration_shared_prefix_audit": dict(
                self.state.cooldown_duration_shared_prefix_audit
            ),
            "_cooldown_duration_policy_decisions": list(
                self.state.cooldown_duration_policy_decisions
            ),
            "_cooldown_duration_policy_audit": dict(
                self.state.cooldown_duration_policy_audit
            ),
            "_cooldown_duration_fork_trace": dict(
                self.state.cooldown_duration_fork_trace
            ),
        }
        for loop_item in ("eta_inventory", "a_spread"):
            self.state.coefficient_name = loop_item
            if self.context.params.get(self.state.coefficient_name) is not None:
                self.state.result[self.state.coefficient_name] = float(
                    getattr(self.state.quote_core_cfg, self.state.coefficient_name)
                )
        if self.state.order_lifecycle_journal_v2_health is not None:
            self.state.result["_order_lifecycle_journal_v2_health"] = dict(
                self.state.order_lifecycle_journal_v2_health
            )
        if self.state.cooldown_duration_shared_prefix_executor is not None:
            self.state.cooldown_duration_shared_prefix_executor.finalize_simulation_result(
                self.state.result
            )
        if self.state.risk_selection is not None:
            self.state.result.update(self.state.risk_selection.finish())
            self.state.result["risk_selection_start_ts_ms"] = self.state.replay_origin_ts_ms
            self.state.result["risk_selection_end_ts_ms"] = self.state.replay_end_ts
        for loop_item in (*self.state.bid_orders, *self.state.ask_orders):
            self.context.order = loop_item
            self.context.event = self.state.l2_production.observe("order_observation_cutoff", self.state.replay_end_ts, self.context.order['side'])
            if self.state.l2_journal is not None:
                self.state.l2_journal.emit("order_observation_cutoff", self.state.replay_end_ts, side=self.context.order['side'],
                    payload={"order_id": int(self.context.order['trace_id']), "administrative_right_censored": True,
                             "reason": "account_observation_ended_not_exchange_terminal",
                             "account_exclusive_end_ns": self.context.params.get('_l2_account_exclusive_end_ns'),
                             "last_observation_clock_ms": int(self.state.replay_end_ts),
                             "last_local_state": self.context.order.get('state'),
                             "last_exchange_accepted": self.context.order.get('exchange_accepted'),
                             "exchange_remaining_btc": self.context.order.get('exchange_remaining'),
                             "local_remaining_btc": self.context.order.get('remaining'),
                             "local_new_ack_published": self.context.order.get('local_new_ack_published'),
                             "pending_request_clocks": {k: self.context.order.get(k) for k in
                                 ('submit_ts', 'activate_ts', 'new_ack_ts', 'cancel_request_ts',
                                  'cancel_effective_ts', 'cancel_ts', 'new_rest_return_ts')},
                             "queue_state": {k: self.context.order.get(k) for k in
                                 ('queue_left', 'exchange_book_queue_path_valid',
                                  'exchange_book_queue_invalidated_reason', 'exchange_book_segment_id')}},
                    production_event=self.context.event)
        for loop_item in self.state.mo_pending:
            self.context.item = loop_item
            self.context.age = self.state.replay_end_ts - self.context.item[0]
            self._observe_markout_item(self.context.item, self.state.replay_end_ts,
                "END_NOT_MATURE" if self.context.age < self.state.markout_horizon_ms else "END_PENDING_NO_RESOLUTION")
        self._emit_markout_observation("markout_observation_cutoff", self.state.replay_end_ts, "", {
            "pending_count": len(self.state.mo_pending),
            "last_observed_pause": dict(self.state.mo_observed_pause),
            "administrative_right_censored": True, "synthetic_exit_emitted": False,
        })
        self.state.result["markout_observation_coverage"] = {
            "eligible": {k: v for k, v in self.state.l2_production.counts.items() if k.startswith("markout_")},
            "delivery_status_location": "_l2_journal.independent_delivery_verified; absent journal means NOT_RECORDED",
            "independent_veto": "NOT_EVALUATED", "source_price_age": "UNKNOWN",
        }
        self.context.event = self.state.l2_production.observe("account_end", self.state.replay_end_ts)
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("account_end", self.state.replay_end_ts,
                payload={k: self.state.result.get(k) for k in
                         ("cash_before_terminal", "final_inventory", "terminal_mtm_pnl", "pnl", "_utc_accounting_marks")},
                production_event=self.context.event)
            self.state.result["_l2_journal"] = self.state.l2_journal.close(production=self.state.l2_production.receipt())
            self.state.result["_l2_journal"]["exchange_book_stats"] = (
                vars(self.state.exchange_book_scheduler_stats) if self.state.exchange_book_scheduler_stats else None)
        if self.context.public_strategy is not None:
            self.state.result["public_strategy"] = self.context.public_strategy.report()
        if self.state.response_update_observer is not None:
            self.state.result['response_update_observation'] = self.state.response_update_observer.snapshot()
        if self.state.response_action_runtime is not None:
            self.state.result['response_action_policy'] = self.state.response_action_runtime.snapshot()
        if self.state.response_action_sampler is not None:
            self.state.result['response_action_sampling'] = dict(
                selected=self.state.response_action_sampler.selected,
                eligible=self.state.response_action_sampler.eligible,
                qualified=self.state.response_action_sampler.qualified,
                unsupported=self.state.response_action_sampler.unsupported)
        if self.context.response_update_fork is not None:
            if self.state.response_update_branch_evidence is None or self.context.response_update_cut_event_ns is None:
                raise ValueError('optional-update branch did not reach its exact decision and cutoff')
            self.state.result['response_update_fork'] = self.state.response_update_branch_evidence
            self.state.result['execution_window'] = dict(
                schema='public_bounded_execution.v1', exclusive_end_ns=self.context.response_update_fork.execution_end_ns,
                last_market_event_ns=int(self.state.trade_ts[self.state.last_processed_event_idx])*1_000_000,
                first_unprocessed_event_ns=self.context.response_update_cut_event_ns, completed=True)
        if self.context.prefill_fork is not None:
            if self.context.prefill_evidence is None or self.context.prefill_cut_event_ns is None:
                raise ValueError('prefill did not reach the exact target and exclusive event cutoff')
            self.state.result['prefill_fork'] = self.context.prefill_evidence
            self.state.result['execution_window'] = dict(
                schema='public_bounded_execution.v1', exclusive_end_ns=self.context.prefill_fork.execution_end_ns,
                last_market_event_ns=int(self.state.trade_ts[self.state.last_processed_event_idx])*1_000_000,
                first_unprocessed_event_ns=self.context.prefill_cut_event_ns, completed=True)
        if self.context.params.get('_bounded_execution_end_ns') is not None:
            # Produced only after the bounded event loop finishes. The settlement
            # consumer must not infer a short execution by clipping a full result.
            self.context.end_ns = self.context.params['_bounded_execution_end_ns']
            if int(self.state.trade_ts[-1]) != (self.context.end_ns - 1) // 1_000_000:
                raise ValueError("bounded execution clock does not reach its declared cutoff")
            if self.state.last_processed_event_idx != self.state.n_trades - 1:
                raise ValueError("bounded execution stopped before the declared cutoff")
            self.state.result['execution_window'] = {
                'schema': 'public_bounded_execution.v1',
                'exclusive_end_ns': self.context.end_ns,
                'last_market_event_ns': int(self.state.trade_ts[-1]) * 1_000_000,
                'first_unprocessed_event_ns': self.context.end_ns,
                'completed': True,
            }
        return self.state.result

    def _first_opener_feature_clock_invalid(self, row: Mapping[str, Any]) -> bool:
        decision_ts_ms = int(row["decision_ts_ms"])
        if int(row["decision_visible_feature_ready_ts_max_ms"]) != decision_ts_ms:
            return True
        if not self.state.first_opener_trace_schema_version.endswith(".v2"):
            return False
        bbo_cutoff = int(row["decision_visible_bbo_cutoff_ts_ms"])
        bbo_source = int(row["decision_bbo_source_ts_ms"])
        bbo_source_kind = str(row["decision_bbo_source_kind"])
        l2_cutoff = int(row["decision_visible_l2_cutoff_ts_ms"])
        l2_source = int(row["decision_l2_source_ts_ms"])
        trade_cutoff = int(row["decision_visible_trade_cutoff_ts_ms"])
        source_offset = int(row["exec_depth_visibility_source_offset_ms"])
        bbo_ready = int(row["bbo_feature_ready_ts_ms"])
        l2_ready = int(row["l2_feature_ready_ts_ms"])
        expected_bbo_ready = decision_ts_ms - (bbo_cutoff - bbo_source)
        expected_l2_ready = decision_ts_ms - (l2_cutoff - l2_source)
        return bool(
            bbo_source <= 0
            or l2_source <= 0
            or bbo_source > bbo_cutoff
            or bbo_cutoff
            - (source_offset if bbo_source_kind == "l2_fallback" else 0)
            > decision_ts_ms
            or bbo_source_kind not in {"bbo", "l2_fallback"}
            or l2_source > l2_cutoff
            or l2_cutoff - source_offset > decision_ts_ms
            or trade_cutoff > decision_ts_ms
            or bbo_ready != expected_bbo_ready
            or l2_ready != expected_l2_ready
            or bbo_ready > decision_ts_ms
            or l2_ready > decision_ts_ms
            or int(row["queue_ahead_asof_ts_ns"])
            > decision_ts_ms * 1_000_000
        )
