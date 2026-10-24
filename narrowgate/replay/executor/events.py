"""Causal event orchestration; phase implementations live with their state owners."""

from __future__ import annotations

from .state import ReplayRuntimeState


class ReplayEvents:
    def run_events(self):
        """Run complete events; checkpoint only before executing the selected event."""
        while True:
            if self.context.pending_resume_event is not None:
                self.state.replay_event = self.context.pending_resume_event
                self.context.pending_resume_event = None
            else:
                self.state.replay_event = self._next_replay_event()
            if self.state.replay_event is None:
                break
            if self.context.response_update_fork is not None and int(self.state.replay_event[1])*1_000_000 >= self.context.response_update_fork.execution_end_ns:
                self.context.response_update_cut_event_ns = int(self.state.replay_event[1])*1_000_000
                break
            if self.context.prefill_fork is not None and int(self.state.replay_event[1])*1_000_000 >= self.context.prefill_fork.execution_end_ns:
                self.context.prefill_cut_event_ns = int(self.state.replay_event[1])*1_000_000
                break
            if (self.context.checkpoint_at_ts_ms is not None
                    and int(self.state.replay_event[1]) >= int(self.context.checkpoint_at_ts_ms)):
                return {
                    "completed": False,
                    "_replay_checkpoint": ReplayRuntimeState.capture_checkpoint(
                        self.state, cut_ts_ms=self.context.checkpoint_at_ts_ms,
                    ),
                }
            if not self._bind_event():
                break

            # Exchange matching and private callbacks advance even when the
            # strategy's main loop is sleeping, computing, draining or blocked.
            self._advance_event_market()
            self._match_event_orders()
            if self._drain_planned_quote_stop():
                continue
            self._quarantine_flat_fork_arms()
            if self._fork_reached_economic_washout():
                break
            self._publish_event_inventory()
            if self.state.system_event_code == self.services.SYNC_CENSOR_CODE:
                self._cooldown_lineage_observe_early_blocker(
                    int(self.state.t),
                    "sync_adjust_unsupported_censor",
                )
                self.state.sync_adjust_censored = True
                self.state.sync_adjust_censor_ts_ms = int(self.state.t)
                break
            if self._process_sync_event():
                continue
            if not self._quote_opportunity_ready():
                continue

            if (
                self.state.scheduled_requote_due
                or self.state.response_force_bid_due
                or self.state.response_force_ask_due
                or self.state.continuation_force_bid_due
                or self.state.continuation_force_ask_due
            ):
                if not self._capture_quote_decision():
                    continue
                if not self._refresh_quote_inputs():
                    continue
                self._compute_quote_targets()
                self._record_quote_context()
                self._apply_quote_side_policies()
                self._apply_quote_response()
                self._apply_quote_inventory_limits()
                self._apply_quote_entry_policies()
                self._apply_quote_fill_cooldowns()
                self._apply_quote_recovery_actions()
                self._apply_order_action_policies()
                self._apply_order_safety_constraints()
                self._resolve_quote_order_updates()
                self._select_quote_risk_actions()
                self._finalize_order_actions()
                self._record_order_decisions()
                self._execute_order_decisions()
                self._finish_quote_decision()

            if self._fork_reached_economic_washout():
                break
            # Track stats
            self.state.aq = abs(self.state.q)
            if self.state.aq > self.state.mx_inv:
                self.state.mx_inv = self.state.aq
            self.state.si_sum += self.state.aq

    def _bind_event(self) -> bool:
        """Bind the selected event before any market or strategy advancement."""
        (self.state.i, self.state.event_ts_ms, self.state.is_market_array_event,
         self.state.is_main_loop_wake, self.state.is_quote_compute_resume) = self.state.replay_event
        if self.state.target_observation is not None:
            self.state.target_observation.advance(int(self.state.event_ts_ms) * 1_000_000)
        self.state.active_quote_compute = self.state.pending_quote_compute if self.state.is_quote_compute_resume else None
        if self.state.is_quote_compute_resume:
            self.state.pending_quote_compute = None
        self.state.bounded_parent_stop_requested = bool(
            self.state.cooldown_duration_shared_prefix_executor is not None
            and not bool(self.state.cooldown_duration_shared_prefix_executor.is_arm_child)
            and bool(
                self.state.cooldown_duration_shared_prefix_executor.bounded_parent_stop_requested
            )
        )
        if (
            self.state.bounded_parent_stop_requested
            or (
                self.state.cooldown_duration_parent_stop_ts_ms > 0
                and self.state.cooldown_duration_shared_prefix_executor is not None
                and not bool(self.state.cooldown_duration_shared_prefix_executor.is_arm_child)
                and int(self.state.trade_ts[self.state.i]) >= self.state.cooldown_duration_parent_stop_ts_ms
            )
        ):
            self.state.cooldown_duration_parent_stop_triggered = True
            self.state.cooldown_duration_parent_stop_trigger_ts_ms = int(self.state.trade_ts[self.state.i])
            return False
        return True

    def _process_sync_event(self) -> bool:
        """Apply a sync-degrade event without also running an ordinary quote."""
        if self.state.system_event_code == self.services.SYNC_EVENT_CODE:
            if not self.state.sync_adjust_degrade_enabled:
                raise RuntimeError(
                    "sync-degrade event supplied while the live control is disabled"
                )
            self.state.sync_adjust_degrade_trigger_count += 1
            self.state.sync_adjust_degrade_until_ms = max(
                int(self.state.sync_adjust_degrade_until_ms),
                int(self.state.t) + int(self.state.sync_adjust_pause_ms),
            )
            self._cooldown_lineage_observe_early_blocker(
                int(self.state.t),
                "sync_adjust_degrade",
            )
            if self.state.sync_adjust_cancel_orders:
                self._request_cancel_all(
                    self.state.bid_orders,
                    int(self.state.t),
                    reason="sync_adjust_degrade",
                )
                self._request_cancel_all(
                    self.state.ask_orders,
                    int(self.state.t),
                    reason="sync_adjust_degrade",
                )
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
            return True
        return False
