"""Replay inventory policy owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional

from strategy.inventory_soft_control import pre_generic_sizes


class InventoryPolicy:
    def _inventory_lifecycle_pnl(self, mark_px: float) -> float:
        return self.state.cash + self.state.q * mark_px

    def _append_fair_center_event(self,
        record: dict[str, Any],
        event_type: str,
        event_ts_ms: int,
        **payload: Any,
    ) -> None:
        if self.state.trace_fair_center_shift_events is None:
            return
        event_seq = int(record.get("_event_seq", 0))
        event = {
            "event_schema_version": "narrowgate_lineage_event_journal.v2",
            "lineage_uid": str(record["lineage_uid"]),
            "lineage_id": int(record["lineage_id"]),
            "inventory_lifecycle_id": int(record["inventory_lifecycle_id"]),
            "day": str(record["day"]),
            "side": str(record["side"]),
            "action": str(record["action"]),
            "event_seq": event_seq,
            "event_type": str(event_type),
            "event_ts_ms": int(event_ts_ms),
            "episode_index": 1,
        }
        event.update(payload)
        self.state.trace_fair_center_shift_events.append(event)
        record["_event_seq"] = event_seq + 1

    def _fair_center_update_path(self, now_ts_ms: int, mark_px: float) -> None:
        record = self.state.fair_center_open_record
        if record is None:
            return
        last_ts = int(record.get("last_path_ts_ms", now_ts_ms))
        dt_s = max(0.0, float(int(now_ts_ms) - last_ts) / 1_000.0)
        record["inventory_time_btc_s"] = float(
            record.get("inventory_time_btc_s", 0.0)
        ) + abs(float(record.get("last_path_inventory", self.state.q))) * dt_s
        current_equity = float(self._inventory_lifecycle_pnl(mark_px))
        post_assignment = current_equity - float(record["assignment_equity_usdc"])
        record["lineage_mae"] = min(
            float(record.get("lineage_mae", 0.0)),
            post_assignment,
        )
        record["lineage_max_abs_inventory"] = max(
            float(record.get("lineage_max_abs_inventory", 0.0)),
            abs(float(self.state.q)),
        )
        record["last_path_ts_ms"] = int(now_ts_ms)
        record["last_path_inventory"] = float(self.state.q)

    def _start_fair_center_assignment(self,
        now_ts_ms: int,
        mark_px: float,
        *,
        state: Any,
        preview: Any,
    ) -> dict[str, Any]:
        if not self.state.fair_center_shift_enabled:
            raise RuntimeError("fair-center assignment is disabled")
        if self.state.fair_center_open_record is not None:
            return self.state.fair_center_open_record
        if self.state.inventory_lifecycle_active or abs(float(self.state.q)) > 1e-10:
            raise RuntimeError("fair-center assignment must precede the opener")
        if not bool(state.valid) or not bool(preview.valid):
            raise RuntimeError("fair-center assignment requires valid causal support")
        if int(preview.effective_shift_ticks) == 0:
            raise RuntimeError("fair-center assignment requires a nonzero tick action")
        if self.context.fair_center_lead_direction is None or self.state.fair_center_randomizer is None:
            raise RuntimeError("fair-center action kernels are unavailable")
        completed = len(self.state.trace_fair_center_shift or ())
        if completed >= self.state.trace_fair_center_shift_max:
            raise RuntimeError("fair-center trace limit exhausted before day end")
        assignment_day = self.services.pd.to_datetime(
            int(now_ts_ms), unit="ms", utc=True
        ).strftime("%Y-%m-%d")
        prospective_id = int(self.state.fair_center_next_id) + 1
        prospective_inventory_lifecycle_id = int(self.state.inventory_lifecycle_count) + 1
        direction = self.context.fair_center_lead_direction(state)
        lineage_uid = (
            f"{self.services.SYMBOL}:{assignment_day}:FAIR_CENTER:"
            f"{prospective_inventory_lifecycle_id}:{int(now_ts_ms)}:{prospective_id}:"
            f"{direction}"
        )
        assignment = self.state.fair_center_randomizer.assign(
            utc_day=assignment_day,
            assignment_lead_direction=direction,
            pre_assignment_inventory_lifecycle_uid=lineage_uid,
        )
        action = str(self.state.fair_center_forced_action or assignment.action)
        self.state.fair_center_next_id = prospective_id
        self.state.fair_center_assignment_counts[action] += 1
        assignment_equity = float(self._inventory_lifecycle_pnl(mark_px))
        record = {
            "schema_version": "narrowgate_lineage_randomized_trace.v2",
            "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
            "mechanism_extension_schema": "none",
            "decision_id": (
                f"fair_center:{self.services.SYMBOL}:{assignment_day}:"
                f"{prospective_inventory_lifecycle_id}:{int(now_ts_ms)}"
            ),
            "lineage_uid": lineage_uid,
            "lineage_id": prospective_id,
            "inventory_lifecycle_id": prospective_inventory_lifecycle_id,
            "day": assignment_day,
            "side": direction,
            "inventory_role": "prospective_inventory_lifecycle_pair",
            "action": action,
            "behavior_propensity": float(
                1.0 if self.state.fair_center_forced_action else self.state.fair_center_probabilities[action]
            ),
            "behavior_prob_local_quote_center": float(
                (
                    int(self.state.fair_center_forced_action == self.context.FAIR_CENTER_CONTROL)
                    if self.state.fair_center_forced_action
                    else self.state.fair_center_probabilities[self.context.FAIR_CENTER_CONTROL]
                )
            ),
            "behavior_prob_cross_venue_fair_quote_center": float(
                (
                    int(self.state.fair_center_forced_action == self.context.FAIR_CENTER_CANDIDATE)
                    if self.state.fair_center_forced_action
                    else self.state.fair_center_probabilities[self.context.FAIR_CENTER_CANDIDATE]
                )
            ),
            "assignment_u": float(assignment.uniform_draw),
            "random_seed": int(self.state.fair_center_shift_seed),
            "randomization_stratum": str(assignment.randomization_stratum),
            "assignment_lead_direction": direction,
            "assignment_ts_ms": int(now_ts_ms),
            "decision_ts_ms": int(now_ts_ms),
            "decision_ts_ns": int(now_ts_ms) * 1_000_000,
            "assignment_before_downstream_path": 1,
            "assignment_fixed_within_lineage": 1,
            "assignment_inventory_btc": float(self.state.q),
            "inventory_lifecycle_age_at_assignment_ms": 0,
            "inventory_lifecycle_start_ts_ms": 0,
            "inventory_lifecycle_started": 0,
            "opener_side": "",
            "inventory_lifecycle_start_equity_usdc": assignment_equity,
            "assignment_equity_usdc": assignment_equity,
            "lineage_terminal_equity_usdc": assignment_equity,
            "inventory_lifecycle_terminal_equity_usdc": assignment_equity,
            "pre_assignment_inventory_lifecycle_pnl_usdc": 0.0,
            "lineage_reward_usdc": 0.0,
            "post_lineage_continuation_value_usdc": 0.0,
            "decision_to_inventory_lifecycle_terminal_value_usdc": 0.0,
            "accounting_identity_error_usdc": 0.0,
            "reward": 0.0,
            "lineage_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_ts_ms": 0,
            "lineage_terminal_reason": "pending",
            "inventory_lifecycle_terminal_reason": "pending",
            "lineage_censored": 0,
            "inventory_lifecycle_censored": 0,
            "final_blocker": "not_observed",
            "final_action_change_ts_ms": 0,
            "final_action_change_status": "eligible_no_change",
            "fair_state_evaluation_count": 0,
            "fair_state_valid_count": 0,
            "fair_state_invalid_count": 0,
            "candidate_coordinate_change_count": 0,
            "actual_final_action_change_count": 0,
            "requested_shift_ticks_abs_sum": 0.0,
            "effective_shift_ticks_abs_sum": 0.0,
            "gtx_clamp_count": 0,
            "pair_spread_violation_count": 0,
            "fill_count": 0,
            "fill_qty_btc": 0.0,
            "buy_fill_count": 0,
            "sell_fill_count": 0,
            "order_submit_count": 0,
            "cancel_request_count": 0,
            "replace_cancel_request_count": 0,
            "queue_reset_count": 0,
            "maker_violation_count": 0,
            "action_generated_ioc_or_taker_count": 0,
            "inventory_time_btc_s": 0.0,
            "lineage_mae": 0.0,
            "lineage_max_abs_inventory": 0.0,
            "last_path_ts_ms": int(now_ts_ms),
            "last_path_inventory": float(self.state.q),
            "support_valid": 1,
            "unsupported_reasons": "",
            "source_kind": "historical_trade_bar_1s",
            "transport_supported": 0,
            "full_cpp_tick_replay_authority": 0,
            "daily_fresh_start": 1,
            "reward_definition": (
                "assignment_to_inventory_lifecycle_terminal_equity_change_usdc"
            ),
            "queue_cost": 0.0,
            "queue_cost_definition": (
                "zero_separate_term_full_path_effect_embedded_in_terminal_reward"
            ),
            "_event_seq": 0,
        }
        self.state.fair_center_open_record = record
        self._append_fair_center_event(
            record,
            "assignment",
            int(now_ts_ms),
            assignment_u=float(assignment.uniform_draw),
            behavior_propensity=float(
                1.0 if self.state.fair_center_forced_action else self.state.fair_center_probabilities[action]
            ),
            assignment_lead_direction=direction,
            requested_shift_ticks=int(preview.requested_shift_ticks),
            effective_shift_ticks=int(preview.effective_shift_ticks),
        )
        return record

    def _apply_fair_center_shift(self,
        now_ts_ms: int,
        *,
        baseline_bid: float,
        baseline_ask: float,
        best_bid: float,
        best_ask: float,
        mark_px: float,
        bid_can_post: bool,
        ask_can_post: bool,
    ) -> tuple[float, float, bool]:
        if not self.state.fair_center_shift_enabled:
            return float(baseline_bid), float(baseline_ask), False
        if self.state.fair_center_cursor is None or self.context.project_fair_center_action_pair is None:
            raise RuntimeError("fair-center replay cursor is unavailable")
        state = self.state.fair_center_cursor.asof(
            int(now_ts_ms) * 1_000_000,
            local_mid=float(mark_px),
        )
        preview = self.context.project_fair_center_action_pair(
            self.context.FAIR_CENTER_CANDIDATE,
            state,
            baseline_bid=float(baseline_bid),
            baseline_ask=float(baseline_ask),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            tick_size=float(self.state.TICK),
        )
        if (
            self.state.fair_center_open_record is None
            and not self.state.inventory_lifecycle_active
            and abs(float(self.state.q)) <= 1e-10
            and (bool(bid_can_post) or bool(ask_can_post))
            and bool(preview.valid)
            and int(preview.effective_shift_ticks) != 0
        ):
            self._start_fair_center_assignment(
                int(now_ts_ms),
                float(mark_px),
                state=state,
                preview=preview,
            )
        record = self.state.fair_center_open_record
        if record is None:
            return float(baseline_bid), float(baseline_ask), False
        record["fair_state_evaluation_count"] = int(
            record.get("fair_state_evaluation_count", 0)
        ) + 1
        record["fair_state_valid_count"] = int(
            record.get("fair_state_valid_count", 0)
        ) + int(bool(state.valid))
        record["fair_state_invalid_count"] = int(
            record.get("fair_state_invalid_count", 0)
        ) + int(not bool(state.valid))
        selected = self.context.project_fair_center_action_pair(
            str(record["action"]),
            state,
            baseline_bid=float(baseline_bid),
            baseline_ask=float(baseline_ask),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            tick_size=float(self.state.TICK),
        )
        record["requested_shift_ticks_abs_sum"] = float(
            record.get("requested_shift_ticks_abs_sum", 0.0)
        ) + abs(float(selected.requested_shift_ticks))
        record["effective_shift_ticks_abs_sum"] = float(
            record.get("effective_shift_ticks_abs_sum", 0.0)
        ) + abs(float(selected.effective_shift_ticks))
        record["gtx_clamp_count"] = int(record.get("gtx_clamp_count", 0)) + int(
            bool(selected.gtx_clamped)
        )
        record["pair_spread_violation_count"] = int(
            record.get("pair_spread_violation_count", 0)
        ) + int(not bool(selected.pair_spread_preserved))
        maker_violation = bool(
            selected.valid
            and (
                (bool(bid_can_post) and float(selected.candidate_bid) >= float(best_ask))
                or (
                    bool(ask_can_post)
                    and float(selected.candidate_ask) <= float(best_bid)
                )
            )
        )
        record["maker_violation_count"] = int(
            record.get("maker_violation_count", 0)
        ) + int(maker_violation)
        if maker_violation:
            raise RuntimeError("fair-center candidate violated the GTX maker boundary")
        if not state.valid:
            record["final_blocker"] = str(state.reason)
        changed = bool(
            str(record["action"]) == self.context.FAIR_CENTER_CANDIDATE
            and selected.valid
            and int(selected.effective_shift_ticks) != 0
            and (bool(bid_can_post) or bool(ask_can_post))
        )
        if changed:
            record["candidate_coordinate_change_count"] = int(
                record.get("candidate_coordinate_change_count", 0)
            ) + 1
            if int(record.get("candidate_coordinate_change_count", 0)) == 1:
                self._append_fair_center_event(
                    record,
                    "first_quote_coordinate_change",
                    int(now_ts_ms),
                    requested_shift_ticks=int(selected.requested_shift_ticks),
                    effective_shift_ticks=int(selected.effective_shift_ticks),
                )
        return (
            float(selected.candidate_bid),
            float(selected.candidate_ask),
            changed,
        )

    def _fair_center_on_inventory_lifecycle_open(self, side: str, now_ts_ms: int) -> None:
        record = self.state.fair_center_open_record
        if record is None:
            return
        if int(record["inventory_lifecycle_id"]) != int(self.state.inventory_lifecycle_count):
            raise RuntimeError("fair-center prospective inventory_lifecycle identity drifted")
        record["inventory_lifecycle_started"] = 1
        record["opener_side"] = str(side).upper()
        record["inventory_lifecycle_start_ts_ms"] = int(now_ts_ms)
        self._append_fair_center_event(
            record,
            "inventory_lifecycle_open",
            int(now_ts_ms),
            opener_side=str(side).upper(),
        )

    def _fair_center_note_fill(self,
        side: str,
        fill_qty: float,
        now_ts_ms: int,
    ) -> None:
        record = self.state.fair_center_open_record
        if record is None:
            return
        record["fill_count"] = int(record.get("fill_count", 0)) + 1
        record["fill_qty_btc"] = float(record.get("fill_qty_btc", 0.0)) + float(
            fill_qty
        )
        key = "buy_fill_count" if str(side).upper() == "BUY" else "sell_fill_count"
        record[key] = int(record.get(key, 0)) + 1
        self._append_fair_center_event(
            record,
            "fill",
            int(now_ts_ms),
            fill_side=str(side).upper(),
            fill_qty_btc=float(fill_qty),
            inventory_after_fill_btc=float(self.state.q),
        )

    def _finalize_fair_center_assignment(self,
        outcome_ts_ms: int,
        mark_px: float,
        *,
        terminal_reason: str,
        censored: bool,
    ) -> tuple[dict[str, Any], Any] | None:
        record = self.state.fair_center_open_record
        if record is None:
            return None
        self._fair_center_update_path(int(outcome_ts_ms), float(mark_px))
        if str(record.get("final_blocker", "")) == "not_observed":
            record["final_blocker"] = (
                "control_action"
                if str(record.get("action", "")) == self.context.FAIR_CENTER_CONTROL
                else "no_final_order_action"
            )
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        decision_to_terminal = terminal_equity - float(
            record["assignment_equity_usdc"]
        )
        record.update(
            {
                "lineage_terminal_ts_ms": int(outcome_ts_ms),
                "inventory_lifecycle_terminal_ts_ms": int(outcome_ts_ms),
                "lineage_terminal_equity_usdc": terminal_equity,
                "inventory_lifecycle_terminal_equity_usdc": terminal_equity,
                "lineage_terminal_reason": str(terminal_reason),
                "inventory_lifecycle_terminal_reason": str(terminal_reason),
                "lineage_censored": int(bool(censored)),
                "inventory_lifecycle_censored": int(bool(censored)),
                "lineage_reward_usdc": decision_to_terminal,
                "post_lineage_continuation_value_usdc": 0.0,
                "decision_to_inventory_lifecycle_terminal_value_usdc": decision_to_terminal,
                "accounting_identity_error_usdc": 0.0,
                "reward": decision_to_terminal,
            }
        )
        self._append_fair_center_event(
            record,
            "lineage_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            censored=int(bool(censored)),
        )
        self._append_fair_center_event(
            record,
            "inventory_lifecycle_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            censored=int(bool(censored)),
        )
        if self.state.trace_fair_center_shift is None:
            raise RuntimeError("fair-center trace is unavailable")
        self.state.trace_fair_center_shift.append(record)
        self.state.fair_center_open_record = None
        return record

    def _append_sell_add_price_penalty_event(self,
        record: dict[str, Any],
        event_type: str,
        event_ts_ms: int,
        **payload: Any,
    ) -> None:
        if self.state.trace_sell_add_price_penalty_events is None:
            return
        event_seq = int(record.get("_event_seq", 0))
        event = {
            "event_schema_version": "narrowgate_lineage_event_journal.v2",
            "lineage_uid": str(record["lineage_uid"]),
            "lineage_id": int(record["lineage_id"]),
            "inventory_lifecycle_id": int(record["inventory_lifecycle_id"]),
            "day": str(record["day"]),
            "side": "SELL",
            "action": str(record["action"]),
            "event_seq": event_seq,
            "event_type": str(event_type),
            "event_ts_ms": int(event_ts_ms),
            "episode_index": 1,
        }
        event.update(payload)
        self.state.trace_sell_add_price_penalty_events.append(event)
        record["_event_seq"] = event_seq + 1

    def _sell_add_price_penalty_update_path(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        record = self.state.sell_add_price_penalty_open_record
        if record is None:
            return
        last_ts = int(record.get("last_path_ts_ms", now_ts_ms))
        dt_s = max(0.0, float(int(now_ts_ms) - last_ts) / 1_000.0)
        record["inventory_time_btc_s"] = float(
            record.get("inventory_time_btc_s", 0.0)
        ) + abs(float(record.get("last_path_inventory", self.state.q))) * dt_s
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        post_assignment_value = terminal_equity - float(
            record["assignment_equity_usdc"]
        )
        record["lineage_mae"] = min(
            float(record.get("lineage_mae", 0.0)),
            float(post_assignment_value),
        )
        record["lineage_max_abs_inventory"] = max(
            float(record.get("lineage_max_abs_inventory", 0.0)),
            abs(float(self.state.q)),
        )
        short_units = max(0.0, -float(self.state.q) / self.state.sell_add_price_penalty_unit_btc)
        record["max_short_inventory_units"] = max(
            float(record.get("max_short_inventory_units", 0.0)),
            short_units,
        )
        if short_units >= 2.0 - 1e-9:
            record["entered_multilevel_short"] = 1
        record["last_path_ts_ms"] = int(now_ts_ms)
        record["last_path_inventory"] = float(self.state.q)

    def _start_sell_add_price_penalty_inventory_lifecycle(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> dict[str, Any]:
        if not self.state.sell_add_price_penalty_enabled:
            raise RuntimeError("SELL-add price-penalty assignment is disabled")
        if not self.state.inventory_lifecycle_active or float(self.state.q) >= -1e-10:
            raise RuntimeError(
                "SELL-add price-penalty assignment requires an active SHORT inventory_lifecycle"
            )
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        if inventory_lifecycle_id in self.state.sell_add_price_penalty_assigned_inventory_lifecycles:
            raise RuntimeError(
                "SELL-add price-penalty contract forbids inventory_lifecycle rerandomization"
            )
        if self.state.sell_add_price_penalty_open_record is not None:
            raise RuntimeError("a SELL-add price-penalty inventory_lifecycle is already open")
        in_flight = len(self.state.trace_sell_add_price_penalty or ())
        if in_flight >= self.state.trace_sell_add_price_penalty_max:
            raise RuntimeError(
                "trace_sell_add_inventory_price_penalty_max exhausted before day end"
            )
        assignment_day = self.services.pd.to_datetime(
            int(now_ts_ms), unit="ms", utc=True
        ).strftime("%Y-%m-%d")
        prospective_id = int(self.state.sell_add_price_penalty_next_id) + 1
        lineage_uid = (
            f"{self.services.SYMBOL}:{assignment_day}:SELL_ADD_PRICE:{inventory_lifecycle_id}:"
            f"{int(now_ts_ms)}:{prospective_id}"
        )
        if self.state.sell_add_price_penalty_randomizer is None:
            raise RuntimeError("SELL-add price-penalty randomizer is unavailable")
        assignment = self.state.sell_add_price_penalty_randomizer.assign(
            utc_day=assignment_day,
            pre_assignment_inventory_lifecycle_uid=lineage_uid,
        )
        action = str(assignment.action)
        self.state.sell_add_price_penalty_next_id = prospective_id
        self.state.sell_add_price_penalty_assignment_counts[action] += 1
        self.state.sell_add_price_penalty_assigned_inventory_lifecycles.add(inventory_lifecycle_id)
        assignment_equity = float(self._inventory_lifecycle_pnl(mark_px))
        record = {
            "schema_version": "narrowgate_lineage_randomized_trace.v2",
            "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
            "mechanism_extension_schema": "none",
            "decision_id": (
                f"sell_add_price_penalty:{self.services.SYMBOL}:{assignment_day}:"
                f"{inventory_lifecycle_id}:{int(now_ts_ms)}"
            ),
            "lineage_uid": lineage_uid,
            "lineage_id": prospective_id,
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "day": assignment_day,
            "side": "SELL",
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": float(
                self.state.sell_add_price_penalty_probabilities[action]
            ),
            "behavior_prob_baseline_current_quote": float(
                self.state.sell_add_price_penalty_probabilities[
                    self.context.SELL_ADD_PRICE_PENALTY_CONTROL
                ]
            ),
            "behavior_prob_sell_add_inventory_price_penalty": float(
                self.state.sell_add_price_penalty_probabilities[
                    self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE
                ]
            ),
            "assignment_u": float(assignment.uniform_draw),
            "random_seed": int(self.state.sell_add_price_penalty_seed),
            "randomization_stratum": str(assignment.randomization_stratum),
            "assignment_ts_ms": int(now_ts_ms),
            "decision_ts_ms": int(now_ts_ms),
            "decision_ts_ns": int(now_ts_ms) * 1_000_000,
            "assignment_before_downstream_path": 1,
            "assignment_fixed_within_lineage": 1,
            "trigger_fill_excluded_from_reward": 1,
            "assignment_inventory_btc": float(self.state.q),
            "inventory_lifecycle_age_at_assignment_ms": max(
                0, int(now_ts_ms) - int(self.state.inventory_lifecycle_start_ts)
            ),
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_start_equity_usdc": float(self.state.inventory_lifecycle_start_pnl),
            "assignment_equity_usdc": assignment_equity,
            "lineage_terminal_equity_usdc": assignment_equity,
            "inventory_lifecycle_terminal_equity_usdc": assignment_equity,
            "pre_assignment_inventory_lifecycle_pnl_usdc": float(
                assignment_equity - self.state.inventory_lifecycle_start_pnl
            ),
            "lineage_reward_usdc": 0.0,
            "post_lineage_continuation_value_usdc": 0.0,
            "decision_to_inventory_lifecycle_terminal_value_usdc": 0.0,
            "accounting_identity_error_usdc": 0.0,
            "lineage_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_ts_ms": 0,
            "lineage_terminal_reason": "pending",
            "inventory_lifecycle_terminal_reason": "pending",
            "final_blocker": "none",
            "final_action_change_ts_ms": 0,
            "final_action_change_status": "eligible_no_change",
            "eligible_sell_add_quote_count": 0,
            "candidate_quote_count": 0,
            "actual_final_action_change_count": 0,
            "cap_truncation_count": 0,
            "full_cap_truncation_count": 0,
            "requested_penalty_bps_sum": 0.0,
            "realized_penalty_bps_sum": 0.0,
            "requested_penalty_ticks_sum": 0.0,
            "realized_penalty_ticks_sum": 0.0,
            "minimum_realized_to_requested_ratio": 1.0,
            "order_submit_count": 0,
            "sell_order_submit_count": 0,
            "cancel_request_count": 0,
            "replace_cancel_request_count": 0,
            "queue_reset_count": 0,
            "intervention_fill_count": 0,
            "intervention_fill_qty": 0.0,
            "sell_add_fill_count": 0,
            "sell_add_fill_qty": 0.0,
            "reducing_buy_fill_count": 0,
            "reducing_buy_fill_qty": 0.0,
            "entered_multilevel_short": int(
                -float(self.state.q) / self.state.sell_add_price_penalty_unit_btc >= 2.0 - 1e-9
            ),
            "max_short_inventory_units": max(
                0.0, -float(self.state.q) / self.state.sell_add_price_penalty_unit_btc
            ),
            "inventory_time_btc_s": 0.0,
            "lineage_mae": 0.0,
            "lineage_max_abs_inventory": abs(float(self.state.q)),
            "last_path_ts_ms": int(now_ts_ms),
            "last_path_inventory": float(self.state.q),
            "queue_cost": 0.0,
            "support_valid": 1,
            "unsupported_reasons": "",
            "full_cpp_tick_replay_authority": 0,
            "daily_fresh_start": 1,
            "price_penalty_inventory_unit_btc": float(
                self.state.sell_add_price_penalty_unit_btc
            ),
            "price_penalty_step_bps": float(self.state.sell_add_price_penalty_step_bps),
            "price_penalty_max_bps": float(self.state.sell_add_price_penalty_max_bps),
            "reward_definition": (
                "assignment_to_inventory_lifecycle_terminal_equity_change_usdc"
            ),
            "queue_cost_definition": (
                "zero_separate_term_full_path_effect_embedded_in_terminal_reward_"
                "with_reset_counts_as_diagnostic"
            ),
            "_event_seq": 0,
        }
        self.state.sell_add_price_penalty_open_record = record
        self._append_sell_add_price_penalty_event(
            record,
            "assignment",
            int(now_ts_ms),
            assignment_u=float(assignment.uniform_draw),
            behavior_propensity=float(
                self.state.sell_add_price_penalty_probabilities[action]
            ),
        )
        return record

    def _apply_sell_add_price_penalty_action(self,
        now_ts_ms: int,
        *,
        baseline_bid: float,
        baseline_ask: float,
        mark_px: float,
        max_pair_spread: float,
        can_post: bool,
        side_ctx: dict[str, Any],
    ) -> tuple[float, bool]:
        if (
            not self.state.sell_add_price_penalty_enabled
            or not self.state.inventory_lifecycle_active
            or float(self.state.q) >= -1e-10
            or not bool(can_post)
        ):
            return float(baseline_ask), False
        record = self.state.sell_add_price_penalty_open_record
        if record is None:
            record = self._start_sell_add_price_penalty_inventory_lifecycle(
                int(now_ts_ms), float(mark_px)
            )
        if int(record["inventory_lifecycle_id"]) != int(self.state.inventory_lifecycle_count):
            raise RuntimeError("SELL-add price-penalty inventory_lifecycle identity drifted")
        record["eligible_sell_add_quote_count"] = int(
            record.get("eligible_sell_add_quote_count", 0)
        ) + 1
        side_ctx["sell_add_inventory_price_penalty_lineage_id"] = int(
            record["lineage_id"]
        )
        side_ctx["sell_add_inventory_price_penalty_action"] = str(
            record["action"]
        )
        if str(record["action"]) != self.context.SELL_ADD_PRICE_PENALTY_CANDIDATE:
            return float(baseline_ask), False
        if self.context.apply_sell_add_price_penalty is None:
            raise RuntimeError("SELL-add price-penalty quote kernel is unavailable")
        quote = self.context.apply_sell_add_price_penalty(
            baseline_bid=float(baseline_bid),
            baseline_ask=float(baseline_ask),
            mid=float(mark_px),
            inventory_btc=float(self.state.q),
            tick_size=float(self.state.TICK),
            max_pair_spread=float(max_pair_spread),
            inventory_unit_btc=float(self.state.sell_add_price_penalty_unit_btc),
            step_bps_per_short_unit=float(self.state.sell_add_price_penalty_step_bps),
            max_penalty_bps=float(self.state.sell_add_price_penalty_max_bps),
        )
        record["candidate_quote_count"] = int(
            record.get("candidate_quote_count", 0)
        ) + 1
        record["requested_penalty_bps_sum"] = float(
            record.get("requested_penalty_bps_sum", 0.0)
        ) + float(quote.requested_penalty_bps)
        record["realized_penalty_bps_sum"] = float(
            record.get("realized_penalty_bps_sum", 0.0)
        ) + float(quote.realized_penalty_bps)
        record["requested_penalty_ticks_sum"] = float(
            record.get("requested_penalty_ticks_sum", 0.0)
        ) + float(quote.requested_penalty_ticks)
        record["realized_penalty_ticks_sum"] = float(
            record.get("realized_penalty_ticks_sum", 0.0)
        ) + float(quote.realized_penalty_ticks)
        record["cap_truncation_count"] = int(
            record.get("cap_truncation_count", 0)
        ) + int(quote.cap_truncated)
        record["full_cap_truncation_count"] = int(
            record.get("full_cap_truncation_count", 0)
        ) + int(quote.fully_truncated)
        ratio = (
            float(quote.realized_penalty_ticks)
            / max(float(quote.requested_penalty_ticks), 1e-12)
        )
        record["minimum_realized_to_requested_ratio"] = min(
            float(record.get("minimum_realized_to_requested_ratio", 1.0)),
            ratio,
        )
        changed = bool(
            float(quote.selected_ask) > float(baseline_ask) + 1e-12
        )
        if changed:
            record["actual_final_action_change_count"] = int(
                record.get("actual_final_action_change_count", 0)
            ) + 1
            if int(record.get("final_action_change_ts_ms", 0) or 0) <= 0:
                record["final_action_change_ts_ms"] = int(now_ts_ms)
                record["final_action_change_status"] = "observed"
                self._append_sell_add_price_penalty_event(
                    record,
                    "action_change",
                    int(now_ts_ms),
                    baseline_ask=float(baseline_ask),
                    selected_ask=float(quote.selected_ask),
                    requested_penalty_bps=float(quote.requested_penalty_bps),
                    realized_penalty_bps=float(quote.realized_penalty_bps),
                )
        side_ctx.update(
            {
                "sell_add_inventory_price_penalty_requested_bps": float(
                    quote.requested_penalty_bps
                ),
                "sell_add_inventory_price_penalty_realized_bps": float(
                    quote.realized_penalty_bps
                ),
                "sell_add_inventory_price_penalty_cap_truncated": bool(
                    quote.cap_truncated
                ),
            }
        )
        return float(quote.selected_ask), changed

    def _sell_add_price_penalty_on_fill(self,
        side: str,
        prev_q: float,
        fill_qty: float,
    ) -> None:
        record = self.state.sell_add_price_penalty_open_record
        if record is None:
            return
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0)
        ) + 1
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0)
        ) + float(fill_qty)
        exposure_increasing = abs(float(self.state.q)) > abs(float(prev_q)) + 1e-10
        reducing = abs(float(self.state.q)) < abs(float(prev_q)) - 1e-10
        if str(side).upper() == "SELL" and exposure_increasing:
            record["sell_add_fill_count"] = int(
                record.get("sell_add_fill_count", 0)
            ) + 1
            record["sell_add_fill_qty"] = float(
                record.get("sell_add_fill_qty", 0.0)
            ) + float(fill_qty)
        if str(side).upper() == "BUY" and reducing:
            record["reducing_buy_fill_count"] = int(
                record.get("reducing_buy_fill_count", 0)
            ) + 1
            record["reducing_buy_fill_qty"] = float(
                record.get("reducing_buy_fill_qty", 0.0)
            ) + float(fill_qty)

    def _finalize_sell_add_price_penalty_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        outcome_ts_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        record = self.state.sell_add_price_penalty_open_record
        if record is None or int(record["inventory_lifecycle_id"]) != int(inventory_lifecycle_id):
            return
        self._sell_add_price_penalty_update_path(int(outcome_ts_ms), float(mark_px))
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        assignment_equity = float(record["assignment_equity_usdc"])
        inventory_lifecycle_start_equity = float(record["inventory_lifecycle_start_equity_usdc"])
        decision_to_terminal = terminal_equity - assignment_equity
        pre_assignment = assignment_equity - inventory_lifecycle_start_equity
        record.update(
            {
                "lineage_terminal_ts_ms": int(outcome_ts_ms),
                "inventory_lifecycle_terminal_ts_ms": int(outcome_ts_ms),
                "lineage_terminal_reason": str(terminal_reason),
                "inventory_lifecycle_terminal_reason": str(terminal_reason),
                "lineage_terminal_equity_usdc": terminal_equity,
                "inventory_lifecycle_terminal_equity_usdc": terminal_equity,
                "pre_assignment_inventory_lifecycle_pnl_usdc": float(pre_assignment),
                "lineage_reward_usdc": float(decision_to_terminal),
                "post_lineage_continuation_value_usdc": 0.0,
                "decision_to_inventory_lifecycle_terminal_value_usdc": float(
                    decision_to_terminal
                ),
                "accounting_identity_error_usdc": 0.0,
                "reward": float(decision_to_terminal),
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_equity - inventory_lifecycle_start_equity
                ),
                "multilevel_short_terminal_value_usdc": float(
                    decision_to_terminal
                    if int(record.get("entered_multilevel_short", 0)) == 1
                    else 0.0
                ),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "repair_event": int(bool(closed)),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "decision_to_terminal_s": max(
                    0.0,
                    float(int(outcome_ts_ms) - int(record["assignment_ts_ms"]))
                    / 1_000.0,
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(int(outcome_ts_ms) - int(record["inventory_lifecycle_start_ts_ms"]))
                    / 1_000.0,
                ),
                "lineage_censored": int(not closed),
                "support_valid": int(bool(record.get("support_valid", 1))),
            }
        )
        if int(record.get("final_action_change_ts_ms", 0) or 0) <= 0:
            record["final_action_change_status"] = "eligible_no_change"
        self._append_sell_add_price_penalty_event(
            record,
            "lineage_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            censored=int(not closed),
        )
        self._append_sell_add_price_penalty_event(
            record,
            "inventory_lifecycle_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            closed=int(bool(closed)),
        )
        record.pop("_event_seq", None)
        if self.state.trace_sell_add_price_penalty is None:
            raise RuntimeError("SELL-add price-penalty trace is unavailable")
        self.state.trace_sell_add_price_penalty.append(record)
        self.state.sell_add_price_penalty_open_record = None

    def _multi_short_repair_inventory_lifecycle_record(self) -> dict[str, Any] | None:
        record = self.state.multi_short_repair_open_record
        if (
            record is not None
            and int(record.get("inventory_lifecycle_id", -1)) == int(self.state.inventory_lifecycle_count)
        ):
            return record
        return self.state.multi_short_repair_pending_inventory_lifecycle.get(int(self.state.inventory_lifecycle_count))

    def _append_multi_short_repair_event(self,
        record: dict[str, Any],
        event_type: str,
        event_ts_ms: int,
        **payload: Any,
    ) -> None:
        if self.state.trace_multi_short_repair_events is None:
            return
        event_seq = int(record.get("_event_seq", 0))
        event = {
            "event_schema_version": "narrowgate_lineage_event_journal.v2",
            "lineage_uid": str(record["lineage_uid"]),
            "lineage_id": int(record["lineage_id"]),
            "inventory_lifecycle_id": int(record["inventory_lifecycle_id"]),
            "day": str(record["day"]),
            "side": "SELL",
            "action": str(record["action"]),
            "event_seq": event_seq,
            "event_type": str(event_type),
            "event_ts_ms": int(event_ts_ms),
            "episode_index": 1,
        }
        event.update(payload)
        self.state.trace_multi_short_repair_events.append(event)
        record["_event_seq"] = event_seq + 1

    def _multi_short_repair_update_path(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        record = self._multi_short_repair_inventory_lifecycle_record()
        if record is None:
            return
        last_ts = int(record.get("last_path_ts_ms", now_ts_ms))
        dt_s = max(0.0, float(int(now_ts_ms) - last_ts) / 1_000.0)
        abs_inventory = abs(float(record.get("last_path_inventory", self.state.q)))
        record["inventory_time_btc_s"] = float(
            record.get("inventory_time_btc_s", 0.0)
        ) + abs_inventory * dt_s
        if int(record.get("treatment_active", 0)) == 1:
            record["active_window_inventory_time_btc_s"] = float(
                record.get("active_window_inventory_time_btc_s", 0.0)
            ) + abs_inventory * dt_s
        current_equity = float(self._inventory_lifecycle_pnl(mark_px))
        post_assignment_value = current_equity - float(
            record["assignment_equity_usdc"]
        )
        record["lineage_mae"] = min(
            float(record.get("lineage_mae", 0.0)),
            float(post_assignment_value),
        )
        record["lineage_max_abs_inventory"] = max(
            float(record.get("lineage_max_abs_inventory", 0.0)),
            abs(float(self.state.q)),
        )
        short_units = max(0.0, -float(self.state.q) / float(self.state.LOT_SIZE))
        record["max_short_inventory_units"] = max(
            float(record.get("max_short_inventory_units", 0.0)),
            short_units,
        )
        record["last_path_ts_ms"] = int(now_ts_ms)
        record["last_path_inventory"] = float(self.state.q)

    def _start_multi_short_repair_inventory_lifecycle(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> dict[str, Any]:
        if not self.state.multi_short_repair_enabled:
            raise RuntimeError("multi-SHORT reducing-BUY assignment is disabled")
        if not self.state.inventory_lifecycle_active or float(self.state.q) > self.state.multi_short_repair_trigger_inventory_btc + 1e-12:
            raise RuntimeError(
                "multi-SHORT reducing-BUY assignment requires a multi-level SHORT inventory_lifecycle"
            )
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        if inventory_lifecycle_id in self.state.multi_short_repair_assigned_inventory_lifecycles:
            raise RuntimeError(
                "multi-SHORT reducing-BUY contract forbids inventory_lifecycle rerandomization"
            )
        if self.state.multi_short_repair_open_record is not None:
            raise RuntimeError("a multi-SHORT reducing-BUY treatment is already open")
        if inventory_lifecycle_id in self.state.multi_short_repair_pending_inventory_lifecycle:
            raise RuntimeError("multi-SHORT reducing-BUY inventory_lifecycle is already pending")
        completed = len(self.state.trace_multi_short_repair or ())
        in_flight = completed + len(self.state.multi_short_repair_pending_inventory_lifecycle)
        if in_flight >= self.state.trace_multi_short_repair_max:
            raise RuntimeError(
                "trace_multi_short_reducing_buy_aggression_max exhausted before day end"
            )
        assignment_day = self.services.pd.to_datetime(
            int(now_ts_ms), unit="ms", utc=True
        ).strftime("%Y-%m-%d")
        prospective_id = int(self.state.multi_short_repair_next_id) + 1
        lineage_uid = (
            f"{self.services.SYMBOL}:{assignment_day}:MULTI_SHORT_REPAIR:{inventory_lifecycle_id}:"
            f"{int(now_ts_ms)}:{prospective_id}"
        )
        if self.state.multi_short_repair_randomizer is None:
            raise RuntimeError("multi-SHORT reducing-BUY randomizer is unavailable")
        assignment = self.state.multi_short_repair_randomizer.assign(
            utc_day=assignment_day,
            pre_assignment_inventory_lifecycle_uid=lineage_uid,
        )
        action = str(assignment.action)
        self.state.multi_short_repair_next_id = prospective_id
        self.state.multi_short_repair_assignment_counts[action] += 1
        self.state.multi_short_repair_assigned_inventory_lifecycles.add(inventory_lifecycle_id)
        assignment_equity = float(self._inventory_lifecycle_pnl(mark_px))
        record = {
            "schema_version": "narrowgate_lineage_randomized_trace.v2",
            "trace_schema_version": "narrowgate_lineage_randomized_trace.v2",
            "mechanism_extension_schema": "none",
            "decision_id": (
                f"multi_short_repair:{self.services.SYMBOL}:{assignment_day}:"
                f"{inventory_lifecycle_id}:{int(now_ts_ms)}"
            ),
            "lineage_uid": lineage_uid,
            "lineage_id": prospective_id,
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "day": assignment_day,
            "side": "SELL",
            "inventory_role": "multi_short_repair",
            "action": action,
            "behavior_propensity": float(
                self.state.multi_short_repair_probabilities[action]
            ),
            "behavior_prob_baseline_reducing_buy": float(
                self.state.multi_short_repair_probabilities[self.context.MULTI_SHORT_REPAIR_CONTROL]
            ),
            "behavior_prob_aggressive_maker_reducing_buy": float(
                self.state.multi_short_repair_probabilities[self.context.MULTI_SHORT_REPAIR_CANDIDATE]
            ),
            "assignment_u": float(assignment.uniform_draw),
            "random_seed": int(self.state.multi_short_repair_seed),
            "randomization_stratum": str(assignment.randomization_stratum),
            "assignment_ts_ms": int(now_ts_ms),
            "decision_ts_ms": int(now_ts_ms),
            "decision_ts_ns": int(now_ts_ms) * 1_000_000,
            "assignment_before_downstream_path": 1,
            "assignment_fixed_within_lineage": 1,
            "trigger_fill_excluded_from_reward": 1,
            "assignment_inventory_btc": float(self.state.q),
            "inventory_lifecycle_age_at_assignment_ms": max(
                0, int(now_ts_ms) - int(self.state.inventory_lifecycle_start_ts)
            ),
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_equity_usdc": float(self.state.inventory_lifecycle_start_pnl),
            "assignment_equity_usdc": assignment_equity,
            "lineage_terminal_equity_usdc": assignment_equity,
            "inventory_lifecycle_terminal_equity_usdc": assignment_equity,
            "pre_assignment_inventory_lifecycle_pnl_usdc": float(
                assignment_equity - self.state.inventory_lifecycle_start_pnl
            ),
            "lineage_reward_usdc": 0.0,
            "post_lineage_continuation_value_usdc": 0.0,
            "decision_to_inventory_lifecycle_terminal_value_usdc": 0.0,
            "accounting_identity_error_usdc": 0.0,
            "lineage_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_ts_ms": 0,
            "lineage_terminal_reason": "pending",
            "inventory_lifecycle_terminal_reason": "pending",
            "final_blocker": "not_observed",
            "final_action_change_ts_ms": 0,
            "final_action_change_status": "eligible_no_change",
            "treatment_active": 1,
            "treatment_release_ts_ms": 0,
            "treatment_release_inventory_btc": float("nan"),
            "eligible_reducing_buy_decision_count": 0,
            "candidate_quote_count": 0,
            "price_action_change_count": 0,
            "defense_pause_observation_count": 0,
            "defense_override_attempt_count": 0,
            "defense_override_effective_count": 0,
            "actual_final_action_change_count": 0,
            "maker_violation_count": 0,
            "action_generated_ioc_or_taker_count": 0,
            "order_submit_count": 0,
            "buy_order_submit_count": 0,
            "cancel_request_count": 0,
            "replace_cancel_request_count": 0,
            "queue_reset_count": 0,
            "intervention_fill_count": 0,
            "intervention_fill_qty": 0.0,
            "active_window_fill_count": 0,
            "reducing_buy_fill_count": 0,
            "reducing_buy_fill_qty": 0.0,
            "sell_exposure_fill_count": 0,
            "sell_exposure_fill_qty": 0.0,
            "entered_multilevel_short": 1,
            "max_short_inventory_units": max(0.0, -float(self.state.q) / float(self.state.LOT_SIZE)),
            "inventory_time_btc_s": 0.0,
            "active_window_inventory_time_btc_s": 0.0,
            "lineage_mae": 0.0,
            "lineage_max_abs_inventory": abs(float(self.state.q)),
            "last_path_ts_ms": int(now_ts_ms),
            "last_path_inventory": float(self.state.q),
            "queue_cost": 0.0,
            "support_valid": 1,
            "unsupported_reasons": "",
            "full_cpp_tick_replay_authority": 0,
            "daily_fresh_start": 1,
            "trigger_inventory_btc": float(
                self.state.multi_short_repair_trigger_inventory_btc
            ),
            "release_inventory_btc": float(
                self.state.multi_short_repair_release_inventory_btc
            ),
            "candidate_price_formula": (
                "min(best_ask-tick,max(baseline_reducing_buy,best_bid))"
            ),
            "defense_override_scope": "reducing_buy_defense_pause_only",
            "order_type": "GTX_maker_only",
            "reward_definition": (
                "assignment_to_inventory_lifecycle_terminal_equity_change_usdc"
            ),
            "queue_cost_definition": (
                "zero_separate_term_full_path_effect_embedded_in_terminal_reward_"
                "with_reset_counts_as_diagnostic"
            ),
            "_event_seq": 0,
        }
        self.state.multi_short_repair_open_record = record
        self._append_multi_short_repair_event(
            record,
            "assignment",
            int(now_ts_ms),
            assignment_u=float(assignment.uniform_draw),
            behavior_propensity=float(self.state.multi_short_repair_probabilities[action]),
            assignment_inventory_btc=float(self.state.q),
        )
        return record

    def _multi_short_repair_note_fill(self,
        side: str,
        prev_q: float,
        fill_qty: float,
    ) -> None:
        record = self._multi_short_repair_inventory_lifecycle_record()
        if record is None:
            return
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0)
        ) + 1
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0)
        ) + float(fill_qty)
        if int(record.get("treatment_active", 0)) == 1:
            record["active_window_fill_count"] = int(
                record.get("active_window_fill_count", 0)
            ) + 1
        exposure_increasing = abs(float(self.state.q)) > abs(float(prev_q)) + 1e-10
        reducing = abs(float(self.state.q)) < abs(float(prev_q)) - 1e-10
        normalized_side = str(side).upper()
        if normalized_side == "BUY" and reducing:
            record["reducing_buy_fill_count"] = int(
                record.get("reducing_buy_fill_count", 0)
            ) + 1
            record["reducing_buy_fill_qty"] = float(
                record.get("reducing_buy_fill_qty", 0.0)
            ) + float(fill_qty)
        if normalized_side == "SELL" and exposure_increasing:
            record["sell_exposure_fill_count"] = int(
                record.get("sell_exposure_fill_count", 0)
            ) + 1
            record["sell_exposure_fill_qty"] = float(
                record.get("sell_exposure_fill_qty", 0.0)
            ) + float(fill_qty)

    def _multi_short_repair_on_inventory_lifecycle_fill(self,
        side: str,
        prev_q: float,
        fill_qty: float,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        if not self.state.multi_short_repair_enabled:
            return
        existing = self._multi_short_repair_inventory_lifecycle_record()
        if existing is not None:
            self._multi_short_repair_note_fill(side, prev_q, fill_qty)
            return
        if int(self.state.inventory_lifecycle_count) in self.state.multi_short_repair_assigned_inventory_lifecycles:
            return
        if str(side).upper() != "SELL":
            return
        if self.context.multi_short_repair_should_start is None:
            raise RuntimeError("multi-SHORT reducing-BUY trigger kernel is unavailable")
        crossed_trigger = bool(
            float(prev_q) > self.state.multi_short_repair_trigger_inventory_btc + 1e-12
            and self.context.multi_short_repair_should_start(
                float(self.state.q),
                trigger_inventory_btc=self.state.multi_short_repair_trigger_inventory_btc,
            )
        )
        if crossed_trigger:
            self._start_multi_short_repair_inventory_lifecycle(int(now_ts_ms), float(mark_px))

    def _terminalize_multi_short_repair_lineage(self,
        outcome_ts_ms: int,
        mark_px: float,
        *,
        terminal_reason: str,
        censored: bool,
    ) -> dict[str, Any] | None:
        record = self.state.multi_short_repair_open_record
        if record is None:
            return None
        self._multi_short_repair_update_path(int(outcome_ts_ms), float(mark_px))
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        assignment_equity = float(record["assignment_equity_usdc"])
        record.update(
            {
                "treatment_active": 0,
                "treatment_release_ts_ms": int(outcome_ts_ms),
                "treatment_release_inventory_btc": float(self.state.q),
                "lineage_terminal_ts_ms": int(outcome_ts_ms),
                "lineage_terminal_equity_usdc": terminal_equity,
                "lineage_terminal_reason": str(terminal_reason),
                "lineage_reward_usdc": float(terminal_equity - assignment_equity),
                "lineage_censored": int(bool(censored)),
            }
        )
        self._append_multi_short_repair_event(
            record,
            "lineage_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            censored=int(bool(censored)),
            terminal_inventory_btc=float(self.state.q),
        )
        inventory_lifecycle_id = int(record["inventory_lifecycle_id"])
        self.state.multi_short_repair_pending_inventory_lifecycle[inventory_lifecycle_id] = record
        self.state.multi_short_repair_open_record = None
        return record

    def _maybe_release_multi_short_repair(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        record = self.state.multi_short_repair_open_record
        if record is None:
            return
        if self.context.multi_short_repair_should_end is None:
            raise RuntimeError("multi-SHORT reducing-BUY release kernel is unavailable")
        if self.context.multi_short_repair_should_end(
            float(self.state.q),
            release_inventory_btc=self.state.multi_short_repair_release_inventory_btc,
        ):
            self._terminalize_multi_short_repair_lineage(
                int(now_ts_ms),
                float(mark_px),
                terminal_reason="inventory_recovered_to_release_threshold",
                censored=False,
            )

    def _multi_short_repair_candidate_active(self) -> bool:
        record = self.state.multi_short_repair_open_record
        return bool(
            record is not None
            and int(record.get("inventory_lifecycle_id", -1)) == int(self.state.inventory_lifecycle_count)
            and int(record.get("treatment_active", 0)) == 1
            and str(record.get("action", "")) == self.context.MULTI_SHORT_REPAIR_CANDIDATE
            and float(self.state.q) < self.state.multi_short_repair_release_inventory_btc - 1e-12
        )

    def _apply_multi_short_repair_quote(self,
        now_ts_ms: int,
        *,
        baseline_bid: float,
        best_bid: float,
        best_ask: float,
        mark_px: float,
        can_post: bool,
        defense_pause_observed: bool,
        defense_override_requested: bool,
        side_ctx: dict[str, Any],
    ) -> tuple[float, bool]:
        record = self.state.multi_short_repair_open_record
        if (
            record is None
            or int(record.get("inventory_lifecycle_id", -1)) != int(self.state.inventory_lifecycle_count)
            or int(record.get("treatment_active", 0)) != 1
        ):
            return float(baseline_bid), False
        record["eligible_reducing_buy_decision_count"] = int(
            record.get("eligible_reducing_buy_decision_count", 0)
        ) + 1
        record["defense_pause_observation_count"] = int(
            record.get("defense_pause_observation_count", 0)
        ) + int(bool(defense_pause_observed))
        record["defense_override_attempt_count"] = int(
            record.get("defense_override_attempt_count", 0)
        ) + int(bool(defense_override_requested))
        side_ctx["multi_short_reducing_buy_aggression_lineage_id"] = int(
            record["lineage_id"]
        )
        side_ctx["multi_short_reducing_buy_aggression_action"] = str(
            record["action"]
        )
        if str(record["action"]) != self.context.MULTI_SHORT_REPAIR_CANDIDATE:
            return float(baseline_bid), False
        if not bool(can_post):
            return float(baseline_bid), False
        if self.context.aggressive_maker_buy_price is None:
            raise RuntimeError("multi-SHORT reducing-BUY quote kernel is unavailable")
        quote = self.context.aggressive_maker_buy_price(
            baseline_price=float(baseline_bid),
            best_bid=float(best_bid),
            best_ask=float(best_ask),
            tick_size=float(self.state.TICK),
            mark_price=float(mark_px),
        )
        record["candidate_quote_count"] = int(
            record.get("candidate_quote_count", 0)
        ) + 1
        record["requested_price_improvement_ticks_sum"] = float(
            record.get("requested_price_improvement_ticks_sum", 0.0)
        ) + max(0.0, float(quote.improvement_ticks))
        record["requested_price_improvement_bps_sum"] = float(
            record.get("requested_price_improvement_bps_sum", 0.0)
        ) + max(0.0, float(quote.improvement_bps))
        record["maker_violation_count"] = int(
            record.get("maker_violation_count", 0)
        ) + int(not quote.maker_valid)
        if not quote.maker_valid:
            raise RuntimeError("multi-SHORT reducing-BUY candidate violated GTX maker bound")
        changed = bool(quote.changed)
        record["price_action_change_count"] = int(
            record.get("price_action_change_count", 0)
        ) + int(changed)
        side_ctx.update(
            {
                "multi_short_reducing_buy_baseline_price": float(
                    quote.baseline_price
                ),
                "multi_short_reducing_buy_selected_price": float(
                    quote.selected_price
                ),
                "multi_short_reducing_buy_improvement_ticks": float(
                    quote.improvement_ticks
                ),
                "multi_short_reducing_buy_improvement_bps": float(
                    quote.improvement_bps
                ),
                "multi_short_reducing_buy_defense_override_requested": bool(
                    defense_override_requested
                ),
                "multi_short_reducing_buy_maker_valid": bool(quote.maker_valid),
            }
        )
        return float(quote.selected_price), changed

    def _multi_short_repair_record_final_decision(self,
        now_ts_ms: int,
        *,
        can_post: bool,
        bid_action: str,
        price_changed: bool,
        defense_override_requested: bool,
        block_reasons: list[str],
    ) -> None:
        record = self.state.multi_short_repair_open_record
        if (
            record is None
            or str(record.get("action", "")) != self.context.MULTI_SHORT_REPAIR_CANDIDATE
        ):
            return
        defense_effective = bool(
            defense_override_requested
            and can_post
            and str(bid_action)
            in {"place", "replace", "cancel_first", "pending_coalesce", "keep"}
        )
        record["defense_override_effective_count"] = int(
            record.get("defense_override_effective_count", 0)
        ) + int(defense_effective)
        price_effective = bool(
            price_changed and str(bid_action) in {"place", "replace", "cancel_first"}
        )
        changed = bool(defense_effective or price_effective)
        if changed:
            record["final_blocker"] = "none"
            record["actual_final_action_change_count"] = int(
                record.get("actual_final_action_change_count", 0)
            ) + 1
            if int(record.get("final_action_change_ts_ms", 0) or 0) <= 0:
                record["final_action_change_ts_ms"] = int(now_ts_ms)
                record["final_action_change_status"] = "observed"
                self._append_multi_short_repair_event(
                    record,
                    "action_change",
                    int(now_ts_ms),
                    bid_action=str(bid_action),
                    price_changed=int(price_effective),
                    defense_override=int(defense_effective),
                )
        elif not bool(can_post):
            record["final_blocker"] = "|".join(block_reasons) or "unknown_blocker"

    def _finalize_multi_short_repair_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        outcome_ts_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        record = self.state.multi_short_repair_pending_inventory_lifecycle.get(int(inventory_lifecycle_id))
        if (
            record is None
            and self.state.multi_short_repair_open_record is not None
            and int(self.state.multi_short_repair_open_record.get("inventory_lifecycle_id", -1))
            == int(inventory_lifecycle_id)
        ):
            record = self._terminalize_multi_short_repair_lineage(
                int(outcome_ts_ms),
                float(mark_px),
                terminal_reason=str(terminal_reason),
                censored=not bool(closed),
            )
        if record is None:
            return
        self._multi_short_repair_update_path(int(outcome_ts_ms), float(mark_px))
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        assignment_equity = float(record["assignment_equity_usdc"])
        inventory_lifecycle_start_equity = float(record["inventory_lifecycle_start_equity_usdc"])
        lineage_terminal_equity = float(record["lineage_terminal_equity_usdc"])
        lineage_reward = lineage_terminal_equity - assignment_equity
        continuation = terminal_equity - lineage_terminal_equity
        decision_to_terminal = terminal_equity - assignment_equity
        identity_error = decision_to_terminal - lineage_reward - continuation
        record.update(
            {
                "inventory_lifecycle_terminal_ts_ms": int(outcome_ts_ms),
                "inventory_lifecycle_terminal_equity_usdc": terminal_equity,
                "inventory_lifecycle_terminal_reason": str(terminal_reason),
                "pre_assignment_inventory_lifecycle_pnl_usdc": float(
                    assignment_equity - inventory_lifecycle_start_equity
                ),
                "lineage_reward_usdc": float(lineage_reward),
                "post_lineage_continuation_value_usdc": float(continuation),
                "decision_to_inventory_lifecycle_terminal_value_usdc": float(
                    decision_to_terminal
                ),
                "accounting_identity_error_usdc": float(identity_error),
                "reward": float(decision_to_terminal),
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_equity - inventory_lifecycle_start_equity
                ),
                "multilevel_short_terminal_value_usdc": float(
                    decision_to_terminal
                ),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "repair_event": int(bool(closed) and str(terminal_reason) == "flat"),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "decision_to_terminal_s": max(
                    0.0,
                    float(int(outcome_ts_ms) - int(record["assignment_ts_ms"]))
                    / 1_000.0,
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(int(outcome_ts_ms) - int(record["inventory_lifecycle_start_ts_ms"]))
                    / 1_000.0,
                ),
                "support_valid": int(bool(record.get("support_valid", 1))),
            }
        )
        if int(record.get("final_action_change_ts_ms", 0) or 0) <= 0:
            record["final_action_change_status"] = "eligible_no_change"
        self._append_multi_short_repair_event(
            record,
            "inventory_lifecycle_terminal",
            int(outcome_ts_ms),
            terminal_reason=str(terminal_reason),
            closed=int(bool(closed)),
        )
        record.pop("_event_seq", None)
        if self.state.trace_multi_short_repair is None:
            raise RuntimeError("multi-SHORT reducing-BUY trace is unavailable")
        self.state.trace_multi_short_repair.append(record)
        self.state.multi_short_repair_pending_inventory_lifecycle.pop(int(inventory_lifecycle_id), None)

    def _update_sell_add_skip_competing_risk(self,
        t_ms: int,
        mark_px: float,
    ) -> None:
        """Record the first side-aware quote-level trend-through event."""

        if not self.state.sell_add_skip_open_records or abs(self.state.q) < 1e-10:
            return
        for record in self.state.sell_add_skip_open_records.values():
            decision_mid = float(record.get("decision_mid", 0.0) or 0.0)
            if decision_mid <= 0.0:
                continue
            side = str(record.get("side", "SELL")).upper()
            signed_adverse = (
                decision_mid - float(mark_px)
                if side == "BUY"
                else float(mark_px) - decision_mid
            )
            adverse_bps = max(
                0.0,
                10_000.0 * signed_adverse / max(decision_mid, 1e-12),
            )
            record["max_adverse_move_bps"] = max(
                float(record.get("max_adverse_move_bps", 0.0) or 0.0),
                adverse_bps,
            )
            if str(record.get("competing_event", "pending")) != "pending":
                continue
            trend_through_price = float(record["trend_through_price"])
            trend_through = (
                float(mark_px) <= trend_through_price + 1e-12
                if side == "BUY"
                else float(mark_px) + 1e-12 >= trend_through_price
            )
            if not trend_through:
                continue
            record.update(
                {
                    "competing_event": "trend_through",
                    "competing_event_code": 2,
                    "competing_event_ts_ms": int(t_ms),
                    "competing_event_time_s": max(
                        0.0,
                        float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                    ),
                    "trend_through_first": 1,
                    "repair_first": 0,
                }
            )

    def _inventory_lifecycle_update_path(self, t_ms: int, mark_px: float) -> None:
        if not self.state.inventory_lifecycle_active:
            return
        timestamp = int(t_ms)
        if self.state.inventory_lifecycle_inventory_time_last_ts_ms <= 0:
            self.state.inventory_lifecycle_inventory_time_last_ts_ms = timestamp
            self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(self.state.q)
        if timestamp < self.state.inventory_lifecycle_inventory_time_last_ts_ms:
            raise RuntimeError("inventory_lifecycle inventory-time clock regressed")
        self.state.inventory_lifecycle_inventory_time_to_date_btc_s += (
            abs(float(self.state.inventory_lifecycle_inventory_time_last_inventory_btc))
            * float(timestamp - self.state.inventory_lifecycle_inventory_time_last_ts_ms)
            / 1_000.0
        )
        self.state.inventory_lifecycle_inventory_time_last_ts_ms = timestamp
        self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(self.state.q)
        pnl_delta = self._inventory_lifecycle_pnl(mark_px) - self.state.inventory_lifecycle_start_pnl
        self.state.inventory_lifecycle_cur_max_abs = max(self.state.inventory_lifecycle_cur_max_abs, abs(self.state.q))
        self.state.inventory_lifecycle_cur_min_pnl = min(self.state.inventory_lifecycle_cur_min_pnl, pnl_delta)
        self.state.inventory_lifecycle_max_abs_inventory = max(self.state.inventory_lifecycle_max_abs_inventory, self.state.inventory_lifecycle_cur_max_abs)
        self.state.inventory_lifecycle_max_duration_s = max(
            self.state.inventory_lifecycle_max_duration_s,
            max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0),
        )
        self.state.inventory_lifecycle_max_adverse_excursion = min(
            self.state.inventory_lifecycle_max_adverse_excursion,
            self.state.inventory_lifecycle_cur_min_pnl,
        )
        self._update_sell_add_skip_competing_risk(t_ms, mark_px)
        self._sell_add_price_penalty_update_path(t_ms, mark_px)
        self._multi_short_repair_update_path(t_ms, mark_px)

    def _finalize_local_action_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_local_action_ope is None:
            return
        record = self.state.local_action_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        # This residual is an accounting decomposition, not a separately
        # identified causal cost model. Randomized action assignment identifies
        # reward; the components explain how that reward travelled through fills
        # and the remainder of the inventory inventory_lifecycle.
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0, float(t_ms - int(record["inventory_lifecycle_start_ts_ms"])) / 1000.0
                ),
                "decision_to_terminal_s": max(
                    0.0, float(t_ms - int(record["decision_ts_ms"])) / 1000.0
                ),
                "repair_event": int(bool(closed)),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
            }
        )
        if len(self.state.trace_local_action_ope) < self.state.trace_local_action_ope_max:
            self.state.trace_local_action_ope.append(record)

    def _finalize_queue_value_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_queue_value_keep_cancel is None:
            return
        record = self.state.queue_value_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        side = str(record["side"])
        if self.state.queue_value_active_episode_by_side.get(side) == int(inventory_lifecycle_id):
            self.state.queue_value_active_episode_by_side.pop(side, None)
        if closed:
            record["repair_ts_ns"] = int(t_ms) * 1_000_000
        if str(record.get("first_event", "pending")) == "pending":
            if closed:
                self._queue_value_set_first_event(
                    record,
                    "inventory_lifecycle_repair",
                    int(t_ms),
                )
            else:
                self._queue_value_set_first_event(record, "censored", int(t_ms))
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        first_event = str(record.get("first_event", "censored"))
        event_code = {
            "favorable_fill": 0,
            "adverse_fill": 1,
            "cancel": 2,
            "adverse_price_jump": 3,
            "inventory_lifecycle_repair": 4,
            "censored": 5,
        }[first_event]
        fill_qty = float(record.get("intervention_fill_qty", 0.0) or 0.0)
        markout_sum = float(
            record.get("fill_markout_qty_weighted_bps_sum", 0.0) or 0.0
        )
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "censor_ts_ns": int(t_ms) * 1_000_000,
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(t_ms - int(record["inventory_lifecycle_start_ts_ms"]))
                    / 1_000.0,
                ),
                "decision_to_terminal_s": max(
                    0.0,
                    float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                ),
                "repair_event": int(bool(closed)),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "first_event_code": int(event_code),
                "event_observed": int(first_event != "censored"),
                "fill_value_markout_bps": (
                    markout_sum / fill_qty if fill_qty > 1e-12 else self.services.math.nan
                ),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
            }
        )
        for event in (
            "favorable_fill",
            "adverse_fill",
            "cancel",
            "adverse_price_jump",
            "inventory_lifecycle_repair",
            "censored",
        ):
            record[f"event_{event}"] = int(first_event == event)
        record.pop("_exchange_book_trade_since_update_qty", None)
        if (
            len(self.state.trace_queue_value_keep_cancel)
            < self.state.trace_queue_value_keep_cancel_max
        ):
            self.state.trace_queue_value_keep_cancel.append(record)

    def _finalize_sell_add_skip_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_sell_add_skip_ope is None:
            return
        record = self.state.sell_add_skip_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        if str(record.get("competing_event", "pending")) == "pending":
            event = "repair" if closed else "censored"
            record.update(
                {
                    "competing_event": event,
                    "competing_event_code": 1 if closed else 0,
                    "competing_event_ts_ms": int(t_ms),
                    "competing_event_time_s": max(
                        0.0,
                        float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                    ),
                    "trend_through_first": 0,
                    "repair_first": int(bool(closed)),
                }
            )
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(t_ms - int(record["inventory_lifecycle_start_ts_ms"]))
                    / 1_000.0,
                ),
                "decision_to_terminal_s": max(
                    0.0,
                    float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                ),
                "repair_event": int(bool(closed)),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
            }
        )
        if len(self.state.trace_sell_add_skip_ope) < self.state.trace_sell_add_skip_ope_max:
            self.state.trace_sell_add_skip_ope.append(record)

    def _finalize_state_conditioned_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_state_conditioned_policy is None:
            return
        record = self.state.state_conditioned_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(t_ms - int(record["inventory_lifecycle_start_ts_ms"])) / 1000.0,
                ),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
            }
        )
        if len(self.state.trace_state_conditioned_policy) < self.state.trace_state_conditioned_policy_max:
            self.state.trace_state_conditioned_policy.append(record)

    def _finalize_safe_add_rearm_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_safe_add_rearm_intervention is None:
            return
        record = self.state.safe_add_rearm_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        fill_qty = float(record.get("intervention_fill_qty", 0.0) or 0.0)
        markout_sum = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0) or 0.0
        )
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(t_ms - int(record["inventory_lifecycle_start_ts_ms"])) / 1000.0,
                ),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "fill_markout_30s_bps": (
                    markout_sum / fill_qty if fill_qty > 1e-12 else self.services.math.nan
                ),
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
            }
        )
        if (
            len(self.state.trace_safe_add_rearm_intervention)
            < self.state.trace_safe_add_rearm_intervention_max
        ):
            self.state.trace_safe_add_rearm_intervention.append(record)

    def _finalize_state_conditioned_rearm_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_state_conditioned_rearm is None:
            return
        record = self.state.state_conditioned_rearm_open_records.pop(
            int(inventory_lifecycle_id), None
        )
        if record is None:
            return
        side = str(record.get("side", ""))
        if self.state.state_conditioned_rearm_active_episode_by_side.get(side) == int(
            inventory_lifecycle_id
        ):
            self.state.state_conditioned_rearm_active_episode_by_side.pop(side, None)
        terminal_mtm = float(self._inventory_lifecycle_pnl(mark_px))
        decision_mtm = float(record["decision_mtm"])
        reward = terminal_mtm - decision_mtm
        fill_value = float(record.get("fill_value", 0.0) or 0.0)
        queue_cost = float(record.get("queue_cost", 0.0) or 0.0)
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        fill_qty = float(record.get("intervention_fill_qty", 0.0) or 0.0)
        markout_sum = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0) or 0.0
        )
        record.update(
            {
                "outcome_ts_ms": int(t_ms),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "terminal_reason": str(terminal_reason),
                "terminal_mtm": terminal_mtm,
                "terminal_inventory_lifecycle_pnl": float(
                    terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                ),
                "inventory_lifecycle_duration_s": max(
                    0.0,
                    float(t_ms - int(record["inventory_lifecycle_start_ts_ms"]))
                    / 1_000.0,
                ),
                "decision_to_outcome_s": max(
                    0.0,
                    float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                ),
                "time_to_repair_s": (
                    max(
                        0.0,
                        float(t_ms - int(record["decision_ts_ms"])) / 1_000.0,
                    )
                    if closed
                    else self.services.math.nan
                ),
                "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                "reward": float(reward),
                "fill_value": fill_value,
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": queue_cost,
                "fill_markout_30s_bps": (
                    markout_sum / fill_qty if fill_qty > 1e-12 else self.services.math.nan
                ),
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
                "state_episode_open_at_outcome": int(
                    bool(record.get("state_episode_active", 0))
                ),
            }
        )
        if len(self.state.trace_state_conditioned_rearm) < self.state.trace_state_conditioned_rearm_max:
            self.state.trace_state_conditioned_rearm.append(record)

    def _record_first_add_decision_fill(self,
        order: dict[str, Any],
        side: str,
        *,
        inventory_before_fill: float,
        fill_qty: float,
        fill_ts_ms: int,
        fill_price: float,
    ) -> None:
        """Open one exact decision-to-terminal diagnostic per add inventory_lifecycle."""

        if self.state.trace_first_add_decision_to_terminal is None:
            return
        inventory_after_fill = (
            float(inventory_before_fill) + float(fill_qty)
            if str(side).upper() == "BUY"
            else float(inventory_before_fill) - float(fill_qty)
        )
        exposure_increasing = (
            abs(float(inventory_before_fill)) > 1e-10
            and abs(inventory_after_fill)
            > abs(float(inventory_before_fill)) + 1e-10
        )
        decision_role = str(order.get("inventory_role_at_submit", ""))
        if (
            not exposure_increasing
            or not self.state.inventory_lifecycle_active
            or decision_role != "add"
        ):
            return
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        if (
            inventory_lifecycle_id in self.state.first_add_decision_recorded_inventory_lifecycles
            or inventory_lifecycle_id in self.state.first_add_decision_open_records
        ):
            return
        in_flight = len(self.state.trace_first_add_decision_to_terminal) + len(
            self.state.first_add_decision_open_records
        )
        if in_flight >= self.state.trace_first_add_decision_to_terminal_max:
            raise RuntimeError(
                "trace_first_add_decision_to_terminal_max exhausted before "
                "the replay denominator was complete"
            )

        submit_ts_ms = int(order.get("submit_ts", order.get("quote_ts", 0)) or 0)
        order_id = int(order.get("trace_id", -1))
        if order_id < 0 or submit_ts_ms <= 0:
            raise RuntimeError("first-add trace requires an exact order decision identity")
        feature_ready_ts_ms = int(
            order.get("decision_visible_feature_ready_ts_max_ms", submit_ts_ms)
            or submit_ts_ms
        )
        if feature_ready_ts_ms > submit_ts_ms:
            raise RuntimeError("first-add trace exposed a future decision feature")
        decision_equity = float(
            order.get(
                "decision_visible_equity_usdc",
                order["decision_equity_at_submit"],
            )
        )
        inventory_lifecycle_start_equity = float(order["inventory_lifecycle_start_equity_at_submit"])
        decision_mid = float(
            order.get(
                "decision_visible_mid_usdc_per_btc",
                order.get("mid_at_quote", fill_price),
            )
            or fill_price
        )
        decision_price = float(order.get("price", fill_price) or fill_price)
        quote_distance_ticks = (
            max(0.0, decision_mid - decision_price)
            if str(side).upper() == "BUY"
            else max(0.0, decision_price - decision_mid)
        ) / max(float(self.state.TICK), 1e-12)
        diag = order.get("quote_diag") or {}
        record = {
            "trace_schema_version": "first_add_decision_to_terminal_trace.v1",
            "day": self.services.pd.to_datetime(
                submit_ts_ms, unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "quality_grade": str(
                self.context.params.get("first_add_trace_quality_grade", "") or ""
            ),
            "decision_id": (
                f"first_add_decision:{self.services.SYMBOL}:{submit_ts_ms}:{order_id}"
            ),
            "order_id": order_id,
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": str(side).upper(),
            "inventory_role": "add",
            "decision_ts_ms": submit_ts_ms,
            "order_submit_ts_ms": submit_ts_ms,
            "order_activation_ts_ms": int(order.get("activate_ts", 0) or 0),
            "decision_visible_feature_ready_ts_max_ms": feature_ready_ts_ms,
            "fill_ts_ms": int(fill_ts_ms),
            "fill_price": float(fill_price),
            "first_add_fill_qty_btc": float(fill_qty),
            "exact_decision_order_fill_join": 1,
            "decision_equity_usdc": decision_equity,
            "inventory_lifecycle_start_equity_usdc": inventory_lifecycle_start_equity,
            "pre_decision_inventory_lifecycle_pnl_usdc": float(
                decision_equity - inventory_lifecycle_start_equity
            ),
            "inventory_btc": float(order.get("inventory_at_submit", 0.0) or 0.0),
            "inventory_lifecycle_age_ms": max(
                0,
                submit_ts_ms
                - int(order.get("inventory_lifecycle_start_ts_at_submit", submit_ts_ms) or submit_ts_ms),
            ),
            "inventory_lifecycle_pnl_so_far_usdc": float(
                order.get("inventory_lifecycle_pnl_so_far_at_submit", 0.0) or 0.0
            ),
            "inventory_lifecycle_mae_so_far_usdc": float(
                order.get("inventory_lifecycle_mae_so_far_at_submit", 0.0) or 0.0
            ),
            "exposure_increasing_fill_count_so_far": int(
                order.get("inventory_lifecycle_exposure_increasing_fills_at_submit", 0) or 0
            ),
            "reducing_fill_count_so_far": int(
                order.get("inventory_lifecycle_reducing_fills_at_submit", 0) or 0
            ),
            "quote_distance_ticks": float(quote_distance_ticks),
            "queue_ahead_btc": float(
                order.get("decision_queue_ahead_btc", self.services.math.nan)
            ),
            "queue_ahead_available": int(
                self.services.math.isfinite(
                    float(order.get("decision_queue_ahead_btc", self.services.math.nan))
                )
            ),
            "queue_ahead_source": str(
                order.get("decision_queue_ahead_source", "unavailable")
            ),
            "queue_ahead_asof_ts_ns": int(
                order.get("decision_queue_ahead_asof_ts_ns", 0) or 0
            ),
            "weighted_mid_proxy_shift_bps": float(
                diag.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                diag.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                diag.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "local_toxicity": float(
                order.get("decision_local_toxicity", 0.0) or 0.0
            ),
            "parent_aggtrade_flow_imbalance": float(
                order.get("decision_parent_aggtrade_flow_imbalance", 0.0)
                or 0.0
            ),
            "inventory_lifecycle_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_equity_usdc": decision_equity,
            "decision_to_inventory_lifecycle_terminal_value_usdc": 0.0,
            "inventory_lifecycle_terminal_reason": "pending",
            "inventory_lifecycle_closed": 0,
            "inventory_lifecycle_censored": 0,
            "inventory_lifecycle_max_abs_inventory_btc": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_mae_usdc": float(self.state.inventory_lifecycle_cur_min_pnl),
        }
        self.state.first_add_decision_recorded_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.first_add_decision_open_records[inventory_lifecycle_id] = record

    def _finalize_first_add_decision_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_first_add_decision_to_terminal is None:
            return
        record = self.state.first_add_decision_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        decision_equity = float(record["decision_equity_usdc"])
        record.update(
            {
                "inventory_lifecycle_terminal_ts_ms": int(t_ms),
                "inventory_lifecycle_terminal_equity_usdc": terminal_equity,
                "decision_to_inventory_lifecycle_terminal_value_usdc": float(
                    terminal_equity - decision_equity
                ),
                "inventory_lifecycle_terminal_reason": str(terminal_reason),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "inventory_lifecycle_max_abs_inventory_btc": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae_usdc": float(self.state.inventory_lifecycle_cur_min_pnl),
            }
        )
        self.state.trace_first_add_decision_to_terminal.append(record)

    def _record_first_opener_decision_fill(self,
        order: dict[str, Any],
        side: str,
        *,
        inventory_before_fill: float,
        fill_qty: float,
        fill_ts_ms: int,
        fill_price: float,
    ) -> None:
        """Open one exact decision-to-terminal diagnostic per new inventory_lifecycle."""

        if self.state.trace_first_opener_decision_to_terminal is None:
            return
        side_upper = str(side).upper()
        inventory_after_fill = (
            float(inventory_before_fill) + float(fill_qty)
            if side_upper == "BUY"
            else float(inventory_before_fill) - float(fill_qty)
        )
        opens_inventory_lifecycle = (
            not self.state.inventory_lifecycle_active
            and abs(float(inventory_before_fill)) <= 1e-10
            and abs(inventory_after_fill) > 1e-10
        )
        decision_role = str(order.get("inventory_role_at_submit", ""))
        if not opens_inventory_lifecycle:
            return

        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count) + 1
        if decision_role != "opener":
            self.state.first_opener_decision_unsupported_inventory_lifecycles.append(
                {
                    "inventory_lifecycle_id": inventory_lifecycle_id,
                    "side": side_upper,
                    "submit_inventory_role": decision_role,
                    "inventory_at_submit_btc": float(
                        order.get("inventory_at_submit", self.services.math.nan)
                    ),
                    "inventory_before_fill_btc": float(inventory_before_fill),
                    "inventory_after_fill_btc": float(inventory_after_fill),
                    "order_state_at_fill": str(order.get("state", "")),
                    "cancel_request_ts_ms": int(
                        order.get("cancel_request_ts", -1) or -1
                    ),
                    "submit_ts_ms": int(
                        order.get("submit_ts", order.get("quote_ts", 0)) or 0
                    ),
                    "fill_ts_ms": int(fill_ts_ms),
                }
            )
            return
        if (
            inventory_lifecycle_id in self.state.first_opener_decision_recorded_inventory_lifecycles
            or inventory_lifecycle_id in self.state.first_opener_decision_open_records
        ):
            return
        in_flight = len(self.state.trace_first_opener_decision_to_terminal) + len(
            self.state.first_opener_decision_open_records
        )
        if in_flight >= self.state.trace_first_opener_decision_to_terminal_max:
            raise RuntimeError(
                "trace_first_opener_decision_to_terminal_max exhausted before "
                "the replay denominator was complete"
            )

        submit_ts_ms = int(order.get("submit_ts", order.get("quote_ts", 0)) or 0)
        activation_ts_ms = int(order.get("activate_ts", 0) or 0)
        order_id = int(order.get("trace_id", -1))
        if order_id < 0 or submit_ts_ms <= 0:
            raise RuntimeError(
                "first-opener trace requires an exact order decision identity"
            )
        if activation_ts_ms <= 0 or activation_ts_ms > int(fill_ts_ms):
            raise RuntimeError(
                "first-opener trace requires activation before the matched fill"
            )
        feature_ready_ts_ms = int(
            order.get("decision_visible_feature_ready_ts_max_ms", submit_ts_ms)
            or submit_ts_ms
        )
        if feature_ready_ts_ms > submit_ts_ms:
            raise RuntimeError("first-opener trace exposed a future decision feature")

        decision_equity = float(
            order.get(
                "decision_visible_equity_usdc",
                order["decision_equity_at_submit"],
            )
        )
        inventory_lifecycle_start_equity = float(
            order.get("inventory_lifecycle_start_equity_at_submit", decision_equity)
        )
        if abs(decision_equity - inventory_lifecycle_start_equity) > 1e-9:
            raise RuntimeError(
                "first-opener decision must begin from a flat inventory_lifecycle boundary"
            )
        decision_mid = float(
            order.get(
                "decision_visible_mid_usdc_per_btc",
                order.get("mid_at_quote", fill_price),
            )
            or fill_price
        )
        decision_price = float(order.get("price", fill_price) or fill_price)
        quote_distance_ticks = (
            max(0.0, decision_mid - decision_price)
            if side_upper == "BUY"
            else max(0.0, decision_price - decision_mid)
        ) / max(float(self.state.TICK), 1e-12)
        diag = order.get("quote_diag") or {}
        record = {
            "trace_schema_version": self.state.first_opener_trace_schema_version,
            "day": self.services.pd.to_datetime(
                submit_ts_ms, unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "quality_grade": str(
                self.context.params.get("first_opener_trace_quality_grade", "") or ""
            ),
            "decision_id": (
                f"first_opener_decision:{self.services.SYMBOL}:{submit_ts_ms}:{order_id}"
            ),
            "order_id": order_id,
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side_upper,
            "inventory_role": "opener",
            "decision_ts_ms": submit_ts_ms,
            "order_submit_ts_ms": submit_ts_ms,
            "order_activation_ts_ms": activation_ts_ms,
            "decision_visible_feature_ready_ts_max_ms": feature_ready_ts_ms,
            "fill_ts_ms": int(fill_ts_ms),
            "fill_price": float(fill_price),
            "first_opener_fill_qty_btc": float(fill_qty),
            "remaining_qty_before_fill_btc": float(
                order.get("remaining", 0.0) + float(fill_qty)
            ),
            "remaining_qty_after_fill_btc": float(
                order.get("remaining", 0.0) or 0.0
            ),
            "cumulative_order_fill_qty_btc": float(
                float(order.get("quantity", fill_qty) or fill_qty)
                - float(order.get("remaining", 0.0) or 0.0)
            ),
            "first_fill_is_partial": int(
                float(order.get("remaining", 0.0) or 0.0) >= float(self.state.LOT_SIZE)
            ),
            "fill_order_state_before_terminal": str(
                order.get("state", self.state.ORDER_OPEN)
            ),
            "fill_while_cancel_pending": int(
                order.get("state") == self.state.ORDER_PENDING_CANCEL
            ),
            "cancel_request_ts_ms": int(
                order.get("cancel_request_ts", -1) or -1
            ),
            "cancel_reason": str(order.get("cancel_reason", "") or ""),
            "order_age_to_fill_ms": max(0, int(fill_ts_ms) - submit_ts_ms),
            "active_age_to_fill_ms": max(0, int(fill_ts_ms) - activation_ts_ms),
            "exact_decision_order_fill_join": 1,
            "decision_equity_usdc": decision_equity,
            "inventory_lifecycle_start_equity_usdc": inventory_lifecycle_start_equity,
            "pre_decision_inventory_lifecycle_pnl_usdc": 0.0,
            "inventory_btc": float(order.get("inventory_at_submit", 0.0) or 0.0),
            "inventory_before_fill_btc": float(inventory_before_fill),
            "inventory_after_fill_btc": float(inventory_after_fill),
            "inventory_lifecycle_opened_by_fill": 1,
            "decision_mid_usdc_per_btc": decision_mid,
            "decision_best_bid_usdc_per_btc": float(
                order.get(
                    "decision_visible_best_bid_usdc_per_btc",
                    order.get("best_bid_at_submit", self.services.math.nan),
                )
            ),
            "decision_best_ask_usdc_per_btc": float(
                order.get(
                    "decision_visible_best_ask_usdc_per_btc",
                    order.get("best_ask_at_submit", self.services.math.nan),
                )
            ),
            "order_price_usdc_per_btc": decision_price,
            "quote_distance_ticks": float(quote_distance_ticks),
            "decision_spread_ticks": float(
                max(
                    0.0,
                    float(
                        order.get(
                            "decision_visible_best_ask_usdc_per_btc",
                            order.get("best_ask_at_submit", self.services.math.nan),
                        )
                    )
                    - float(
                        order.get(
                            "decision_visible_best_bid_usdc_per_btc",
                            order.get("best_bid_at_submit", self.services.math.nan),
                        )
                    ),
                )
                / max(float(self.state.TICK), 1e-12)
            ),
            "queue_ahead_btc": float(
                order.get("decision_queue_ahead_btc", self.services.math.nan)
            ),
            "queue_ahead_available": int(
                self.services.math.isfinite(
                    float(order.get("decision_queue_ahead_btc", self.services.math.nan))
                )
            ),
            "queue_ahead_source": str(
                order.get("decision_queue_ahead_source", "unavailable")
            ),
            "queue_ahead_asof_ts_ns": int(
                order.get("decision_queue_ahead_asof_ts_ns", 0) or 0
            ),
            "weighted_mid_proxy_shift_bps": float(
                diag.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "book_imbalance": float(diag.get("book_imb", 0.0) or 0.0),
            "near_depth_total_btc": float(
                diag.get("near_depth_total", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                diag.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                diag.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "local_toxicity": float(
                order.get("decision_local_toxicity", 0.0) or 0.0
            ),
            "decision_visible_bbo_cutoff_ts_ms": int(
                order.get("decision_visible_bbo_cutoff_ts_ms", submit_ts_ms)
                or submit_ts_ms
            ),
            "decision_bbo_source_ts_ms": int(
                order.get("decision_bbo_source_ts_ms", 0) or 0
            ),
            "decision_bbo_source_kind": str(
                order.get("decision_bbo_source_kind", "unavailable")
                or "unavailable"
            ),
            "decision_visible_l2_cutoff_ts_ms": int(
                order.get("decision_visible_l2_cutoff_ts_ms", submit_ts_ms)
                or submit_ts_ms
            ),
            "decision_l2_source_ts_ms": int(
                order.get("decision_l2_source_ts_ms", 0) or 0
            ),
            "decision_visible_trade_cutoff_ts_ms": int(
                order.get("decision_visible_trade_cutoff_ts_ms", submit_ts_ms)
                or submit_ts_ms
            ),
            "exec_depth_visibility_source_offset_ms": int(
                order.get("exec_depth_visibility_source_offset_ms", 0) or 0
            ),
            "bbo_feature_ready_ts_ms": int(
                order.get("decision_bbo_feature_ready_ts_ms", submit_ts_ms)
                or submit_ts_ms
            ),
            "l2_feature_ready_ts_ms": int(
                order.get("decision_l2_feature_ready_ts_ms", submit_ts_ms)
                or submit_ts_ms
            ),
            "feature_clock_source": "modeled_receive_ready_source_asof_at_submit",
            "queue_ahead_decision_authority": "exchange_time_diagnostic_only",
            "maker_fee_rate": float(self.state.maker_fee),
            "maker_fee_asset": "USDC",
            "first_fill_fee_usdc": float(fill_price)
            * float(fill_qty)
            * float(self.state.maker_fee),
            "inventory_lifecycle_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_equity_usdc": decision_equity,
            "decision_to_flat_or_day_end_mtm_usdc": 0.0,
            "inventory_lifecycle_terminal_reason": "pending",
            "inventory_lifecycle_terminal_kind": "pending",
            "inventory_lifecycle_closed": 0,
            "inventory_lifecycle_censored": 0,
            "inventory_lifecycle_terminal_inventory_btc": float(inventory_after_fill),
            "inventory_lifecycle_terminal_mark_price_usdc_per_btc": float(fill_price),
            "inventory_lifecycle_max_abs_inventory_btc": float(abs(inventory_after_fill)),
            "inventory_lifecycle_mae_usdc": 0.0,
        }
        if not self.state.first_opener_trace_schema_version.endswith(".v2"):
            record.update(
                {
                    "parent_aggtrade_flow_imbalance": float(
                        order.get(
                            "decision_parent_aggtrade_flow_imbalance",
                            0.0,
                        )
                        or 0.0
                    ),
                    "individual_trade_taker_flow_imbalance_5s": float(
                        order.get(
                            "decision_individual_trade_taker_flow_imbalance_5s",
                            0.0,
                        )
                        or 0.0
                    ),
                    "individual_trade_source_ts_ms": int(
                        order.get(
                            "decision_individual_trade_source_ts_ms",
                            0,
                        )
                        or 0
                    ),
                    "individual_trade_feature_ready_ts_ms": submit_ts_ms,
                }
            )
        self.state.first_opener_decision_recorded_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.first_opener_decision_open_records[inventory_lifecycle_id] = record

    def _finalize_first_opener_decision_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        t_ms: int,
        mark_px: float,
        *,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        if self.state.trace_first_opener_decision_to_terminal is None:
            return
        record = self.state.first_opener_decision_open_records.pop(int(inventory_lifecycle_id), None)
        if record is None:
            return
        terminal_equity = float(self._inventory_lifecycle_pnl(mark_px))
        decision_equity = float(record["decision_equity_usdc"])
        record.update(
            {
                "inventory_lifecycle_terminal_ts_ms": int(t_ms),
                "inventory_lifecycle_terminal_equity_usdc": terminal_equity,
                "decision_to_flat_or_day_end_mtm_usdc": float(
                    terminal_equity - decision_equity
                ),
                "inventory_lifecycle_terminal_reason": str(terminal_reason),
                "inventory_lifecycle_terminal_kind": (
                    "flat" if closed else "day_end_mtm_censored"
                ),
                "inventory_lifecycle_closed": int(bool(closed)),
                "inventory_lifecycle_censored": int(not closed),
                "inventory_lifecycle_terminal_inventory_btc": float(self.state.q),
                "inventory_lifecycle_terminal_mark_price_usdc_per_btc": float(mark_px),
                "inventory_lifecycle_max_abs_inventory_btc": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_mae_usdc": float(self.state.inventory_lifecycle_cur_min_pnl),
            }
        )
        self.state.trace_first_opener_decision_to_terminal.append(record)

    def _finalize_inventory_lifecycle_boundary(self,
        t_ms: int,
        mark_px: float,
        *,
        terminal_reason: str,
    ) -> None:
        self.state.local_order_lifecycle.inventory_lifecycle_repair(
            int(self.state.inventory_lifecycle_count),
            int(t_ms),
        )
        self._record_local_order_value_inventory_lifecycle_repair(
            self.state.inventory_lifecycle_count,
            int(t_ms),
        )
        self._finalize_local_action_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_queue_value_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_sell_add_skip_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_state_conditioned_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_safe_add_rearm_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_state_conditioned_rearm_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            t_ms,
            mark_px,
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_cooldown_lineage_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            outcome_ts_ms=int(t_ms),
            mark_px=float(mark_px),
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_first_add_decision_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            int(t_ms),
            float(mark_px),
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_first_opener_decision_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            int(t_ms),
            float(mark_px),
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_sell_add_price_penalty_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            int(t_ms),
            float(mark_px),
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_multi_short_repair_inventory_lifecycle(
            self.state.inventory_lifecycle_count,
            int(t_ms),
            float(mark_px),
            closed=True,
            terminal_reason=terminal_reason,
        )
        self._finalize_fair_center_assignment(
            int(t_ms),
            float(mark_px),
            terminal_reason=terminal_reason,
            censored=False,
        )
        self.state.inventory_lifecycle_closed_count += 1
        self.state.inventory_lifecycle_active = False
        if self.state.ranked_toxicity_guard_binding_active:
            self.context.ranked_toxicity_guard_binding.on_inventory_lifecycle_terminal(
                event_ts_ns=int(t_ms) * 1_000_000,
                candidate_inventory_lifecycle_ordinal=int(self.state.inventory_lifecycle_count),
            )

    def _inventory_lifecycle_on_fill(self,
        side: str,
        prev_q: float,
        fill_qty: float,
        t_ms: int,
        fill_price: float,
        fee_rate: float,
        *,
        physical_fill_identity: str,
        economic_legs: tuple[dict[str, Any], ...],
    ) -> None:
        new_q = self.state.q
        crossed_side = prev_q * new_q < -1e-20
        if (
            not economic_legs
            or any(
                str(leg.get("physical_fill_identity", ""))
                != str(physical_fill_identity)
                for leg in economic_legs
            )
            or not self.services.math.isclose(
                sum(float(leg["quantity_btc"]) for leg in economic_legs),
                float(fill_qty),
                rel_tol=0.0,
                abs_tol=1e-10,
            )
        ):
            raise RuntimeError("inventory_lifecycle economic legs lost physical fill identity")
        self.state.inventory_lifecycle_economic_fill_leg_trace.extend(
            dict(leg) for leg in economic_legs
        )
        if crossed_side:
            # Attribute a flip at the exact zero-inventory boundary.  The cash
            # state after the full fill contains both fee legs; add back only
            # the opening leg when valuing the old inventory_lifecycle.
            final_q = self.state.q
            final_cash = self.state.cash
            if (
                len(economic_legs) != 2
                or economic_legs[0]["economic_leg_role"] != "closing"
                or economic_legs[1]["economic_leg_role"] != "opening"
            ):
                raise RuntimeError("cross-zero fill requires closing/opening legs")
            closing_qty = float(economic_legs[0]["quantity_btc"])
            opening_qty = float(economic_legs[1]["quantity_btc"])
            zero_equity = self.services.inventory_lifecycle_flip_zero_boundary_equity(
                post_fill_cash=final_cash,
                post_fill_position=final_q,
                fill_price=fill_price,
                opening_quantity=opening_qty,
                fee_rate=fee_rate,
            )

            if side == "BUY":
                self.state.inventory_lifecycle_buy_fills += 1
                self.state.inventory_lifecycle_run_buy_fills += 1
            else:
                self.state.inventory_lifecycle_sell_fills += 1
                self.state.inventory_lifecycle_run_sell_fills += 1
            self.state.inventory_lifecycle_reducing_fills += 1
            self.state.inventory_lifecycle_run_reducing_fills += 1
            self.state.first_add_inventory_lifecycle_reducing_fills += 1

            self.state.q = 0.0
            self.state.cash = zero_equity
            self._sell_add_price_penalty_on_fill(side, prev_q, closing_qty)
            self._multi_short_repair_on_inventory_lifecycle_fill(
                side,
                prev_q,
                closing_qty,
                int(t_ms),
                float(fill_price),
            )
            self._fair_center_note_fill(side, closing_qty, int(t_ms))
            self._inventory_lifecycle_update_path(t_ms, fill_price)
            self.state.inventory_lifecycle_inventory_time_last_inventory_btc = 0.0
            self._maybe_release_multi_short_repair(int(t_ms), float(fill_price))
            self._finalize_inventory_lifecycle_boundary(
                int(t_ms),
                float(fill_price),
                terminal_reason="flip",
            )

            self.state.q = final_q
            self.state.cash = final_cash
            self.state.inventory_lifecycle_active = True
            self.state.inventory_lifecycle_count += 1
            self.state.inventory_lifecycle_exposure_increasing_fills = 0
            self.state.inventory_lifecycle_reducing_fills = 0
            self.state.inventory_lifecycle_buy_fills = 0
            self.state.inventory_lifecycle_sell_fills = 0
            self.state.first_add_inventory_lifecycle_exposure_increasing_fills = 0
            self.state.first_add_inventory_lifecycle_reducing_fills = 0
            self.state.inventory_lifecycle_start_ts = int(t_ms)
            self.state.inventory_lifecycle_start_pnl = float(zero_equity)
            self.state.inventory_lifecycle_cur_max_abs = abs(new_q)
            self.state.inventory_lifecycle_cur_min_pnl = 0.0
            self.state.inventory_lifecycle_inventory_time_to_date_btc_s = 0.0
            self.state.inventory_lifecycle_inventory_time_last_ts_ms = int(t_ms)
            self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(new_q)
            if side == "BUY":
                self.state.inventory_lifecycle_buy_fills += 1
                self.state.inventory_lifecycle_run_buy_fills += 1
            else:
                self.state.inventory_lifecycle_sell_fills += 1
                self.state.inventory_lifecycle_run_sell_fills += 1
            self.state.inventory_lifecycle_exposure_increasing_fills += 1
            self.state.inventory_lifecycle_run_exposure_increasing_fills += 1
            self.state.first_add_inventory_lifecycle_exposure_increasing_fills += 1
            self._fair_center_on_inventory_lifecycle_open(side, int(t_ms))
            self._sell_add_price_penalty_on_fill(side, 0.0, opening_qty)
            self._multi_short_repair_on_inventory_lifecycle_fill(
                side,
                0.0,
                opening_qty,
                int(t_ms),
                float(fill_price),
            )
            self._fair_center_note_fill(side, opening_qty, int(t_ms))
            for live_order in (*self.state.bid_orders, *self.state.ask_orders):
                self.state.local_order_lifecycle.bind_inventory_lifecycle(
                    live_order,
                    int(self.state.inventory_lifecycle_count),
                )
            self._inventory_lifecycle_update_path(t_ms, fill_price)
            self._maybe_release_multi_short_repair(int(t_ms), float(fill_price))
            return

        opened_inventory_lifecycle = False
        if not self.state.inventory_lifecycle_active and abs(prev_q) < 1e-10 and abs(new_q) >= 1e-10:
            self.state.inventory_lifecycle_active = True
            self.state.inventory_lifecycle_count += 1
            opened_inventory_lifecycle = True
            self.state.inventory_lifecycle_exposure_increasing_fills = 0
            self.state.inventory_lifecycle_reducing_fills = 0
            self.state.inventory_lifecycle_buy_fills = 0
            self.state.inventory_lifecycle_sell_fills = 0
            self.state.first_add_inventory_lifecycle_exposure_increasing_fills = 0
            self.state.first_add_inventory_lifecycle_reducing_fills = 0
            self.state.inventory_lifecycle_start_ts = int(t_ms)
            self.state.inventory_lifecycle_start_pnl = self._inventory_lifecycle_pnl(fill_price) + (
                float(fill_price) * float(fill_qty) * float(fee_rate)
            )
            self.state.inventory_lifecycle_cur_max_abs = abs(new_q)
            self.state.inventory_lifecycle_cur_min_pnl = 0.0
            self.state.inventory_lifecycle_inventory_time_to_date_btc_s = 0.0
            self.state.inventory_lifecycle_inventory_time_last_ts_ms = int(t_ms)
            self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(new_q)
        if opened_inventory_lifecycle:
            self._fair_center_on_inventory_lifecycle_open(side, int(t_ms))
            for live_order in (*self.state.bid_orders, *self.state.ask_orders):
                self.state.local_order_lifecycle.bind_inventory_lifecycle(
                    live_order,
                    int(self.state.inventory_lifecycle_count),
                )
        if not self.state.inventory_lifecycle_active:
            return

        if side == "BUY":
            self.state.inventory_lifecycle_buy_fills += 1
            self.state.inventory_lifecycle_run_buy_fills += 1
        else:
            self.state.inventory_lifecycle_sell_fills += 1
            self.state.inventory_lifecycle_run_sell_fills += 1
        exposure_increasing = abs(new_q) > abs(prev_q) + 1e-10
        reducing = abs(new_q) < abs(prev_q) - 1e-10
        if exposure_increasing:
            self.state.inventory_lifecycle_exposure_increasing_fills += 1
            self.state.inventory_lifecycle_run_exposure_increasing_fills += 1
            self.state.first_add_inventory_lifecycle_exposure_increasing_fills += 1
        if reducing:
            self.state.inventory_lifecycle_reducing_fills += 1
            self.state.inventory_lifecycle_run_reducing_fills += 1
            self.state.first_add_inventory_lifecycle_reducing_fills += 1
        self._sell_add_price_penalty_on_fill(side, prev_q, fill_qty)
        self._multi_short_repair_on_inventory_lifecycle_fill(
            side,
            prev_q,
            fill_qty,
            int(t_ms),
            float(fill_price),
        )
        self._fair_center_note_fill(side, fill_qty, int(t_ms))
        if exposure_increasing and abs(prev_q) > 1e-10:
            self.state.inventory_lifecycle_shadow_reducing_only_blocks += 1
            if abs(prev_q) >= 0.006:
                self.state.inventory_lifecycle_shadow_inv_006_blocks += 1
            if abs(prev_q) >= 0.008:
                self.state.inventory_lifecycle_shadow_inv_008_blocks += 1
            if abs(prev_q) >= 0.010:
                self.state.inventory_lifecycle_shadow_inv_010_blocks += 1
            age_s = max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0)
            if age_s >= 20.0 * 60.0:
                self.state.inventory_lifecycle_shadow_age_20m_blocks += 1
            if age_s >= 40.0 * 60.0:
                self.state.inventory_lifecycle_shadow_age_40m_blocks += 1
            if age_s >= 60.0 * 60.0:
                self.state.inventory_lifecycle_shadow_age_60m_blocks += 1

        self._inventory_lifecycle_update_path(t_ms, fill_price)
        self.state.inventory_lifecycle_inventory_time_last_inventory_btc = float(new_q)
        self._maybe_release_multi_short_repair(int(t_ms), float(fill_price))
        if abs(new_q) < 1e-10:
            self._finalize_inventory_lifecycle_boundary(
                int(t_ms),
                float(fill_price),
                terminal_reason="flat",
            )

    def _inventory_lifecycle_exposure_risk_active(self,
        side: str,
        t_ms: int,
        *,
        inv_threshold: float,
        age_s_threshold: float,
    ) -> bool:
        """Return True when a large/old inventory_lifecycle would add exposure on this side."""
        if not self.state.inventory_lifecycle_active or abs(self.state.q) < 1e-10:
            return False
        exposure_increasing = (side == "BUY" and self.state.q > 0.0) or (side == "SELL" and self.state.q < 0.0)
        if not exposure_increasing:
            return False
        inv_hit = (
            inv_threshold > 0.0
            and abs(self.state.q) >= inv_threshold
        )
        age_s = max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0)
        age_hit = age_s_threshold > 0.0 and age_s >= age_s_threshold
        return bool(inv_hit or age_hit)

    def _inventory_lifecycle_stop_add_active(self, side: str, t_ms: int) -> bool:
        """Return True when inventory_lifecycle risk should block only exposure-increasing quotes."""
        if not self.state.inventory_lifecycle_stop_add_enabled:
            return False
        return self._inventory_lifecycle_exposure_risk_active(
            side,
            t_ms,
            inv_threshold=self.state.inventory_lifecycle_stop_add_inv_threshold,
            age_s_threshold=self.state.inventory_lifecycle_stop_add_age_s,
        )

    def _inventory_lifecycle_soft_control_active(self, side: str, t_ms: int, side_ctx: Optional[dict] = None) -> bool:
        """Return True when inventory_lifecycle risk should only soften the add-side quote."""
        if not self.state.inventory_lifecycle_soft_control_enabled:
            return False
        base_active = self._inventory_lifecycle_exposure_risk_active(
            side,
            t_ms,
            inv_threshold=self.state.inventory_lifecycle_soft_inv_threshold,
            age_s_threshold=self.state.inventory_lifecycle_soft_age_s,
        )
        if not base_active:
            return False
        if not self.state.inventory_lifecycle_soft_gate_cfg.enabled:
            return True

        side_ctx = side_ctx if isinstance(side_ctx, dict) else {}
        inventory_lifecycle_age_s = (
            max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0)
            if self.state.inventory_lifecycle_active else 0.0
        )
        try:
            pred_ret = float(side_ctx.get("pred_ret", self.state.quote_ret) or 0.0)
        except Exception:
            pred_ret = float(self.state.quote_ret or 0.0)
        side_adverse_ret = max(0.0, -pred_ret) if side == "BUY" else max(0.0, pred_ret)
        try:
            refill_edge = float(side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0) - float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            )
        except Exception:
            refill_edge = 0.0
        trend_norm = 0.0
        if self.state.inventory_lifecycle_soft_gate_cfg.trend_ret_ref > 0.0:
            trend_norm = min(
                1.0,
                side_adverse_ret / max(self.state.inventory_lifecycle_soft_gate_cfg.trend_ret_ref, 1e-12),
            )
        good_refill_norm = 0.0
        if self.state.inventory_lifecycle_soft_gate_cfg.refill_ref > 0.0:
            good_refill_norm = min(
                1.0,
                max(0.0, refill_edge) / max(self.state.inventory_lifecycle_soft_gate_cfg.refill_ref, 1e-12),
            )
        micro_reversion_score = good_refill_norm * (1.0 - trend_norm)
        gate = self.services.inventory_lifecycle_soft_gate_result(
            side=side,
            prev_inventory=self.state.q,
            max_inventory=self.state.max_inv,
            inventory_lifecycle_age_s=inventory_lifecycle_age_s,
            side_adverse_ret=side_adverse_ret,
            refill_edge=refill_edge,
            micro_reversion_score=micro_reversion_score,
            cfg=self.state.inventory_lifecycle_soft_gate_cfg,
        )
        side_ctx["inventory_lifecycle_soft_gate_enabled"] = True
        side_ctx["inventory_lifecycle_soft_gate_active"] = bool(gate.active)
        side_ctx["inventory_lifecycle_soft_inventory_lifecycle_score"] = float(gate.inventory_lifecycle_score)
        side_ctx["inventory_lifecycle_soft_trend_score"] = float(gate.trend_score)
        side_ctx["inventory_lifecycle_soft_repair_weak"] = bool(gate.repair_weak)
        side_ctx["inventory_lifecycle_soft_side_adverse_ret"] = float(gate.side_adverse_ret)
        side_ctx["inventory_lifecycle_soft_refill_edge"] = float(gate.refill_edge)
        side_ctx["inventory_lifecycle_soft_micro_reversion_score"] = float(gate.micro_reversion_score)
        return bool(gate.active)

    def _quarantine_flat_fork_arms(self):
        """Stop new exposure when the original fork reaches its joint washout."""
        if (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and not self.state.ema_add_wait_fork_quarantine
            and abs(float(self.state.q)) <= 1e-10
            and not self.state.inventory_lifecycle_active
        ):
            # Once either arm reaches flat, it may only drain inherited order
            # state.  A pending-cancel fill can reopen inventory; the quote
            # policy below then permits reducing quotes only until flat again.
            self.state.ema_add_wait_fork_quarantine = True
            self.state.ema_add_wait_fork_quarantine_ts_ms = int(self.state.t)
            self._request_cancel_all(
                self.state.bid_orders,
                int(self.state.t),
                reason="ema_add_wait_joint_washout",
            )
            self._request_cancel_all(
                self.state.ask_orders,
                int(self.state.t),
                reason="ema_add_wait_joint_washout",
            )

        if (
            self.state.cooldown_duration_fork_enabled
            and self.state.cooldown_duration_fork_assigned
            and not self.state.cooldown_duration_fork_quarantine
            and abs(float(self.state.q)) <= 1e-10
            and not self.state.inventory_lifecycle_active
        ):
            self.state.cooldown_duration_fork_quarantine = True
            self.state.cooldown_duration_fork_quarantine_ts_ms = int(self.state.t)

    def _fork_reached_economic_washout(self) -> bool:
        """Finish only when inventory and all scheduler-owned order risk are gone."""
        if (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_quarantine
            and abs(float(self.state.q)) <= 1e-10
            and not self.state.inventory_lifecycle_active
            and not self.state.bid_orders
            and not self.state.ask_orders
            and self.state.dynamic_fill_hazard_hold is None
            and not self.state.dynamic_fill_hazard_paths
        ):
            self.state.ema_add_wait_fork_terminal = True
            self.state.ema_add_wait_fork_terminal_ts_ms = int(self.state.t)
            self.state.ema_add_wait_fork_reason = "arm_economic_washout"
            return True

        if (
            self.state.cooldown_duration_fork_enabled
            and self.state.cooldown_duration_fork_quarantine
            and abs(float(self.state.q)) <= 1e-10
            and not self.state.inventory_lifecycle_active
            and not self.state.bid_orders
            and not self.state.ask_orders
            and self.state.dynamic_fill_hazard_hold is None
            and not self.state.dynamic_fill_hazard_paths
        ):
            self.state.cooldown_duration_fork_terminal = True
            self.state.cooldown_duration_fork_terminal_ts_ms = int(self.state.t)
            self.state.cooldown_duration_fork_reason = "arm_economic_washout"
            return True
        return False

    def _apply_quote_inventory_limits(self):
        """Use shared sizing rules, then enforce inventory and notional limits."""
        # ── Eta: inventory-weighted order size ──
        if self.state.eta > 0.0 and self.state.max_inv > 1e-10:
            # Retain this checkpoint field; sizing itself has one owner.
            self.state.q_norm = self.state.q / self.state.max_inv
        self.state.bid_sz, self.state.ask_sz = pre_generic_sizes(
            order_size=self.state.order_size, lot=self.state.LOT_SIZE,
            inventory=self.state.q, max_inventory=self.state.max_inv,
            eta=self.state.eta, symmetric=self.state.symmetric_size,
        )

        # Shared live/replay side-policy sizing. A valid minimum-lot quote
        # remains at one lot, matching MakerEngine._policy_sized_qty().

        self.state.bid_sz = self._apply_common_policy_size(self.state.bid_sz, self.state.bid_common_policy.size_mult)
        self.state.ask_sz = self._apply_common_policy_size(self.state.ask_sz, self.state.ask_common_policy.size_mult)

        # ── Position room cap ──
        if self.state.q > 0.0:
            self.state.room = self.state.max_inv - self.state.q
            self.state.room = self.services.math.floor(self.state.room / self.state.LOT_SIZE) * self.state.LOT_SIZE
            if self.state.room >= self.state.LOT_SIZE:
                if self.state.bid_sz > self.state.room:
                    self.state.bid_sz = self.state.room
            else:
                self.state.bid_sz = 0.0
        if self.state.q < 0.0:
            self.state.room = self.state.max_inv - (-self.state.q)
            self.state.room = self.services.math.floor(self.state.room / self.state.LOT_SIZE) * self.state.LOT_SIZE
            if self.state.room >= self.state.LOT_SIZE:
                if self.state.ask_sz > self.state.room:
                    self.state.ask_sz = self.state.room
            else:
                self.state.ask_sz = 0.0

        # ── Anti-flip cap ──
        if self.state.q < -self.state.LOT_SIZE:
            self.state.close_cap = self.services.math.floor((-self.state.q) / self.state.LOT_SIZE) * self.state.LOT_SIZE
            if self.state.bid_sz > self.state.close_cap:
                self.state.bid_sz = self.state.close_cap
        if self.state.q > self.state.LOT_SIZE:
            self.state.close_cap = self.services.math.floor(self.state.q / self.state.LOT_SIZE) * self.state.LOT_SIZE
            if self.state.ask_sz > self.state.close_cap:
                self.state.ask_sz = self.state.close_cap

        self.state.bid_notional_capped = False
        self.state.ask_notional_capped = False
        if self.services.math.isfinite(self.state.max_position_value):
            if self.services._exposure_increasing("BUY", self.state.q, self.state.bid_sz, self.state.LOT_SIZE):
                self.state.capped = self.services.cap_exposure_qty_by_position_value(
                    side="BUY", current_qty=self.state.q, mid=self.state.mid, requested_qty=self.state.bid_sz,
                    max_position_value=self.state.max_position_value, lot=self.state.LOT_SIZE,
                )
                self.state.bid_notional_capped = self.state.capped < self.state.bid_sz
                self.state.bid_sz = self.state.capped
            if self.services._exposure_increasing("SELL", self.state.q, self.state.ask_sz, self.state.LOT_SIZE):
                self.state.capped = self.services.cap_exposure_qty_by_position_value(
                    side="SELL", current_qty=self.state.q, mid=self.state.mid, requested_qty=self.state.ask_sz,
                    max_position_value=self.state.max_position_value, lot=self.state.LOT_SIZE,
                )
                self.state.ask_notional_capped = self.state.capped < self.state.ask_sz
                self.state.ask_sz = self.state.capped
            self.state.risk_notional_cap_count += int(
                self.state.bid_route_due and self.state.bid_notional_capped
            ) + int(self.state.ask_route_due and self.state.ask_notional_capped)

        self.state.bid_can_post_after_inventory = self.state.q < self.state.max_inv and self.state.bid_sz >= self.state.LOT_SIZE
        self.state.ask_can_post_after_inventory = self.state.q > -self.state.max_inv and self.state.ask_sz >= self.state.LOT_SIZE
        if self.context.inventory_markout_scope == "exclude_pure_reducing" or self.context.inventory_cooldown_mode != "original":
            # Read-only audit after the unchanged lot/room/anti-flip/notional
            # consumers. Between the pre-policy role and this point there
            # is no event delivery or pending mutation, and all later size
            # consumers are non-increasing. A zero-sized order stays blocked
            # by the original inventory_or_size admission below.
            try:
                self.context._inventory_pending_snapshot = tuple(self.services.PendingExposure(
                    self._ranked_guard_order_id(order), str(order["side"]), float(order["remaining"]))
                    for order in (*self.state.bid_orders, *self.state.ask_orders)
                    if float(order["remaining"]) > 0)
            except (KeyError, TypeError, ValueError, OverflowError):
                self.context._inventory_pending_snapshot = None
            for loop_item in (("BUY", self.state.bid_sz), ("SELL", self.state.ask_sz)):
                self.context._inventory_side, self.context._inventory_size = loop_item
                self.state.quote_context[self.context._inventory_side]["final_quantity_role"] = self.services.pure_reducing_role(
                    side=self.context._inventory_side, inventory=self.state.q, quantity=self.context._inventory_size,
                    pending_orders=self.context._inventory_pending_snapshot,
                    snapshot_known=self.context._inventory_pending_snapshot is not None)
        self.state.hb_new = self.state.bid_can_post_after_inventory
        self.state.ha_new = self.state.ask_can_post_after_inventory
        self.state.bid_block_reasons: list[str] = [] if self.state.hb_new else ["inventory_or_size"]
        self.state.ask_block_reasons: list[str] = [] if self.state.ha_new else ["inventory_or_size"]
        if self.state.bid_notional_capped and not self.state.hb_new:
            self.state.bid_block_reasons.append("position_value_cap")
        if self.state.ask_notional_capped and not self.state.ha_new:
            self.state.ask_block_reasons.append("position_value_cap")

        self.state.bid_stale_hard_pause = bool(
            self.state.bid_common_policy.reason_mask & self.services.POLICY_REASON_STALE_HARD
        )
        self.state.bid_defense_hard_pause = bool(
            self.state.quote_context["BUY"].get("defense_guard", False)
            and self.state.quote_context["BUY"].get("defense_pause", False)
        )
        self.state.bid_multi_short_defense_override_requested = bool(
            self.state.bid_defense_hard_pause and self._multi_short_repair_candidate_active()
        )
        self.state.bid_common_hard_pause = bool(
            self.state.bid_stale_hard_pause
            or (
                self.state.bid_defense_hard_pause
                and not self.state.bid_multi_short_defense_override_requested
            )
        )
        self.state.quote_context["BUY"][
            "multi_short_reducing_buy_defense_override_requested"
        ] = bool(self.state.bid_multi_short_defense_override_requested)
        self.state.ask_common_hard_pause = bool(
            self.state.ask_common_policy.reason_mask & self.services.POLICY_REASON_STALE_HARD
            or (
                self.state.quote_context["SELL"].get("defense_guard", False)
                and self.state.quote_context["SELL"].get("defense_pause", False)
            )
        )
        if self.state.bid_common_hard_pause:
            self.state.hb_new = False
            self.state.bid_block_reasons.append("common_policy_hard_pause")
        if self.state.ask_common_hard_pause:
            self.state.ha_new = False
            self.state.ask_block_reasons.append("common_policy_hard_pause")
        if (
            self.state.hb_new
            and self.state.q >= 0.0
            and not self.state.bid_common_policy.allow_exposure_increase
        ):
            self.state.hb_new = False
            self.state.bid_block_reasons.append("common_policy_exposure")
        if (
            self.state.ha_new
            and self.state.q <= 0.0
            and not self.state.ask_common_policy.allow_exposure_increase
        ):
            self.state.ha_new = False
            self.state.ask_block_reasons.append("common_policy_exposure")

    def _apply_quote_entry_policies(self):
        """Apply lifecycle, multi-market and final exposure admission decisions."""
        # a long inventory_lifecycle may block BUY adds while still allowing SELL
        # reducers, and vice versa for a short inventory_lifecycle.
        self.state.quote_context["BUY"]["inventory_lifecycle_stop_add"] = False
        self.state.quote_context["SELL"]["inventory_lifecycle_stop_add"] = False
        self.state.bid_inventory_lifecycle_stop_add = bool(
            self.state.bid_route_due
            and self.state.hb_new
            and self._inventory_lifecycle_stop_add_active("BUY", int(self.state.t))
        )
        self.state.ask_inventory_lifecycle_stop_add = bool(
            self.state.ask_route_due
            and self.state.ha_new
            and self._inventory_lifecycle_stop_add_active("SELL", int(self.state.t))
        )
        if self.state.bid_inventory_lifecycle_stop_add:
            self.state.hb_new = False
            self.state.quote_context["BUY"]["inventory_lifecycle_stop_add"] = True
            self.state.bid_block_reasons.append("inventory_lifecycle_stop_add")
            self.state.inventory_lifecycle_stop_add_count += 1
            self.state.bid_inventory_lifecycle_stop_add_count += 1
        if self.state.ask_inventory_lifecycle_stop_add:
            self.state.ha_new = False
            self.state.quote_context["SELL"]["inventory_lifecycle_stop_add"] = True
            self.state.ask_block_reasons.append("inventory_lifecycle_stop_add")
            self.state.inventory_lifecycle_stop_add_count += 1
            self.state.ask_inventory_lifecycle_stop_add_count += 1

        # Cross-venue post-fill policy is deliberately downstream of quote
        # construction.  It can veto only the inventory-increasing side;
        # it cannot alter spread, size, or the reducing quote.
        self.state.global_flow_state = (
            self.state.reference_scheduler.snapshot(now_ns=self.state.policy_now_ns)
            if self.state.reference_scheduler is not None
            else self.state.historical_global_flow_cursor.asof(
                self.state.policy_now_ns,
                horizon_ms=self.state.multi_market_policy_cfg.horizon_ms,
            )
            if self.state.historical_global_flow_cursor is not None
            else None
        )
        self.state.policy_decision = self.state.multi_market_policy.evaluate(
            self.services.MultiMarketPolicyContext(
                decision_ts_ns=self.state.policy_now_ns,
                inventory=float(self.state.q),
                inventory_lifecycle_active=bool(self.state.inventory_lifecycle_active),
                inventory_lifecycle_age_s=(
                    max(0.0, float(self.state.t - self.state.inventory_lifecycle_start_ts) / 1000.0)
                    if self.state.inventory_lifecycle_active
                    else 0.0
                ),
                inventory_lifecycle_fills=int(self.state.inventory_lifecycle_buy_fills + self.state.inventory_lifecycle_sell_fills),
                repair_probability=self.state.repair_probability,
                repair_probability_change=self.state.repair_probability_change,
                repair_probability_age_ms=self.state.repair_probability_age_ms,
                global_flow_state=self.state.global_flow_state,
            )
        )
        self.state.multi_market_policy_eval_count += int(self.state.multi_market_policy_cfg.enabled)
        self.state.bid_policy_baseline_allow_post = bool(self.state.hb_new)
        self.state.ask_policy_baseline_allow_post = bool(self.state.ha_new)
        for loop_item in (
            ("BUY", self.state.policy_decision.buy),
            ("SELL", self.state.policy_decision.sell),
        ):
            self.state.side, self.state.side_decision = loop_item
            self.state.side_ctx = self.state.quote_context[self.state.side]
            self.state.side_ctx["multi_market_policy_mode"] = self.state.policy_decision.mode
            self.state.side_ctx["multi_market_policy_hit"] = bool(self.state.side_decision.active)
            self.state.side_ctx["multi_market_policy_reason"] = self.state.side_decision.reason
            self.state.side_ctx["multi_market_policy_baseline_allow_post"] = int(
                self.state.bid_policy_baseline_allow_post
                if self.state.side == "BUY"
                else self.state.ask_policy_baseline_allow_post
            )
            self.state.side_ctx["multi_market_repair_probability"] = self.state.repair_probability
            self.state.side_ctx["multi_market_repair_probability_change"] = (
                self.state.repair_probability_change
            )
            self.state.side_ctx["multi_market_repair_probability_age_ms"] = (
                self.state.repair_probability_age_ms
            )
            for loop_item in self.state.policy_decision.evidence.items():
                self.state.key, self.state.value = loop_item
                self.state.side_ctx[f"multi_market_{self.state.key}"] = self.state.value

        self.state.bid_policy_hit = bool(self.state.policy_decision.buy.active)
        self.state.ask_policy_hit = bool(self.state.policy_decision.sell.active)
        self.state.multi_market_policy_hit_count += int(self.state.bid_policy_hit) + int(self.state.ask_policy_hit)
        self.state.bid_multi_market_policy_hit_count += int(self.state.bid_policy_hit)
        self.state.ask_multi_market_policy_hit_count += int(self.state.ask_policy_hit)
        self.state.bid_policy_block = (
            self.state.hb_new
            and self.state.q > 0.0
            and not self.state.policy_decision.buy.allow_exposure_increase
        )
        self.state.ask_policy_block = (
            self.state.ha_new
            and self.state.q < 0.0
            and not self.state.policy_decision.sell.allow_exposure_increase
        )
        self.state.quote_context["BUY"]["multi_market_policy_effective_block"] = bool(
            self.state.bid_policy_block
        )
        self.state.quote_context["SELL"]["multi_market_policy_effective_block"] = bool(
            self.state.ask_policy_block
        )
        if self.state.bid_policy_block:
            self.state.hb_new = False
            self.state.bid_block_reasons.append("multi_market_post_fill_stop_add")
            self.state.multi_market_policy_effective_block_count += 1
            self.state.bid_multi_market_policy_effective_block_count += 1
        if self.state.ask_policy_block:
            self.state.ha_new = False
            self.state.ask_block_reasons.append("multi_market_post_fill_stop_add")
            self.state.multi_market_policy_effective_block_count += 1
            self.state.ask_multi_market_policy_effective_block_count += 1
        self.state.quote_context["BUY"]["multi_market_policy_candidate_allow_post"] = int(
            self.state.hb_new
        )
        self.state.quote_context["SELL"]["multi_market_policy_candidate_allow_post"] = int(
            self.state.ha_new
        )

        if self.state.flat_unilateral_max_ms > 0 and abs(self.state.q) <= (self.state.LOT_SIZE * 0.5):
            self.state.bid_blocked = (not self.state.hb_new) and self.state.q >= 0.0 and self.state.bid_sz >= self.state.LOT_SIZE and self.state.q < self.state.max_inv
            self.state.ask_blocked = (not self.state.ha_new) and self.state.q <= 0.0 and self.state.ask_sz >= self.state.LOT_SIZE and self.state.q > -self.state.max_inv
            if self.state.bid_blocked and self.state.ha_new and not self.state.ask_blocked:
                if self.state.flat_unilateral_bid_started_ms <= 0:
                    self.state.flat_unilateral_bid_started_ms = self.state.t
                if self.state.t - self.state.flat_unilateral_bid_started_ms >= self.state.flat_unilateral_max_ms:
                    self.state.hb_new = True
                    self.state.flat_unilateral_release_count += 1
                    self.state.flat_unilateral_bid_release_count += 1
                self.state.flat_unilateral_ask_started_ms = 0
            elif self.state.ask_blocked and self.state.hb_new and not self.state.bid_blocked:
                if self.state.flat_unilateral_ask_started_ms <= 0:
                    self.state.flat_unilateral_ask_started_ms = self.state.t
                if self.state.t - self.state.flat_unilateral_ask_started_ms >= self.state.flat_unilateral_max_ms:
                    self.state.ha_new = True
                    self.state.flat_unilateral_release_count += 1
                    self.state.flat_unilateral_ask_release_count += 1
                self.state.flat_unilateral_bid_started_ms = 0
            else:
                if not self.state.bid_blocked:
                    self.state.flat_unilateral_bid_started_ms = 0
                if not self.state.ask_blocked:
                    self.state.flat_unilateral_ask_started_ms = 0
        else:
            self.state.flat_unilateral_bid_started_ms = 0
            self.state.flat_unilateral_ask_started_ms = 0

        # pause_exposure treats the configured spread cap as a risk limit,
        # not as permission to pull a wide quote back toward the market.
        # Apply it after flat-unilateral release so the release TTL cannot
        # silently reopen an exposure-increasing side.
        if bool(self.state.quote_context["BUY"].get("cap_exposure_block", False)) and self.state.q >= 0.0:
            self.state.hb_new = False
            self.state.bid_block_reasons.append("spread_cap")
            self.state.cap_exposure_block_count += 1
            self.state.bid_cap_exposure_block_count += 1
        if bool(self.state.quote_context["SELL"].get("cap_exposure_block", False)) and self.state.q <= 0.0:
            self.state.ha_new = False
            self.state.ask_block_reasons.append("spread_cap")
            self.state.cap_exposure_block_count += 1
            self.state.ask_cap_exposure_block_count += 1

    def _apply_common_policy_size(self, size: float, mult: float) -> float:
        raw = float(size) * float(mult)
        if raw <= 0.0:
            return 0.0
        floored = self.services.math.floor(raw / self.state.LOT_SIZE) * self.state.LOT_SIZE
        if size >= self.state.LOT_SIZE and floored < self.state.LOT_SIZE:
            return self.state.LOT_SIZE
        return floored

    def _kept_order_exceeds_notional(self, side, order, inventory, mark):
        if order is None or not self.services.math.isfinite(self.state.max_position_value):
            return False
        remaining = float(order.get("remaining", 0.0))
        if not self.services._exposure_increasing(side, inventory, remaining, self.state.LOT_SIZE):
            return False
        capped = self.services.cap_exposure_qty_by_position_value(
            side=side, current_qty=inventory, mid=mark, requested_qty=remaining,
            max_position_value=self.state.max_position_value, lot=self.state.LOT_SIZE,
        )
        return capped + max(self.state.LOT_SIZE * 1e-9, 1e-12) < remaining
