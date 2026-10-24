"""Replay cooldown policy owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from strategy.fill_cooldown import OnlineVarianceTimeEpisode


class CooldownPolicy:
    def _variance_episode_for_side(self,
        side: str,
    ) -> OnlineVarianceTimeEpisode | None:
        return self.state.variance_episode_buy if side == "BUY" else self.state.variance_episode_sell

    def _advance_variance_episode(self,
        side: str,
        now_ts_ms: int,
    ) -> OnlineVarianceTimeEpisode | None:
        episode = self._variance_episode_for_side(side)
        if episode is not None and self.state.variance_rate_stream is not None:
            self.state.variance_rate_stream.advance(episode, int(now_ts_ms))
        return episode

    def _cooldown_lineage_record(self, side: str) -> dict[str, Any] | None:
        return self.state.variance_time_lineage_open_by_side.get(str(side).upper())

    def _append_cooldown_lineage_event(self,
        record: dict[str, Any],
        event_type: str,
        event_ts_ms: int,
        **payload: Any,
    ) -> None:
        if self.state.trace_variance_time_lineage_events is None:
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
            "episode_index": int(record.get("_episode_index", 0)),
        }
        event.update(payload)
        self.state.trace_variance_time_lineage_events.append(event)
        record["_event_seq"] = event_seq + 1

    def _cooldown_lineage_update_path(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        if not self.state.variance_time_lineage_randomized_enabled:
            return
        for record in self.state.variance_time_lineage_open_by_side.values():
            last_ts = int(record.get("last_path_ts_ms", now_ts_ms))
            dt_s = max(0.0, float(int(now_ts_ms) - last_ts) / 1_000.0)
            record["inventory_time_btc_s"] = float(
                record.get("inventory_time_btc_s", 0.0)
            ) + abs(float(record.get("last_path_inventory", self.state.q))) * dt_s
            mtm = float(self.state.cash) + float(self.state.q) * float(mark_px)
            pnl_delta = mtm - float(record["decision_mtm"])
            record["lineage_mae"] = min(
                float(record.get("lineage_mae", 0.0)),
                float(pnl_delta),
            )
            record["lineage_max_abs_inventory"] = max(
                float(record.get("lineage_max_abs_inventory", 0.0)),
                abs(float(self.state.q)),
            )
            record["last_path_ts_ms"] = int(now_ts_ms)
            record["last_path_inventory"] = float(self.state.q)

    def _accrue_local_path_to(self, now_ts_ms: int, mark_px: float) -> None:
        """Accrue the pre-boundary local state through one exact millisecond."""

        timestamp = int(now_ts_ms)
        if timestamp < self.state.local_path_accrual_ts_ms:
            raise RuntimeError("local lifecycle path clock regressed")
        price = float(mark_px)
        if not self.services.math.isfinite(price) or price <= 0.0:
            price = float(self.state.prev_mark_px)
        dt_s = float(timestamp - self.state.local_path_accrual_ts_ms) / 1_000.0
        mark_for_interval = (
            float(self.state.prev_mark_px) if float(self.state.prev_mark_px) > 0.0 else price
        )
        self.state.signed_inventory_time_s += float(self.state.q) * dt_s
        self.state.abs_inventory_time_s += abs(float(self.state.q)) * dt_s
        self.state.sq_inventory_time_s += float(self.state.q) * float(self.state.q) * dt_s
        self.state.signed_notional_inventory_time_s += (
            float(self.state.q) * mark_for_interval * dt_s
        )
        self.state.notional_inventory_time_s += abs(float(self.state.q)) * mark_for_interval * dt_s
        self.state.inventory_pnl += float(self.state.q) * (price - float(self.state.prev_mark_px))
        self.state.local_path_accrual_ts_ms = timestamp
        self.state.prev_mark_px = price
        self._inventory_lifecycle_update_path(timestamp, price)
        self._fair_center_update_path(timestamp, price)
        self._cooldown_lineage_update_path(timestamp, price)

    def _cooldown_lineage_add_fill_value(self,
        record: dict[str, Any],
        *,
        side: str,
        fill_ts_ms: int,
        fill_price: float,
        fill_qty: float,
        fee_rate: float,
    ) -> None:
        target_ts = int(fill_ts_ms) + int(
            self.state.variance_time_lineage_markout_horizon_ms
        )
        future_mid, observed_ts, source, censored = self._fixed_horizon_book_mid(
            target_ts
        )
        source_age_ms = (
            max(0, target_ts - int(observed_ts)) if observed_ts > 0 else 10**18
        )
        supported = bool(
            not censored
            and self.services.math.isfinite(float(future_mid))
            and float(future_mid) > 0.0
            and source_age_ms <= self.state.variance_time_lineage_markout_max_age_ms
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0)
        ) + 1
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0)
        ) + float(fill_qty)
        if not supported:
            record["fill_value_unsupported_count"] = int(
                record.get("fill_value_unsupported_count", 0)
            ) + 1
            record["support_valid"] = 0
            reasons = record.setdefault("_unsupported_reasons", set())
            reasons.add(
                "markout_censored"
                if censored
                else "markout_book_stale_or_missing"
            )
            return
        signed_change = (
            float(future_mid) - float(fill_price)
            if str(side).upper() == "BUY"
            else float(fill_price) - float(future_mid)
        )
        fill_value = (
            float(fill_qty) * signed_change
            - float(fill_qty)
            * float(fill_price)
            * max(0.0, float(fee_rate))
        )
        record["fill_value"] = float(record.get("fill_value", 0.0)) + float(
            fill_value
        )
        record["fill_value_supported_count"] = int(
            record.get("fill_value_supported_count", 0)
        ) + 1
        record["fill_value_source_age_ms_max"] = max(
            int(record.get("fill_value_source_age_ms_max", 0)),
            int(source_age_ms),
        )
        record["fill_value_source"] = str(source)

    def _finalize_cooldown_lineage(self,
        side: str,
        *,
        outcome_ts_ms: int,
        mark_px: float,
        terminal_reason: str,
        censored: bool,
    ) -> dict[str, Any] | None:
        normalized_side = str(side).upper()
        record = self.state.variance_time_lineage_open_by_side.get(normalized_side)
        if record is None:
            return None
        if (
            self.state.lineage_outcome_contract_version == "v2"
            and str(terminal_reason) == "opposite_side_fill"
            and abs(float(self.state.q)) >= 1e-10
        ):
            raise RuntimeError(
                "lineage outcome contract v2 requires assignment persistence "
                "through inventory_lifecycle terminal"
            )
        self._cooldown_lineage_update_path(int(outcome_ts_ms), float(mark_px))
        terminal_episode = self._advance_variance_episode(
            normalized_side,
            int(outcome_ts_ms),
        )
        if self.state.lineage_outcome_contract_version == "v2":
            self._append_cooldown_lineage_event(
                record,
                "episode_close",
                int(outcome_ts_ms),
                close_reason=str(terminal_reason),
                accumulated_qv_bps2=(
                    float(terminal_episode.accumulated_qv_bps2)
                    if terminal_episode is not None
                    else 0.0
                ),
            )
        self.state.variance_time_lineage_open_by_side.pop(normalized_side, None)
        terminal_mtm = float(self.state.cash) + float(self.state.q) * float(mark_px)
        reward = terminal_mtm - float(record["decision_mtm"])
        fill_value = float(record.get("fill_value", 0.0))
        queue_cost = float(record.get("queue_cost", 0.0))
        inventory_lifecycle_cost = fill_value - queue_cost - reward
        if self.state.lineage_outcome_contract_version == "v2":
            prior_qv = float(
                record.get("variance_accumulated_qv_bps2_prior_episodes", 0.0)
            )
            terminal_qv = (
                float(terminal_episode.accumulated_qv_bps2)
                if terminal_episode is not None
                else 0.0
            )
            if terminal_episode is not None:
                wall_ready_ts_ms = int(terminal_episode.baseline_ready_ts_ms)
                candidate_ready_ts_ms = int(terminal_episode.ready_ts_ms or 0)
                budget_bps2 = float(terminal_episode.budget_bps2)
            else:
                wall_ready_ts_ms = int(record.get("wall_ready_ts_ms", 0))
                candidate_ready_ts_ms = int(
                    record.get("variance_ready_ts_ms", 0) or 0
                )
                budget_bps2 = float(
                    record.get("variance_budget_bps2_at_terminal", 0.0)
                )
            if candidate_ready_ts_ms > 0:
                variance_ready_status = "observed"
                if candidate_ready_ts_ms < wall_ready_ts_ms:
                    clock_direction = "earlier"
                elif candidate_ready_ts_ms > wall_ready_ts_ms:
                    clock_direction = "later"
                else:
                    clock_direction = "equal"
            else:
                variance_ready_status = "censored_before_ready"
                clock_direction = "censored_unknown"
            first_direction = str(
                record.get("first_clock_direction", "not_observed")
            )
            if first_direction in {"earlier", "later"}:
                clock_direction = first_direction
            action_change_ts_ms = int(
                record.get("final_action_change_ts_ms", 0) or 0
            )
            if action_change_ts_ms > 0:
                action_change_status = "observed"
            elif int(record.get("eligible_decision_count", 0)) <= 0:
                action_change_status = "no_eligible_decision"
            elif bool(censored) and first_direction == "not_observed":
                action_change_status = "censored_before_resolution"
            else:
                action_change_status = "eligible_no_change"
            record.update(
                {
                    "lineage_terminal_ts_ms": int(outcome_ts_ms),
                    "lineage_terminal_equity_usdc": float(terminal_mtm),
                    "lineage_reward_usdc": float(reward),
                    "variance_budget_bps2_at_terminal": budget_bps2,
                    "variance_accumulated_qv_bps2_at_terminal": (
                        prior_qv + terminal_qv
                    ),
                    "wall_ready_ts_ms": wall_ready_ts_ms,
                    "variance_ready_ts_ms": candidate_ready_ts_ms,
                    "variance_ready_status": variance_ready_status,
                    "clock_direction": clock_direction,
                    "final_blocker": str(
                        record.get("final_blocker", "not_observed")
                        or "not_observed"
                    ),
                    "final_action_change_status": action_change_status,
                }
            )
        record.update(
            {
                "outcome_ts_ms": int(outcome_ts_ms),
                "lineage_duration_s": max(
                    0.0,
                    float(int(outcome_ts_ms) - int(record["decision_ts_ms"]))
                    / 1_000.0,
                ),
                "lineage_terminal_reason": str(terminal_reason),
                "lineage_censored": int(bool(censored)),
                "lineage_terminal_mtm": float(terminal_mtm),
                "reward": float(reward),
                "inventory_lifecycle_cost": float(inventory_lifecycle_cost),
                "queue_cost": float(queue_cost),
                "reward_identity_error": float(
                    reward - (fill_value - inventory_lifecycle_cost - queue_cost)
                ),
                "support_valid": int(bool(record.get("support_valid", 1))),
                "unsupported_reasons": "|".join(
                    sorted(record.pop("_unsupported_reasons", set()))
                ),
                "queue_reset_reason_counts": self.services.json.dumps(
                    record.pop("_queue_reset_reason_counts", {}),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "q90_cancel_request_count": int(
                    self.state.dynamic_fill_hazard_cancel_request_count
                    - int(record["q90_cancel_request_count_at_start"])
                ),
                "q90_cancel_ack_count": int(
                    self.state.dynamic_fill_hazard_cancel_ack_count
                    - int(record["q90_cancel_ack_count_at_start"])
                ),
                "q90_pre_ack_fill_count": int(
                    self.state.dynamic_fill_hazard_pre_ack_fill_count
                    - int(record["q90_pre_ack_fill_count_at_start"])
                ),
                "q90_recovery_count": int(
                    self.state.dynamic_fill_hazard_recovery_count
                    - int(record["q90_recovery_count_at_start"])
                ),
                "q90_reentry_count": int(
                    self.state.dynamic_fill_hazard_reentry_count
                    - int(record["q90_reentry_count_at_start"])
                ),
                "loss_cooldown_trigger_count": int(
                    self.state.consecutive_loss_cooldown.trigger_count
                    - int(record["loss_cooldown_trigger_count_at_start"])
                ),
                "sync_degrade_trigger_count": int(
                    self.state.sync_adjust_degrade_trigger_count
                    - int(record["sync_degrade_trigger_count_at_start"])
                ),
            }
        )
        if self.state.lineage_outcome_contract_version == "v2":
            self._append_cooldown_lineage_event(
                record,
                "lineage_terminal",
                int(outcome_ts_ms),
                terminal_reason=str(terminal_reason),
                censored=int(bool(censored)),
            )
        if self.state.trace_variance_time_lineage is None:
            raise RuntimeError("randomized lineage trace is unavailable")
        self.state.trace_variance_time_lineage.append(record)
        self.state.variance_time_lineage_pending_inventory_lifecycle.setdefault(
            int(record["inventory_lifecycle_id"]), []
        ).append(record)
        return record

    def _finalize_cooldown_lineage_inventory_lifecycle(self,
        inventory_lifecycle_id: int,
        *,
        outcome_ts_ms: int,
        mark_px: float,
        closed: bool,
        terminal_reason: str,
    ) -> None:
        records = self.state.variance_time_lineage_pending_inventory_lifecycle.pop(
            int(inventory_lifecycle_id), []
        )
        if not records:
            return
        terminal_mtm = float(self.state.cash) + float(self.state.q) * float(mark_px)
        for record in records:
            if self.state.lineage_outcome_contract_version == "v2":
                inventory_lifecycle_start_equity = float(
                    record["inventory_lifecycle_start_equity_usdc"]
                )
                assignment_equity = float(record["assignment_equity_usdc"])
                lineage_terminal_equity = float(
                    record["lineage_terminal_equity_usdc"]
                )
                pre_assignment = assignment_equity - inventory_lifecycle_start_equity
                lineage_reward = lineage_terminal_equity - assignment_equity
                continuation = terminal_mtm - lineage_terminal_equity
                decision_to_terminal = terminal_mtm - assignment_equity
                identity_error = (
                    decision_to_terminal - lineage_reward - continuation
                )
                record.update(
                    {
                        "inventory_lifecycle_terminal_ts_ms": int(outcome_ts_ms),
                        "inventory_lifecycle_terminal_equity_usdc": float(terminal_mtm),
                        "pre_assignment_inventory_lifecycle_pnl_usdc": float(
                            pre_assignment
                        ),
                        "lineage_reward_usdc": float(lineage_reward),
                        "post_lineage_continuation_value_usdc": float(
                            continuation
                        ),
                        "decision_to_inventory_lifecycle_terminal_value_usdc": float(
                            decision_to_terminal
                        ),
                        "accounting_identity_error_usdc": float(
                            identity_error
                        ),
                    }
                )
            record.update(
                {
                    "inventory_lifecycle_outcome_ts_ms": int(outcome_ts_ms),
                    "inventory_lifecycle_closed": int(bool(closed)),
                    "inventory_lifecycle_censored": int(not closed),
                    "inventory_lifecycle_terminal_reason": str(terminal_reason),
                    "terminal_inventory_lifecycle_pnl": float(
                        terminal_mtm - float(record["inventory_lifecycle_start_pnl"])
                    ),
                    "inventory_lifecycle_mae": float(self.state.inventory_lifecycle_cur_min_pnl),
                    "inventory_lifecycle_max_abs_inventory": float(self.state.inventory_lifecycle_cur_max_abs),
                    "inventory_lifecycle_duration_s": max(
                        0.0,
                        float(
                            int(outcome_ts_ms)
                            - int(record["inventory_lifecycle_start_ts_ms"])
                        )
                        / 1_000.0,
                    ),
                    "decision_to_terminal_s": max(
                        0.0,
                        float(
                            int(outcome_ts_ms)
                            - int(record["decision_ts_ms"])
                        )
                        / 1_000.0,
                    ),
                    "repair_event": int(bool(closed)),
                }
            )
            if self.state.lineage_outcome_contract_version == "v2":
                self._append_cooldown_lineage_event(
                    record,
                    "inventory_lifecycle_terminal",
                    int(outcome_ts_ms),
                    terminal_reason=str(terminal_reason),
                    closed=int(bool(closed)),
                )
                for internal_key in (
                    "_event_seq",
                    "_episode_index",
                    "_last_policy_state",
                ):
                    record.pop(internal_key, None)

    def _start_cooldown_lineage(self,
        side: str,
        *,
        fill_ts_ms: int,
        prev_q: float,
        mark_px: float,
        consecutive_units: float,
        baseline_cooldown_ms: float,
    ) -> dict[str, Any]:
        if self.state.trace_variance_time_lineage is None:
            raise RuntimeError("randomized lineage trace is unavailable")
        in_flight = len(self.state.trace_variance_time_lineage) + len(
            self.state.variance_time_lineage_open_by_side
        )
        if in_flight >= self.state.trace_variance_time_lineage_max:
            raise RuntimeError(
                "trace_variance_time_lineage_max exhausted before day end"
            )
        normalized_side = str(side).upper()
        assignment_day = self.services.pd.to_datetime(
            int(fill_ts_ms), unit="ms", utc=True
        ).strftime("%Y-%m-%d")
        prospective_lineage_id = int(self.state.variance_time_lineage_next_id) + 1
        lineage_uid = (
            f"{self.services.SYMBOL}:{assignment_day}:{normalized_side}:{int(fill_ts_ms)}:"
            f"{prospective_lineage_id}"
        )
        if self.state.variance_time_lineage_stratified_randomizer is not None:
            (
                action,
                assignment_u,
                randomization_stratum,
            ) = self.state.variance_time_lineage_stratified_randomizer.assign(
                utc_day=assignment_day,
                side=normalized_side,
                pre_assignment_lineage_uid=lineage_uid,
            )
            randomization_block_index = -1
            randomization_block_position = -1
        else:
            action, assignment_u = self.services.choose_lineage_randomized_action(
                self.state.variance_time_lineage_rng,
                self.state.variance_time_lineage_probabilities,
            )
            randomization_stratum = f"{assignment_day}|{normalized_side}"
            randomization_block_index = -1
            randomization_block_position = -1
        self.state.variance_time_lineage_next_id = prospective_lineage_id
        self.state.variance_time_lineage_assignment_counts[action] += 1
        anticipated_inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        inventory_lifecycle_start_ts_at_assignment = int(self.state.inventory_lifecycle_start_ts)
        inventory_lifecycle_start_pnl_at_assignment = float(self.state.inventory_lifecycle_start_pnl)
        if not self.state.inventory_lifecycle_active and abs(float(prev_q)) < 1e-10:
            anticipated_inventory_lifecycle_id += 1
            inventory_lifecycle_start_ts_at_assignment = int(fill_ts_ms)
            inventory_lifecycle_start_pnl_at_assignment = float(self.state.cash) + float(self.state.q) * float(
                mark_px
            )
        if self.state.lineage_outcome_contract_version == "v2":
            if anticipated_inventory_lifecycle_id in self.state.variance_time_lineage_assigned_inventory_lifecycles:
                raise RuntimeError(
                    "lineage outcome contract v2 forbids a second assignment "
                    "before inventory_lifecycle terminal"
                )
            self.state.variance_time_lineage_assigned_inventory_lifecycles.add(anticipated_inventory_lifecycle_id)
        assignment_equity = float(self.state.cash) + float(self.state.q) * float(mark_px)
        assignment_budget_bps2 = (
            float(self.state.variance_time_reference_rate[normalized_side])
            * float(baseline_cooldown_ms)
            / 1_000.0
        )
        wall_ready_ts_ms = int(
            self.services.math.ceil(float(fill_ts_ms) + float(baseline_cooldown_ms))
        )
        record = {
            "schema_version": (
                "narrowgate_lineage_randomized_trace.v2"
                if self.state.lineage_outcome_contract_version == "v2"
                else "volatility_time_add_rearm_lineage.v1"
            ),
            "trace_schema_version": (
                "narrowgate_lineage_randomized_trace.v2"
                if self.state.lineage_outcome_contract_version == "v2"
                else "volatility_time_add_rearm_lineage.v1"
            ),
            "mechanism_extension_schema": (
                "variance_time_v1"
                if self.state.lineage_outcome_contract_version == "v2"
                else "legacy_variance_time_v1"
            ),
            "decision_id": (
                f"variance_time_lineage:{self.services.SYMBOL}:{int(fill_ts_ms)}:"
                f"{self.state.variance_time_lineage_next_id}:{normalized_side}"
            ),
            "lineage_uid": lineage_uid,
            "lineage_id": int(self.state.variance_time_lineage_next_id),
            "day": assignment_day,
            "decision_ts_ms": int(fill_ts_ms),
            "decision_ts_ns": int(fill_ts_ms) * 1_000_000,
            "assignment_ts_ms": int(fill_ts_ms),
            "side": normalized_side,
            "inventory_role": "add",
            "action": str(action),
            "behavior_propensity": float(
                self.state.variance_time_lineage_probabilities[action]
            ),
            "behavior_prob_fixed_wall_time_85n": float(
                self.state.variance_time_lineage_probabilities[self.services.LINEAGE_CONTROL_ACTION]
            ),
            "behavior_prob_variance_time_rearm": float(
                self.state.variance_time_lineage_probabilities[self.services.LINEAGE_CANDIDATE_ACTION]
            ),
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.variance_time_lineage_seed),
            "randomization_stratum": randomization_stratum,
            "randomization_block_index": int(randomization_block_index),
            "randomization_block_position": int(randomization_block_position),
            "assignment_before_downstream_path": 1,
            "assignment_fixed_within_lineage": 1,
            "trigger_fill_excluded_from_reward": 1,
            "inventory_lifecycle_id": int(anticipated_inventory_lifecycle_id),
            "inventory_lifecycle_start_ts_ms": int(inventory_lifecycle_start_ts_at_assignment),
            "inventory_lifecycle_start_pnl": float(inventory_lifecycle_start_pnl_at_assignment),
            "decision_mtm": assignment_equity,
            "decision_inventory": float(self.state.q),
            "assignment_inventory_btc": float(self.state.q),
            "inventory_lifecycle_age_at_assignment_ms": max(
                0,
                int(fill_ts_ms) - int(inventory_lifecycle_start_ts_at_assignment),
            ),
            "assignment_consecutive_same_side_fill_units": float(
                consecutive_units
            ),
            "terminal_consecutive_same_side_fill_units": float(
                consecutive_units
            ),
            "variance_budget_bps2_at_assignment": assignment_budget_bps2,
            "variance_budget_bps2_at_terminal": assignment_budget_bps2,
            "variance_accumulated_qv_bps2_prior_episodes": 0.0,
            "variance_accumulated_qv_bps2_at_terminal": 0.0,
            "wall_ready_ts_ms": wall_ready_ts_ms,
            "variance_ready_ts_ms": 0,
            "variance_ready_status": "censored_before_ready",
            "clock_direction": "censored_unknown",
            "first_clock_direction": "not_observed",
            "final_blocker": "not_observed",
            "final_action_change_ts_ms": 0,
            "final_action_change_status": "no_change",
            "lineage_terminal_ts_ms": 0,
            "inventory_lifecycle_terminal_ts_ms": 0,
            "inventory_lifecycle_start_equity_usdc": float(
                inventory_lifecycle_start_pnl_at_assignment
            ),
            "assignment_equity_usdc": assignment_equity,
            "lineage_terminal_equity_usdc": assignment_equity,
            "inventory_lifecycle_terminal_equity_usdc": assignment_equity,
            "pre_assignment_inventory_lifecycle_pnl_usdc": (
                assignment_equity - float(inventory_lifecycle_start_pnl_at_assignment)
            ),
            "lineage_reward_usdc": 0.0,
            "post_lineage_continuation_value_usdc": 0.0,
            "decision_to_inventory_lifecycle_terminal_value_usdc": 0.0,
            "accounting_identity_error_usdc": 0.0,
            "last_path_ts_ms": int(fill_ts_ms),
            "last_path_inventory": float(self.state.q),
            "inventory_time_btc_s": 0.0,
            "lineage_mae": 0.0,
            "lineage_max_abs_inventory": abs(float(self.state.q)),
            "intervention_fill_count": 0,
            "intervention_fill_qty": 0.0,
            "fill_value": 0.0,
            "fill_value_supported_count": 0,
            "fill_value_unsupported_count": 0,
            "fill_value_source_age_ms_max": 0,
            "fill_value_source": "none",
            "queue_cost": 0.0,
            "queue_reset_count": 0,
            "decision_cycle_count": 0,
            "eligible_decision_count": 0,
            "early_blocker_observation_count": 0,
            "mechanical_timing_change_count": 0,
            "candidate_unmasked_action_change_count": 0,
            "actual_final_action_change_count": 0,
            "support_valid": 1,
            "q90_cancel_request_count_at_start": int(
                self.state.dynamic_fill_hazard_cancel_request_count
            ),
            "q90_cancel_ack_count_at_start": int(
                self.state.dynamic_fill_hazard_cancel_ack_count
            ),
            "q90_pre_ack_fill_count_at_start": int(
                self.state.dynamic_fill_hazard_pre_ack_fill_count
            ),
            "q90_recovery_count_at_start": int(
                self.state.dynamic_fill_hazard_recovery_count
            ),
            "q90_reentry_count_at_start": int(
                self.state.dynamic_fill_hazard_reentry_count
            ),
            "loss_cooldown_trigger_count_at_start": int(
                self.state.consecutive_loss_cooldown.trigger_count
            ),
            "sync_degrade_trigger_count_at_start": int(
                self.state.sync_adjust_degrade_trigger_count
            ),
            "reward_definition": "decision_to_lineage_terminal_mtm_usdc",
            "fill_value_definition": "maker_signed_30s_book_mid_usdc",
            "inventory_lifecycle_cost_definition": (
                "fill_value_minus_queue_cost_minus_reward_residual"
            ),
            "full_cpp_tick_replay_authority": 0,
            "daily_fresh_start": 1,
            "_event_seq": 0,
            "_episode_index": 1,
        }
        self.state.variance_time_lineage_open_by_side[str(side).upper()] = record
        if self.state.lineage_outcome_contract_version == "v2":
            self._append_cooldown_lineage_event(
                record,
                "assignment",
                int(fill_ts_ms),
                assignment_u=float(assignment_u),
                behavior_propensity=float(
                    self.state.variance_time_lineage_probabilities[action]
                ),
            )
        return record

    def _cooldown_lineage_on_fill(self,
        side: str,
        *,
        prev_q: float,
        fill_ts_ms: int,
        fill_price: float,
        fill_qty: float,
        fee_rate: float,
        consecutive_units: float | None = None,
        baseline_cooldown_ms: float | None = None,
    ) -> None:
        if not self.state.variance_time_lineage_randomized_enabled:
            return
        normalized_side = str(side).upper()
        opposite_side = "SELL" if normalized_side == "BUY" else "BUY"
        mark_px = float(
            self._book_snapshot_at(int(fill_ts_ms), float(fill_price))[4]
        )
        opposite_record = self._cooldown_lineage_record(opposite_side)
        if opposite_record is not None:
            self._cooldown_lineage_add_fill_value(
                opposite_record,
                side=normalized_side,
                fill_ts_ms=int(fill_ts_ms),
                fill_price=float(fill_price),
                fill_qty=float(fill_qty),
                fee_rate=float(fee_rate),
            )
            self._finalize_cooldown_lineage(
                opposite_side,
                outcome_ts_ms=int(fill_ts_ms),
                mark_px=mark_px,
                terminal_reason="opposite_side_fill",
                censored=False,
            )
        same_record = self._cooldown_lineage_record(normalized_side)
        if same_record is not None:
            if consecutive_units is not None:
                same_record["terminal_consecutive_same_side_fill_units"] = float(
                    consecutive_units
                )
            self._cooldown_lineage_add_fill_value(
                same_record,
                side=normalized_side,
                fill_ts_ms=int(fill_ts_ms),
                fill_price=float(fill_price),
                fill_qty=float(fill_qty),
                fee_rate=float(fee_rate),
            )
        exposure_increasing = abs(float(self.state.q)) > abs(float(prev_q)) + 1e-10
        if exposure_increasing and same_record is None:
            if consecutive_units is None or baseline_cooldown_ms is None:
                raise RuntimeError(
                    "new randomized lineage lacks native cooldown state"
                )
            self._start_cooldown_lineage(
                normalized_side,
                fill_ts_ms=int(fill_ts_ms),
                prev_q=float(prev_q),
                mark_px=mark_px,
                consecutive_units=float(consecutive_units),
                baseline_cooldown_ms=float(baseline_cooldown_ms),
            )

    def _cooldown_lineage_observe_decision(self,
        side: str,
        now_ts_ms: int,
        *,
        can_post: bool,
        exposure_increasing: bool,
        block_reasons: list[str],
        side_ctx: Mapping[str, Any],
    ) -> None:
        if not self.state.variance_time_lineage_randomized_enabled or not exposure_increasing:
            return
        record = self._cooldown_lineage_record(side)
        if record is None:
            return
        side_last_fill_ts = self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        side_cooldown_ms = (
            self.state.last_buy_fill_cooldown_ms
            if side == "BUY"
            else self.state.last_sell_fill_cooldown_ms
        )
        side_units = self.state.consec_buy if side == "BUY" else self.state.consec_sell
        wall_active = bool(
            side_units > 1e-10
            and side_cooldown_ms > 0.0
            and int(now_ts_ms) - int(side_last_fill_ts) < side_cooldown_ms
        )
        episode = self._advance_variance_episode(side, int(now_ts_ms))
        candidate_active = bool(episode is not None and episode.active)
        reason_text = self._decision_reason_text(dict(side_ctx), list(block_reasons))
        record["final_blocker"] = str(reason_text or "none")
        if self.state.lineage_outcome_contract_version == "v2" and episode is not None:
            record["variance_budget_bps2_at_terminal"] = float(
                episode.budget_bps2
            )
            record["variance_accumulated_qv_bps2_at_terminal"] = float(
                record.get("variance_accumulated_qv_bps2_prior_episodes", 0.0)
            ) + float(episode.accumulated_qv_bps2)
            record["wall_ready_ts_ms"] = int(episode.baseline_ready_ts_ms)
            if episode.ready_ts_ms is not None:
                record["variance_ready_ts_ms"] = int(episode.ready_ts_ms)
                record["variance_ready_status"] = "observed"
        record["decision_cycle_count"] = int(
            record.get("decision_cycle_count", 0)
        ) + 1
        record["eligible_decision_count"] = int(
            record.get("eligible_decision_count", 0)
        ) + 1
        if self.state.lineage_outcome_contract_version == "v2":
            policy_state = (
                str(reason_text or "none"),
                int(wall_active),
                int(candidate_active),
                int(bool(can_post)),
            )
            if record.get("_last_policy_state") != policy_state:
                self._append_cooldown_lineage_event(
                    record,
                    "policy_state_change",
                    int(now_ts_ms),
                    blocker=policy_state[0],
                    wall_active=policy_state[1],
                    candidate_active=policy_state[2],
                    can_post=policy_state[3],
                )
                record["_last_policy_state"] = policy_state
        if wall_active == candidate_active:
            return
        record["mechanical_timing_change_count"] = int(
            record.get("mechanical_timing_change_count", 0)
        ) + 1
        clock_direction = "earlier" if wall_active else "later"
        if self.state.lineage_outcome_contract_version == "v2":
            record["clock_direction"] = clock_direction
            if str(record.get("first_clock_direction", "not_observed")) == "not_observed":
                record["first_clock_direction"] = clock_direction
        reason_tokens = set(reason_text.split("|"))
        reason_tokens.discard("none")
        other_blockers = reason_tokens - {"fill_cd"}
        candidate_unmasked = bool(
            (wall_active and not candidate_active and can_post)
            or (
                not wall_active
                and candidate_active
                and not can_post
                and not other_blockers
                and "fill_cd" in reason_tokens
            )
        )
        if candidate_unmasked:
            record["candidate_unmasked_action_change_count"] = int(
                record.get("candidate_unmasked_action_change_count", 0)
            ) + 1
            if str(record["action"]) == self.services.LINEAGE_CANDIDATE_ACTION:
                record["actual_final_action_change_count"] = int(
                    record.get("actual_final_action_change_count", 0)
                ) + 1
                if self.state.lineage_outcome_contract_version == "v2":
                    record["final_action_change_ts_ms"] = int(now_ts_ms)
                    record["final_action_change_status"] = "observed"
                    self._append_cooldown_lineage_event(
                        record,
                        "action_change",
                        int(now_ts_ms),
                        clock_direction=clock_direction,
                        blocker=str(reason_text or "none"),
                    )

    def _cooldown_lineage_observe_early_blocker(self,
        now_ts_ms: int,
        blocker: str,
    ) -> None:
        if not self.state.variance_time_lineage_randomized_enabled:
            return
        normalized_blocker = str(blocker).strip() or "unknown_early_blocker"
        for side, record in self.state.variance_time_lineage_open_by_side.items():
            episode = self._advance_variance_episode(side, int(now_ts_ms))
            record["final_blocker"] = normalized_blocker
            record["early_blocker_observation_count"] = int(
                record.get("early_blocker_observation_count", 0)
            ) + 1
            if self.state.lineage_outcome_contract_version == "v2":
                policy_state = (
                    normalized_blocker,
                    -1,
                    int(bool(episode is not None and episode.active)),
                    0,
                )
                if record.get("_last_policy_state") != policy_state:
                    self._append_cooldown_lineage_event(
                        record,
                        "policy_state_change",
                        int(now_ts_ms),
                        blocker=normalized_blocker,
                        wall_active=-1,
                        candidate_active=policy_state[2],
                        can_post=0,
                    )
                    record["_last_policy_state"] = policy_state
            if self.state.lineage_outcome_contract_version == "v2" and episode is not None:
                record["variance_budget_bps2_at_terminal"] = float(
                    episode.budget_bps2
                )
                record["variance_accumulated_qv_bps2_at_terminal"] = float(
                    record.get(
                        "variance_accumulated_qv_bps2_prior_episodes",
                        0.0,
                    )
                ) + float(episode.accumulated_qv_bps2)
                record["wall_ready_ts_ms"] = int(episode.baseline_ready_ts_ms)
                if episode.ready_ts_ms is not None:
                    record["variance_ready_ts_ms"] = int(episode.ready_ts_ms)
                    record["variance_ready_status"] = "observed"

    def _cooldown_rearm_on_fill(self, side, visible_ms, new_cooldown_ms):
        runtime = self.state.cooldown_rearm_runtime
        if runtime is None:
            return
        duration = (self.state.last_buy_fill_cooldown_ms if side == 'BUY'
                    else self.state.last_sell_fill_cooldown_ms)
        runtime.on_fill(side=side, visible_ms=int(visible_ms),
                        positive_cooldown=float(new_cooldown_ms) > 0,
                        natural_deadline_ms=float(visible_ms) + float(duration))

    def _cooldown_rearm_admission(self, side, original_blocks, *, mutate_state=True):
        runtime = getattr(self.state, 'cooldown_rearm_runtime', None)
        if runtime is None:
            return original_blocks
        compute = self.state.active_quote_compute
        if compute is None:
            raise ValueError('cooldown recovery requires the original split-compute clock')
        duration = (self.state.last_buy_fill_cooldown_ms if side == 'BUY'
                    else self.state.last_sell_fill_cooldown_ms)
        fill_ms = (self.state.last_buy_fill_ts if side == 'BUY'
                   else self.state.last_sell_fill_ts)
        return runtime.evaluate(side=side, now_ms=int(self.state.t),
            input_cutoff_ms=int(compute['prediction_cutoff_ms']),
            compute_ready_ms=int(self.state.t), original_blocks=bool(original_blocks),
            natural_deadline_ms=float(fill_ms) + float(duration) if duration > 0 else None,
            mutate_state=mutate_state)

    def _fill_cooldown_active(self,
        side: str,
        now_ts_ms: int,
        *,
        mutate_state: bool = True,
    ) -> bool:
        if self.state.fill_cooldown_clock_mode == "variance_time":
            episode = (
                self._advance_variance_episode(side, int(now_ts_ms))
                if mutate_state
                else self._variance_episode_for_side(side)
            )
            return bool(episode is not None and episode.active)
        if self.state.fill_cooldown_clock_mode == "randomized_lineage":
            record = self._cooldown_lineage_record(side)
            if (
                record is not None
                and str(record.get("action", ""))
                == self.services.LINEAGE_CANDIDATE_ACTION
            ):
                episode = (
                    self._advance_variance_episode(side, int(now_ts_ms))
                    if mutate_state
                    else self._variance_episode_for_side(side)
                )
                return bool(episode is not None and episode.active)
        if side == "BUY":
            return bool(
                self.state.consec_buy > 1e-10
                and self.state.last_buy_fill_cooldown_ms > 0.0
                and int(now_ts_ms) - self.state.last_buy_fill_ts
                < self.state.last_buy_fill_cooldown_ms
            )
        return bool(
            self.state.consec_sell > 1e-10
            and self.state.last_sell_fill_cooldown_ms > 0.0
            and int(now_ts_ms) - self.state.last_sell_fill_ts
            < self.state.last_sell_fill_cooldown_ms
        )

    def _variance_time_on_fill(self,
        side: str,
        *,
        prev_q: float,
        fill_ts_ms: int,
        consecutive_units: float,
        baseline_cooldown_ms: float,
    ) -> None:
        """Update the candidate clock while preserving baseline fill lineage."""

        if not self.state.variance_time_enabled:
            return
        prior_episode = self._variance_episode_for_side(side)
        lineage_record = self._cooldown_lineage_record(side)
        if side == "BUY":
            self.state.variance_episode_sell = None
            exposure_increasing = abs(float(self.state.q)) > abs(float(prev_q)) + 1e-10
        else:
            self.state.variance_episode_buy = None
            exposure_increasing = abs(float(self.state.q)) > abs(float(prev_q)) + 1e-10
        if not exposure_increasing:
            # Reducing fills still increment same-side units in the shared
            # baseline lineage, but do not restart an active add clock.
            return
        if float(baseline_cooldown_ms) <= 0.0:
            raise ValueError(
                "exposure-increasing variance-time fill lacks a baseline cooldown"
            )
        if prior_episode is not None and self.state.variance_rate_stream is not None:
            self.state.variance_rate_stream.advance(prior_episode, int(fill_ts_ms))
        episode = self.services.start_online_variance_time_episode(
            side=side,
            episode_start_ts_ms=int(fill_ts_ms),
            baseline_cooldown_ms=float(baseline_cooldown_ms),
            consecutive_same_side_fill_units=float(consecutive_units),
            reference_rate_bps2_per_s=float(self.state.variance_time_reference_rate[side]),
            minimum_wall_time_ms=self.state.variance_time_minimum_wall_time_ms,
            maximum_wall_time_ms=self.state.variance_time_maximum_wall_time_ms,
            max_feature_age_ms=self.state.variance_time_max_feature_age_ms,
        )
        if side == "BUY":
            self.state.variance_episode_buy = episode
        else:
            self.state.variance_episode_sell = episode
        if (
            self.state.lineage_outcome_contract_version == "v2"
            and lineage_record is not None
        ):
            if prior_episode is not None:
                self._append_cooldown_lineage_event(
                    lineage_record,
                    "episode_close",
                    int(fill_ts_ms),
                    close_reason="same_side_fill_restart",
                    accumulated_qv_bps2=float(
                        prior_episode.accumulated_qv_bps2
                    ),
                )
                lineage_record["variance_accumulated_qv_bps2_prior_episodes"] = (
                    float(
                        lineage_record.get(
                            "variance_accumulated_qv_bps2_prior_episodes",
                            0.0,
                        )
                    )
                    + float(prior_episode.accumulated_qv_bps2)
                )
                lineage_record["_episode_index"] = int(
                    lineage_record.get("_episode_index", 1)
                ) + 1
            lineage_record.update(
                {
                    "terminal_consecutive_same_side_fill_units": float(
                        consecutive_units
                    ),
                    "variance_budget_bps2_at_terminal": float(
                        episode.budget_bps2
                    ),
                    "wall_ready_ts_ms": int(episode.baseline_ready_ts_ms),
                    "variance_ready_ts_ms": 0,
                    "variance_ready_status": "censored_before_ready",
                    "clock_direction": "censored_unknown",
                }
            )
            self._append_cooldown_lineage_event(
                lineage_record,
                "episode_start",
                int(fill_ts_ms),
                consecutive_same_side_fill_units=float(consecutive_units),
                variance_budget_bps2=float(episode.budget_bps2),
                wall_ready_ts_ms=int(episode.baseline_ready_ts_ms),
            )

    def _variance_trace_state(self, side: str, now_ts_ms: int) -> dict[str, object]:
        episode = self._advance_variance_episode(side, int(now_ts_ms))
        if episode is None:
            return {
                "variance_time_episode_active": 0,
                "variance_time_budget_bps2": 0.0,
                "variance_time_accumulated_qv_bps2": 0.0,
                "variance_time_valid_interval_ms": 0.0,
                "variance_time_stale_frozen_ms": 0.0,
                "variance_time_baseline_ready_ts_ms": 0,
                "variance_time_candidate_ready_ts_ms": 0,
                "variance_time_release_reason": "none",
            }
        return {
            "variance_time_episode_active": int(episode.active),
            "variance_time_budget_bps2": float(episode.budget_bps2),
            "variance_time_accumulated_qv_bps2": float(
                episode.accumulated_qv_bps2
            ),
            "variance_time_valid_interval_ms": float(episode.valid_interval_ms),
            "variance_time_stale_frozen_ms": float(episode.stale_frozen_ms),
            "variance_time_baseline_ready_ts_ms": int(
                episode.baseline_ready_ts_ms
            ),
            "variance_time_candidate_ready_ts_ms": int(
                episode.ready_ts_ms or 0
            ),
            "variance_time_release_reason": str(
                episode.release_reason or "pending"
            ),
        }

    def _reducing_cooldown_vol_mult(self) -> float:
        if self.state.fill_cooldown_reducing_vol_ref <= 0.0:
            return 1.0
        vol = float(self.state.cur_vol or 0.0)
        if not self.services.math.isfinite(vol) or vol <= 0.0:
            return 1.0
        ratio = vol / max(self.state.fill_cooldown_reducing_vol_ref, 1e-12)
        return max(
            self.state.fill_cooldown_reducing_vol_min_mult,
            min(self.state.fill_cooldown_reducing_vol_max_mult, ratio),
        )

    def _reducing_cooldown_inventory_lifecycle_gate_active(self, prev_q: float, t_ms: int) -> bool:
        """Return whether a reducing fill should start the short cooldown.

        中文说明：减仓 cooldown 是风险状态下的节流工具，不是普通减仓的
        常驻阻断。这里用成交前库存和 inventory_lifecycle age 做 quote-time/replay-time
        可见条件，避免用成交后的结果反推。
        """
        if not self.state.fill_cooldown_reducing_inventory_lifecycle_only:
            return True
        abs_q = abs(float(prev_q or 0.0))
        inv_hit = (
            self.state.fill_cooldown_reducing_inv_threshold > 0.0
            and abs_q >= self.state.fill_cooldown_reducing_inv_threshold
        )
        ratio_hit = (
            self.state.fill_cooldown_reducing_inv_ratio > 0.0
            and abs_q / max(float(self.state.order_size or 0.0), self.state.LOT_SIZE) >= self.state.fill_cooldown_reducing_inv_ratio
        )
        age_s = (
            max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0)
            if self.state.inventory_lifecycle_active else 0.0
        )
        age_hit = self.state.fill_cooldown_reducing_age_s > 0.0 and age_s >= self.state.fill_cooldown_reducing_age_s
        return bool(inv_hit or ratio_hit or age_hit)

    def _cooldown_duration_fork_path_update(self, now_ts_ms: int, mark_px: float) -> None:
        if not self.state.cooldown_duration_fork_assigned:
            return
        timestamp = int(now_ts_ms)
        if timestamp < self.state.cooldown_duration_fork_path_last_ts_ms:
            raise RuntimeError("cooldown duration fork path clock regressed")
        elapsed_s = (
            float(timestamp - self.state.cooldown_duration_fork_path_last_ts_ms) / 1_000.0
        )
        self.state.cooldown_duration_fork_inventory_time_btc_s += (
            abs(float(self.state.cooldown_duration_fork_path_inventory_btc)) * elapsed_s
        )
        equity_delta = (
            float(self.state.cash)
            + float(self.state.q) * float(mark_px)
            - self.state.cooldown_duration_fork_assignment_equity_usdc
        )
        self.state.cooldown_duration_fork_mae_usdc = min(
            self.state.cooldown_duration_fork_mae_usdc,
            equity_delta,
        )
        self.state.cooldown_duration_fork_max_abs_inventory_btc = max(
            self.state.cooldown_duration_fork_max_abs_inventory_btc,
            abs(float(self.state.q)),
        )
        self.state.cooldown_duration_fork_path_last_ts_ms = timestamp
        self.state.cooldown_duration_fork_path_inventory_btc = float(self.state.q)

    def _cooldown_v2_capture_assignment_snapshot(self,
        *,
        side: str,
        prev_q: float,
        consecutive_units: float,
        fill_ts_ms: int,
        baseline_duration_ms: float,
        previous_fill_ts_ms: int,
        previous_cooldown_ms: float,
        order: Optional[dict],
        queue_ahead_before_fill_btc: Optional[float],
        exposure_fill_ordinal: int,
        anticipated_inventory_lifecycle_id: int,
    ) -> dict[str, Any] | None:
        if self.state.cooldown_v2_snapshot_emitter is None:
            return None
        if order is None:
            raise RuntimeError("cooldown v2 snapshot requires an exact order")
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise RuntimeError("cooldown v2 snapshot side is invalid")
        order_id = int(order.get("trace_id", -1))
        if order_id < 0:
            raise RuntimeError("cooldown v2 snapshot lacks an order identity")
        fill_qty = abs(float(self.state.q) - float(prev_q))
        order_qty = float(order.get("quantity", 0.0) or 0.0)
        remaining_after = max(0.0, float(order.get("remaining", 0.0) or 0.0))
        cumulative_after = max(0.0, order_qty - remaining_after)
        cumulative_before = max(0.0, cumulative_after - fill_qty)
        partial_ordinal = int(
            order.get("_cooldown_v2_partial_fill_ordinal", 0) or 0
        ) + 1
        order["_cooldown_v2_partial_fill_ordinal"] = partial_ordinal

        queue_seed_status = str(
            order.get("_exchange_book_queue_seed_status", "") or ""
        )
        queue_path_valid = bool(
            order.get("exchange_book_queue_path_valid", False)
        ) and not bool(order.get("exchange_book_queue_ambiguous", False))
        if (
            self.state.exchange_book_queue_mode == "strict"
            and queue_path_valid
            and queue_seed_status in {"exact", "known_zero"}
        ):
            queue_state = queue_seed_status
            queue_ahead: float | None = (
                0.0
                if queue_seed_status == "known_zero"
                else max(0.0, float(queue_ahead_before_fill_btc or 0.0))
            )
        else:
            queue_state = "unknown"
            queue_ahead = None

        target_price_tick = self.services._order_price_tick(order, self.state.TICK)
        target_displayed_status = "unknown"
        target_displayed_qty: float | None = None
        if self.state.exec_message_schedules:
            visible_depth_index, visible_depth_ns, _ = self._message_clock_state(
                "depth", int(fill_ts_ms)
            )
            if visible_depth_index >= 0:
                displayed = self._l2_visible_queue_ahead(
                    normalized_side,
                    float(order["price"]),
                    target_ts=visible_depth_ns // 1_000_000,
                )
                if displayed is not None and self.services.math.isfinite(displayed):
                    target_displayed_qty = max(0.0, float(displayed))
                    target_displayed_status = (
                        "known_zero" if target_displayed_qty == 0.0 else "exact"
                    )
        elif self.state.exchange_book_scheduler is not None and queue_path_valid:
            target_lookup = self.state.exchange_book_scheduler.lookup(
                normalized_side,
                target_price_tick,
            )
            target_lookup_causal = (
                int(target_lookup.asof_exchange_ts_ns)
                <= int(fill_ts_ms) * 1_000_000
            )
            if target_lookup_causal and bool(target_lookup.strict_usable):
                if str(target_lookup.status) == "known_zero":
                    target_displayed_status = "known_zero"
                    target_displayed_qty = 0.0
                elif (
                    str(target_lookup.status) == "exact"
                    and target_lookup.quantity is not None
                    and self.services.math.isfinite(float(target_lookup.quantity))
                ):
                    target_displayed_status = "exact"
                    target_displayed_qty = max(
                        0.0, float(target_lookup.quantity)
                    )

        same_fill_count_before = (
            int(self.state.nfb) - 1 if normalized_side == "BUY" else int(self.state.nfa) - 1
        )
        opposite_fill_count = int(self.state.nfa) if normalized_side == "BUY" else int(self.state.nfb)
        opposite_fill_ts_ms = (
            int(self.state.last_sell_fill_ts)
            if normalized_side == "BUY"
            else int(self.state.last_buy_fill_ts)
        )
        same_fill_age_s = (
            None
            if same_fill_count_before <= 0
            else max(
                0.0,
                float(int(fill_ts_ms) - int(previous_fill_ts_ms)) / 1_000.0,
            )
        )
        opposite_fill_age_s = (
            None
            if opposite_fill_count <= 0
            else max(
                0.0,
                float(int(fill_ts_ms) - opposite_fill_ts_ms) / 1_000.0,
            )
        )
        previous_deadline_ts_ms = int(
            round(float(previous_fill_ts_ms) + float(previous_cooldown_ms))
        )
        cooldown_remaining_ms = max(
            0.0, float(previous_deadline_ts_ms - int(fill_ts_ms))
        )
        lineage_revision_before = int(
            self.state.cooldown_v2_lineage_revision[normalized_side]
        )
        lineage_revision = lineage_revision_before + 1
        cooldown_owner = (
            f"{normalized_side.lower()}-lineage-{lineage_revision_before}"
            if cooldown_remaining_ms > 0.0 and lineage_revision_before > 0
            else "none"
        )
        role_at_fill = "opener" if abs(float(prev_q)) < 1e-10 else "add"
        prior_inventory_lifecycle_add_count = (
            max(0, int(self.state.inventory_lifecycle_exposure_increasing_fills) - 1)
            if self.state.inventory_lifecycle_active
            else 0
        )
        fill_visible_ts_ns = int(fill_ts_ms) * 1_000_000
        m0_context = {
            "assignment_ts_ns": fill_visible_ts_ns,
            "fill_visible_ts_ns": fill_visible_ts_ns,
            "side": normalized_side,
            "role_at_fill": role_at_fill,
            "inventory_before_fill_btc": float(prev_q),
            "inventory_after_fill_btc": float(self.state.q),
            "fill_qty_btc": float(fill_qty),
            "order_qty_btc": float(order_qty),
            "cumulative_filled_qty_before_btc": float(cumulative_before),
            "cumulative_filled_qty_after_btc": float(cumulative_after),
            "remaining_order_qty_after_btc": float(remaining_after),
            "partial_fill_ordinal": partial_ordinal,
            "fill_is_partial": bool(remaining_after > 1e-12),
            "order_age_s": max(
                0.0,
                float(int(fill_ts_ms) - int(order.get("activate_ts", fill_ts_ms)))
                / 1_000.0,
            ),
            "queue_ahead_before_fill_btc": queue_ahead,
            "queue_state_before_fill": queue_state,
            "target_price_tick": int(target_price_tick),
            "target_price_displayed_qty_btc": target_displayed_qty,
            "target_price_displayed_qty_status": target_displayed_status,
            "target_price_displayed_qty_known": bool(
                target_displayed_status in {"exact", "known_zero"}
            ),
            "target_price_displayed_qty_is_queue_ahead": False,
            "consecutive_units_after": float(consecutive_units),
            "baseline_duration_ms": float(baseline_duration_ms),
            "inventory_lifecycle_age_s": (
                max(0.0, float(int(fill_ts_ms) - self.state.inventory_lifecycle_start_ts) / 1_000.0)
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "inventory_lifecycle_add_count": int(prior_inventory_lifecycle_add_count),
            "inventory_lifecycle_mae_to_date_usdc": (
                float(self.state.inventory_lifecycle_cur_min_pnl) if self.state.inventory_lifecycle_active else 0.0
            ),
            "inventory_lifecycle_inventory_time_to_date_btc_s": (
                float(self.state.inventory_lifecycle_inventory_time_to_date_btc_s)
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "last_same_side_fill_age_s": same_fill_age_s,
            "last_opposite_side_fill_age_s": opposite_fill_age_s,
            "cooldown_remaining_ms": float(cooldown_remaining_ms),
            "cooldown_blocker_active": bool(cooldown_remaining_ms > 0.0),
            "cooldown_lineage_revision_before": lineage_revision_before,
            "cooldown_deadline_owner": cooldown_owner,
        }
        assignment_id = (
            f"cooldown-v2:{self.services.SYMBOL}:{fill_ts_ms}:{normalized_side}:"
            f"{order_id}:{exposure_fill_ordinal}"
        )
        snapshot = self.state.cooldown_v2_snapshot_emitter.capture_exposure_fill(
            assignment_id=assignment_id,
            fill_event_id=(
                f"fill:{order_id}:{partial_ordinal}:{fill_ts_ms}:"
                f"{exposure_fill_ordinal}"
            ),
            client_order_id=f"replay-order-{order_id}",
            lineage_id=f"cooldown-{normalized_side.lower()}-lineage",
            lineage_revision=lineage_revision,
            partial_fill_ordinal=partial_ordinal,
            partial_fill_qty_btc=float(fill_qty),
            fill_exchange_ts_ns=int(
                order.get("_callback_exchange_ts_ms", fill_ts_ms)
            ) * 1_000_000,
            fill_visible_ts_ns=fill_visible_ts_ns,
            m0_context=m0_context,
        )
        self.state.cooldown_v2_lineage_revision[normalized_side] = lineage_revision
        receipt = {
            "snapshot_id": str(snapshot.snapshot_id),
            "assignment_id": str(snapshot.assignment_id),
            "side": normalized_side,
            "role_at_fill": role_at_fill,
            "inventory_lifecycle_id": int(anticipated_inventory_lifecycle_id),
            "exposure_fill_ordinal": int(exposure_fill_ordinal),
            "partial_fill_ordinal": int(partial_ordinal),
            "policy_input_valid": bool(snapshot.policy_input_valid),
            "fallback_policy_id": snapshot.fallback_policy_id,
            "fallback_reason": snapshot.fallback_reason,
            "source_bundle_sha256": str(snapshot.source_bundle_sha256),
        }
        self.state.cooldown_v2_snapshot_receipts.append(receipt)
        return receipt, snapshot

    def _cooldown_duration_for_exposure_fill(self,
        *,
        side: str,
        prev_q: float,
        consecutive_units: float,
        fill_ts_ms: int,
        baseline_duration_ms: float,
        previous_fill_ts_ms: int,
        previous_cooldown_ms: float,
        order: Optional[dict],
        queue_ahead_before_fill_btc: Optional[float],
    ) -> float:

        normalized_side = str(side).upper()
        exposure_increasing = (
            (normalized_side == "BUY" and float(prev_q) >= 0.0)
            or (normalized_side == "SELL" and float(prev_q) <= 0.0)
        )
        if not exposure_increasing:
            return float(baseline_duration_ms)
        baseline_ms = float(baseline_duration_ms)
        if not self.services.math.isfinite(baseline_ms) or baseline_ms <= 0.0:
            raise RuntimeError(
                "exposure-increasing fill lacks a positive baseline cooldown"
            )
        self.state.cooldown_duration_exposure_fill_ordinal += 1
        ordinal = int(self.state.cooldown_duration_exposure_fill_ordinal)
        order_id = int((order or {}).get("trace_id", -1))
        anticipated_inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        if not self.state.inventory_lifecycle_active and abs(float(prev_q)) < 1e-10:
            anticipated_inventory_lifecycle_id += 1
        fill_qty = abs(float(self.state.q) - float(prev_q))
        units_added = fill_qty / max(float(self.state.order_size), float(self.state.LOT_SIZE))
        prior_deadline_ts_ms = int(
            round(float(previous_fill_ts_ms) + float(previous_cooldown_ms))
        )
        callback_book = (
            self._decision_visible_book(
                int(fill_ts_ms), float("nan"), advance_visibility_state=False
            )
            if self.state.exec_message_schedules else None
        )
        opportunity = {
            "schema_version": (
                "multiscale_ema_boolean_cooldown_duration_opportunity.v1"
            ),
            "exposure_fill_ordinal": ordinal,
            "fill_visible_ts_ms": int(fill_ts_ms),
            "fill_exchange_ts_ms": int(
                (order or {}).get("_callback_exchange_ts_ms", fill_ts_ms)
            ),
            "fill_clock_semantics": (
                "exchange_match_and_sampled_local_private_visibility"
                if self.state.private_fill_visibility_enabled else
                "native_exchange_event_revealed_at_replay_event_clock_"
                "no_live_receive_time_claim"
            ),
            "live_receive_time_authority": False,
            "side": normalized_side,
            "role_at_fill": (
                "opener" if abs(float(prev_q)) < 1e-10 else "add"
            ),
            "order_id": order_id,
            "inventory_lifecycle_id": anticipated_inventory_lifecycle_id,
            "inventory_before_fill_btc": float(prev_q),
            "inventory_after_fill_btc": float(self.state.q),
            "fill_qty_btc": fill_qty,
            "unit_qty_btc": max(float(self.state.order_size), float(self.state.LOT_SIZE)),
            "consecutive_units_before": max(
                0.0, float(consecutive_units) - units_added
            ),
            "consecutive_units_after": float(consecutive_units),
            "prior_deadline_ts_ms": prior_deadline_ts_ms,
            "baseline_duration_ms": baseline_ms,
            "baseline_deadline_ts_ms": int(round(float(fill_ts_ms) + baseline_ms)),
            "canonical_mid": float(callback_book["mid"] if callback_book else self.state.mid),
            "best_bid": float(callback_book["best_bid"] if callback_book else self.state.cur_best_bid),
            "best_ask": float(callback_book["best_ask"] if callback_book else self.state.cur_best_ask),
            "decision_visible_bbo_index": int(
                callback_book["bbo_idx"] if callback_book else
                self.state.active_decision_bbo_idx
                if self.state.active_decision_bbo_idx is not None
                else self.state.bbo_idx
            ),
            "decision_visible_l2_index": int(
                callback_book["l2_idx"] if callback_book else self.state.l2_idx
            ),
            "market_event_index": int(self.state.i),
            "assignment_equity_usdc": float(self.state.cash) + float(self.state.q) * float(self.state.mid),
        }
        snapshot_capture = self._cooldown_v2_capture_assignment_snapshot(
            side=normalized_side,
            prev_q=float(prev_q),
            consecutive_units=float(consecutive_units),
            fill_ts_ms=int(fill_ts_ms),
            baseline_duration_ms=float(baseline_ms),
            previous_fill_ts_ms=int(previous_fill_ts_ms),
            previous_cooldown_ms=float(previous_cooldown_ms),
            order=order,
            queue_ahead_before_fill_btc=queue_ahead_before_fill_btc,
            exposure_fill_ordinal=ordinal,
            anticipated_inventory_lifecycle_id=anticipated_inventory_lifecycle_id,
        )
        snapshot_receipt = (
            snapshot_capture[0] if snapshot_capture is not None else None
        )
        assignment_snapshot = (
            snapshot_capture[1] if snapshot_capture is not None else None
        )
        repeated_policy_applied_ms: float | None = None
        if self.state.cooldown_duration_policy_evaluator is not None:
            if assignment_snapshot is None or snapshot_receipt is None:
                raise RuntimeError(
                    "repeated cooldown policy lacks an atomic assignment snapshot"
                )
            raw_decision = self.state.cooldown_duration_policy_evaluator.evaluate(
                assignment_snapshot,
                baseline_duration_ms=baseline_ms,
            )
            required_decision_fields = {
                "action_id",
                "duration_ms",
                "fallback_reason",
                "matched_rule_index",
                "policy_sha256",
                "predicate_bundle_sha256",
                "snapshot_id",
                "support_valid",
            }
            missing_decision_fields = sorted(
                field
                for field in required_decision_fields
                if not hasattr(raw_decision, field)
            )
            if missing_decision_fields:
                raise RuntimeError(
                    "cooldown duration policy decision is incomplete: "
                    f"{missing_decision_fields}"
                )
            repeated_policy_applied_ms = float(raw_decision.duration_ms)
            if (
                not self.services.math.isfinite(repeated_policy_applied_ms)
                or repeated_policy_applied_ms <= 0.0
            ):
                raise RuntimeError(
                    "cooldown duration policy returned an invalid duration"
                )
            if str(raw_decision.snapshot_id) != str(
                snapshot_receipt["snapshot_id"]
            ):
                raise RuntimeError(
                    "cooldown duration policy decision snapshot drifted"
                )
            decision_payload = {
                "schema_version": (
                    "causal_multichannel_window_boolean_cooldown_"
                    "repeated_policy_decision.v1"
                ),
                "exposure_fill_ordinal": ordinal,
                "fill_visible_ts_ms": int(fill_ts_ms),
                "side": normalized_side,
                "role_at_fill": str(opportunity["role_at_fill"]),
                "inventory_lifecycle_id": anticipated_inventory_lifecycle_id,
                "order_id": order_id,
                "baseline_duration_ms": baseline_ms,
                "action_id": str(raw_decision.action_id),
                "duration_ms": repeated_policy_applied_ms,
                "fallback_reason": (
                    None
                    if raw_decision.fallback_reason is None
                    or str(raw_decision.fallback_reason).strip() == ""
                    else str(raw_decision.fallback_reason)
                ),
                "matched_rule_index": (
                    None
                    if raw_decision.matched_rule_index is None
                    else int(raw_decision.matched_rule_index)
                ),
                "policy_sha256": str(raw_decision.policy_sha256),
                "predicate_bundle_sha256": str(
                    raw_decision.predicate_bundle_sha256
                ),
                "snapshot_id": str(raw_decision.snapshot_id),
                "support_valid": bool(raw_decision.support_valid),
            }
            self.state.cooldown_duration_policy_decisions.append(decision_payload)
            opportunity.update(
                {
                    "repeated_policy_action_id": str(raw_decision.action_id),
                    "repeated_policy_duration_ms": repeated_policy_applied_ms,
                    "repeated_policy_policy_sha256": str(
                        raw_decision.policy_sha256
                    ),
                    "repeated_policy_predicate_bundle_sha256": str(
                        raw_decision.predicate_bundle_sha256
                    ),
                    "repeated_policy_support_valid": bool(
                        raw_decision.support_valid
                    ),
                    "repeated_policy_fallback_reason": str(
                        raw_decision.fallback_reason
                    ),
                }
            )
        if self.state.cooldown_duration_shared_prefix_dispatch_enabled:
            if snapshot_receipt is None:
                raise RuntimeError(
                    "shared-prefix opportunity lacks an atomic assignment snapshot"
                )
            opportunity.update(
                {
                    "cooldown_v2_snapshot_id": str(
                        snapshot_receipt["snapshot_id"]
                    ),
                    "partial_fill_ordinal": int(
                        snapshot_receipt["partial_fill_ordinal"]
                    ),
                    "cooldown_v2_source_bundle_sha256": str(
                        snapshot_receipt["source_bundle_sha256"]
                    ),
                    "exchange_book_queue_mode": str(
                        self.state.exchange_book_queue_mode
                    ),
                    "exchange_book_queue_scope": (
                        "strategy_independent_native_snapshot_delta_exchange_time_v1"
                        if self.state.exchange_book_queue_mode == "strict"
                        and self.state.exchange_book_scheduler is not None
                        else "not_strict_native"
                    ),
                    "strict_counter_baseline": (
                        self._exchange_book_strict_counter_snapshot()
                    ),
                    "exchange_book_queue_missing_trace_cursor": len(
                        self.state.exchange_book_queue_missing_trace
                    ),
                    "exchange_book_queue_missing_count_at_assignment": int(
                        self.state.exchange_book_queue_missing_count
                    ),
                }
            )
            if bool(snapshot_receipt["policy_input_valid"]):
                arm_selection = (
                    self.state.cooldown_duration_shared_prefix_executor.dispatch(opportunity)
                )
                if arm_selection is not None:
                    # The arm result must contain assignment-to-washout
                    # ambiguity evidence only. Prefix counters remain bound in
                    # the immutable opportunity contract.
                    self.state.exchange_book_queue_ambiguity_trace.clear()
                    self.state.cooldown_duration_shared_prefix_dispatch_enabled = False
                    self.state.cooldown_duration_fork_enabled = True
                    self.state.cooldown_duration_fork_action = str(arm_selection.action)
                    self.state.cooldown_duration_fork_target_ordinal = ordinal
                    self.state.cooldown_duration_fork_target_ts_ms = int(fill_ts_ms)
                    self.state.cooldown_duration_fork_target_side = normalized_side
                    self.state.cooldown_duration_fork_target_order_id = order_id
                    self.state.cooldown_duration_fork_target_inventory_lifecycle_id = anticipated_inventory_lifecycle_id
                    self.state.cooldown_duration_fork_expected_baseline_ms = baseline_ms
                    self.state.cooldown_duration_fork_fixed_ms = float(
                        arm_selection.fixed_duration_ms
                    )
                    if arm_selection.exact_owner_policy_sha256 is not None:
                        if (
                            arm_selection.exact_owner_action is None
                            or repeated_policy_applied_ms is None
                        ):
                            raise RuntimeError(
                                "shared-prefix exact-owner arm lacks its target decision"
                            )
                        self.state.cooldown_duration_fork_baseline_policy_enabled = True
                        self.state.cooldown_duration_fork_expected_owner_action = str(
                            arm_selection.exact_owner_action
                        )
                        self.state.cooldown_duration_fork_expected_owner_policy_sha256 = str(
                            arm_selection.exact_owner_policy_sha256
                        )
                    else:
                        self.state.cooldown_v2_snapshot_emitter = None
        if self.state.trace_cooldown_duration_opportunities is not None:
            if len(self.state.trace_cooldown_duration_opportunities) >= self.state.cooldown_duration_trace_max:
                raise RuntimeError(
                    "cooldown duration opportunity trace limit exhausted"
                )
            self.state.trace_cooldown_duration_opportunities.append(opportunity)

        target_matches = bool(
            self.state.cooldown_duration_fork_enabled
            and ordinal == self.state.cooldown_duration_fork_target_ordinal
            and int(fill_ts_ms) == self.state.cooldown_duration_fork_target_ts_ms
            and normalized_side == self.state.cooldown_duration_fork_target_side
            and order_id == self.state.cooldown_duration_fork_target_order_id
            and anticipated_inventory_lifecycle_id
            == self.state.cooldown_duration_fork_target_inventory_lifecycle_id
        )
        if self.state.cooldown_duration_fork_enabled and not target_matches:
            if (
                self.state.cooldown_duration_fork_assigned
                and normalized_side == self.state.cooldown_duration_fork_target_side
            ):
                # A later exposure fill creates a new baseline lineage.  The
                # one-shot target override must not leak into it.
                self.state.cooldown_duration_target_override_active = False
            if repeated_policy_applied_ms is not None:
                return repeated_policy_applied_ms
            return baseline_ms
        if not self.state.cooldown_duration_fork_enabled:
            if repeated_policy_applied_ms is not None:
                return repeated_policy_applied_ms
            return baseline_ms
        if self.state.cooldown_duration_fork_assigned:
            raise RuntimeError("cooldown duration fork attempted a second assignment")
        if not self.services.math.isclose(
            baseline_ms,
            self.state.cooldown_duration_fork_expected_baseline_ms,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise RuntimeError("cooldown duration fork baseline duration drifted")
        self.state.cooldown_duration_fork_assigned = True
        self.state.cooldown_duration_target_override_active = True
        self.state.cooldown_duration_fork_assignment_ts_ms = int(fill_ts_ms)
        self.state.cooldown_duration_fork_assignment_equity_usdc = float(
            opportunity["assignment_equity_usdc"]
        )
        self.state.cooldown_duration_fork_assignment_inventory_btc = float(self.state.q)
        self.state.cooldown_duration_fork_baseline_ms = baseline_ms
        self.state.cooldown_duration_fork_exact_owner_baseline_ms = (
            float(repeated_policy_applied_ms)
            if repeated_policy_applied_ms is not None
            else baseline_ms
        )
        self.state.cooldown_duration_fork_exact_owner_action = (
            str(raw_decision.action_id)
            if repeated_policy_applied_ms is not None
            else "CONTROL_85N"
        )
        if self.state.cooldown_duration_fork_baseline_policy_enabled and (
            self.state.cooldown_duration_fork_exact_owner_action
            != self.state.cooldown_duration_fork_expected_owner_action
        ):
            raise RuntimeError(
                "one-shot exact-owner target action drifted from the admitted panel: "
                f"expected={self.state.cooldown_duration_fork_expected_owner_action}, "
                f"observed={self.state.cooldown_duration_fork_exact_owner_action}, "
                f"side={normalized_side}, fill_ts_ms={int(fill_ts_ms)}, "
                f"order_id={order_id}, inventory_lifecycle_id={anticipated_inventory_lifecycle_id}, "
                f"baseline_ms={baseline_ms}, "
                "exact_owner_baseline_ms="
                f"{self.state.cooldown_duration_fork_exact_owner_baseline_ms}"
            )
        self.state.cooldown_duration_fork_applied_ms = (
            baseline_ms
            if self.state.cooldown_duration_fork_action == "CONTROL_85N"
            else self.state.cooldown_duration_fork_fixed_ms
        )
        self.state.cooldown_duration_fork_deadline_ts_ms = int(
            round(
                float(fill_ts_ms)
                + float(self.state.cooldown_duration_fork_applied_ms)
            )
        )
        self.state.cooldown_duration_target_control_deadline_ts_ms = int(
            round(
                float(fill_ts_ms)
                + float(self.state.cooldown_duration_fork_exact_owner_baseline_ms)
            )
        )
        self.state.cooldown_duration_fork_assignment_buy_fills = int(self.state.nfb)
        self.state.cooldown_duration_fork_assignment_sell_fills = int(self.state.nfa)
        self.state.cooldown_duration_fork_path_last_ts_ms = int(fill_ts_ms)
        self.state.cooldown_duration_fork_path_inventory_btc = float(self.state.q)
        self.state.cooldown_duration_fork_mae_usdc = 0.0
        self.state.cooldown_duration_fork_max_abs_inventory_btc = abs(float(self.state.q))
        self.state.cooldown_duration_fork_reason = "assigned"
        return float(self.state.cooldown_duration_fork_applied_ms)

    def _cooldown_ms_for_fill(self,
        side: str,
        prev_q: float,
        consec_units: float,
        t_ms: int,
        *,
        previous_fill_ts_ms: int,
        previous_cooldown_ms: float,
        order: Optional[dict] = None,
        queue_ahead_before_fill_btc: Optional[float] = None,
    ) -> float:
        """Choose cooldown at fill time, matching live MakerEngine semantics.

        中文说明：不要在 quote 时用“当前库存方向”重算 cooldown 类型。
        一笔 SELL 如果刚好把 long 减到 flat，它仍然应该使用减仓短 cooldown，
        而不是因为 q=0 被误判成下一次 SELL 加仓而套用完整 fill_cooldown。
        """
        if (
            self.state.cooldown_duration_fork_assigned
            and str(side).upper() != self.state.cooldown_duration_fork_target_side
        ):
            # The frozen control ABI clears the opposite-side deadline.
            self.state.cooldown_duration_target_override_active = False
        exposure_increasing_fill = (
            (side == "BUY" and prev_q >= 0.0)
            or (side == "SELL" and prev_q <= 0.0)
        )
        if exposure_increasing_fill or self.state.fill_cooldown_apply_reducing:
            base_ms = float(self.state.fill_cooldown_ms)
            if exposure_increasing_fill and self.state.adaptive_add_cooldown_cfg.enabled:
                diag = (order or {}).get("quote_diag") or {}
                ema = self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask
                try:
                    pred_ret = float(diag.get("pred_ret", self.state.cur_ret) or 0.0)
                except Exception:
                    pred_ret = float(self.state.cur_ret or 0.0)
                side_adverse_ret = max(0.0, -pred_ret) if side == "BUY" else max(0.0, pred_ret)
                try:
                    refill_edge = float(diag.get("l2_book_refresh_ratio", 0.0) or 0.0) - float(
                        diag.get("l2_book_cancel_ratio", 0.0) or 0.0
                    )
                except Exception:
                    refill_edge = 0.0
                inventory_lifecycle_age_s = (
                    max(0.0, float(t_ms - self.state.inventory_lifecycle_start_ts) / 1000.0)
                    if self.state.inventory_lifecycle_active else 0.0
                )
                trend_norm = 0.0
                if self.state.adaptive_add_cooldown_cfg.trend_ret_ref > 0.0:
                    trend_norm = min(
                        1.0,
                        side_adverse_ret / max(self.state.adaptive_add_cooldown_cfg.trend_ret_ref, 1e-12),
                    )
                good_refill_norm = 0.0
                if self.state.adaptive_add_cooldown_cfg.refill_ref > 0.0:
                    good_refill_norm = min(
                        1.0,
                        max(0.0, refill_edge) / max(self.state.adaptive_add_cooldown_cfg.refill_ref, 1e-12),
                    )
                micro_reversion_score = good_refill_norm * (1.0 - trend_norm)
                add_mult = self.services.adaptive_add_cooldown_multiplier(
                    side=side,
                    side_markout_ema=ema,
                    consec_units=consec_units,
                    prev_inventory=prev_q,
                    max_inventory=self.state.max_inv,
                    inventory_lifecycle_age_s=inventory_lifecycle_age_s,
                    side_adverse_ret=side_adverse_ret,
                    refill_edge=refill_edge,
                    micro_reversion_score=micro_reversion_score,
                    cfg=self.state.adaptive_add_cooldown_cfg,
                )
                if abs(add_mult - 1.0) > 1e-12:
                    self.state.adaptive_add_cooldown_hit_count += 1
                    if side == "BUY":
                        self.state.adaptive_add_cooldown_bid_hit_count += 1
                    else:
                        self.state.adaptive_add_cooldown_ask_hit_count += 1
                    self.state.adaptive_add_cooldown_trace.append(
                        {
                            "t_ms": int(t_ms),
                            "side": side,
                            "order_id": int((order or {}).get("trace_id", -1)),
                            "quote_ts": int((order or {}).get("quote_ts", t_ms) or t_ms),
                            "price": float((order or {}).get("price", 0.0) or 0.0),
                            "mid_at_quote": float((order or {}).get("mid_at_quote", 0.0) or 0.0),
                            "prev_inventory": float(prev_q),
                            "post_inventory": float(self.state.q),
                            "fill_qty_abs": float(abs(self.state.q - prev_q)),
                            "consec_units": float(consec_units),
                            "inventory_lifecycle_age_s": float(inventory_lifecycle_age_s),
                            "side_markout_ema": float(ema),
                            "side_adverse_ret": float(side_adverse_ret),
                            "refill_edge": float(refill_edge),
                            "micro_reversion_score": float(micro_reversion_score),
                            "adaptive_add_cooldown_mult": float(add_mult),
                            "adaptive_add_cooldown_gate_side": str(self.state.adaptive_add_cooldown_cfg.gate_side),
                            "gate_inventory_lifecycle_score": float(self.state.adaptive_add_cooldown_cfg.gate_inventory_lifecycle_score),
                            "gate_trend_score": float(self.state.adaptive_add_cooldown_cfg.gate_trend_score),
                            "gate_refill_edge_max": float(self.state.adaptive_add_cooldown_cfg.gate_refill_edge_max),
                            "gate_reversion_max": float(self.state.adaptive_add_cooldown_cfg.gate_reversion_max),
                            "pred_ret": float(pred_ret),
                            "l2_book_refresh_ratio": float(diag.get("l2_book_refresh_ratio", 0.0) or 0.0),
                            "l2_book_cancel_ratio": float(diag.get("l2_book_cancel_ratio", 0.0) or 0.0),
                            "l2_near_depth_total": float(
                                diag.get("l2_near_depth_total", diag.get("near_depth_total", 0.0)) or 0.0
                            ),
                            "final_distance_to_mid": float(diag.get("final_distance_to_mid", 0.0) or 0.0),
                            "final_quote_delta_to_bbo": float(diag.get("final_quote_delta_to_bbo", 0.0) or 0.0),
                            "queue_init": float((order or {}).get("queue_init", 0.0) or 0.0),
                            "queue_local_rank": float((order or {}).get("queue_local_rank", 0.5) or 0.5),
                        }
                    )
                base_ms *= add_mult
        else:
            base_ms = float(self.state.fill_cooldown_reducing_ms)
            if base_ms > 0.0:
                if self._reducing_cooldown_inventory_lifecycle_gate_active(prev_q, t_ms):
                    base_ms *= self._reducing_cooldown_vol_mult()
                else:
                    base_ms = 0.0
        if base_ms <= 0.0:
            return 0.0
        baseline_duration_ms = base_ms * (
            self.services.inventory_cooldown_multiplier(consec_units, self.context.inventory_cooldown_mode)
            if self.context.inventory_cooldown_mode != "original" else max(1.0, consec_units)
        )
        return self._cooldown_duration_for_exposure_fill(
            side=side,
            prev_q=prev_q,
            consecutive_units=consec_units,
            fill_ts_ms=t_ms,
            baseline_duration_ms=baseline_duration_ms,
            previous_fill_ts_ms=previous_fill_ts_ms,
            previous_cooldown_ms=previous_cooldown_ms,
            order=order,
            queue_ahead_before_fill_btc=queue_ahead_before_fill_btc,
        )

    def _apply_quote_fill_cooldowns(self):
        """Advance original variance-time and recovery cooldown decisions."""
        # ── Step 27: fill cooldown — prevent same-side quoting ──
        # 中文说明：cooldown 长度在 fill 发生时决定；这里不要根据当前 q
        # 重新推断加仓/减仓，否则“减仓成交后变 flat”的边界会被误套完整冷却。
        self.state.bid_can_post_before_fill_cd = bool(self.state.hb_new)
        self.state.ask_can_post_before_fill_cd = bool(self.state.ha_new)
        if self.state.variance_time_enabled:
            if self.services.inventory_cooldown_blocks_action(
                    self._fill_cooldown_active("BUY", int(self.state.t)), self.context.inventory_cooldown_mode):
                self.state.hb_new = False
                self.state.bid_block_reasons.append("fill_cd")
            if self.services.inventory_cooldown_blocks_action(
                    self._fill_cooldown_active("SELL", int(self.state.t)), self.context.inventory_cooldown_mode):
                self.state.ha_new = False
                self.state.ask_block_reasons.append("fill_cd")
        else:
            if self.state.consec_buy > 1e-10 and self.state.last_buy_fill_cooldown_ms > 0.0:
                if self.state.t - self.state.last_buy_fill_ts >= self.state.last_buy_fill_cooldown_ms:
                    self._post_cooldown_budget_note_release_due(
                        "BUY",
                        ready_ts_ms=int(
                            round(
                                float(self.state.last_buy_fill_ts)
                                + float(self.state.last_buy_fill_cooldown_ms)
                            )
                        ),
                        observed_ts_ms=int(self.state.t),
                        last_fill_ts_ms=int(self.state.last_buy_fill_ts),
                        consecutive_units=float(self.state.consec_buy),
                    )
                    if self.state.fill_cooldown_reset_consec_on_expiry:
                        self.state.consec_buy = 0.0
                    self.state.last_buy_fill_cooldown_ms = 0.0
                else:
                    self.state.block_bid = True
                    if (
                        self.state.cooldown_duration_target_override_active
                        and self.state.cooldown_duration_fork_target_side == "BUY"
                        and float(self.state.q) < 0.0
                    ):
                        self.state.cooldown_duration_reducing_permission_control_checks += 1
                        self.state.block_bid = (
                            int(self.state.t)
                            < self.state.cooldown_duration_target_control_deadline_ts_ms
                        )
                    if self.state.block_bid and self.context.inventory_cooldown_mode != "admission_off":
                        self.state.hb_new = False
                        self.state.bid_block_reasons.append("fill_cd")
            if self.state.consec_sell > 1e-10 and self.state.last_sell_fill_cooldown_ms > 0.0:
                if self.state.t - self.state.last_sell_fill_ts >= self.state.last_sell_fill_cooldown_ms:
                    self._post_cooldown_budget_note_release_due(
                        "SELL",
                        ready_ts_ms=int(
                            round(
                                float(self.state.last_sell_fill_ts)
                                + float(self.state.last_sell_fill_cooldown_ms)
                            )
                        ),
                        observed_ts_ms=int(self.state.t),
                        last_fill_ts_ms=int(self.state.last_sell_fill_ts),
                        consecutive_units=float(self.state.consec_sell),
                    )
                    if self.state.fill_cooldown_reset_consec_on_expiry:
                        self.state.consec_sell = 0.0
                    self.state.last_sell_fill_cooldown_ms = 0.0
                else:
                    self.state.block_ask = True
                    if (
                        self.state.cooldown_duration_target_override_active
                        and self.state.cooldown_duration_fork_target_side == "SELL"
                        and float(self.state.q) > 0.0
                    ):
                        self.state.cooldown_duration_reducing_permission_control_checks += 1
                        self.state.block_ask = (
                            int(self.state.t)
                            < self.state.cooldown_duration_target_control_deadline_ts_ms
                        )
                    if self.state.block_ask and self.context.inventory_cooldown_mode != "admission_off":
                        self.state.ha_new = False
                        self.state.ask_block_reasons.append("fill_cd")

            if self.state.cooldown_duration_target_override_active:
                self.state.target_is_buy = self.state.cooldown_duration_fork_target_side == "BUY"
                self.state.target_is_exposure = (
                    float(self.state.q) >= 0.0 if self.state.target_is_buy else float(self.state.q) <= 0.0
                )
                if self.state.target_is_exposure:
                    self.state.candidate_active = (
                        self.state.last_buy_fill_cooldown_ms > 0.0
                        and int(self.state.t) - self.state.last_buy_fill_ts
                        < self.state.last_buy_fill_cooldown_ms
                        if self.state.target_is_buy
                        else self.state.last_sell_fill_cooldown_ms > 0.0
                        and int(self.state.t) - self.state.last_sell_fill_ts
                        < self.state.last_sell_fill_cooldown_ms
                    )
                    self.state.control_active = (
                        int(self.state.t) < self.state.cooldown_duration_target_control_deadline_ts_ms
                    )
                    if bool(self.state.candidate_active) != bool(self.state.control_active):
                        self.state.cooldown_duration_exposure_permission_change_count += 1

        if self.state.cooldown_rearm_runtime is not None:
            # Keep the original expiry/counter updates above. Override ONLY
            # the cooldown veto, including after natural expiry; never undo
            # another guard or the positive-fill risk cancellation.
            for loop_item in ('BUY', 'SELL'):
                self.context.side = loop_item
                self.context.blocked = self._cooldown_rearm_admission(
                    self.context.side, self._fill_cooldown_active(self.context.side, int(self.state.t)))
                if self.context.side == 'BUY':
                    self.state.hb_new = self.state.bid_can_post_before_fill_cd and not self.context.blocked
                    self.state.bid_block_reasons = [r for r in self.state.bid_block_reasons if r != 'fill_cd']
                    if self.context.blocked:
                        self.state.bid_block_reasons.append('fill_cd')
                else:
                    self.state.ha_new = self.state.ask_can_post_before_fill_cd and not self.context.blocked
                    self.state.ask_block_reasons = [r for r in self.state.ask_block_reasons if r != 'fill_cd']
                    if self.context.blocked:
                        self.state.ask_block_reasons.append('fill_cd')

        # Freeze the unmodified operational cooldown decision before any
        # research-only rearm/skip hook can alter the side permission.
        self.state.bid_can_post_after_fill_cd = bool(self.state.hb_new)
        self.state.ask_can_post_after_fill_cd = bool(self.state.ha_new)
