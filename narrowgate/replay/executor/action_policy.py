"""Replay action policy owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional

from strategy.replace_threshold import coalesce_pending_replace


class ActionPolicy:
    def _queue_value_set_first_event(self,
        record: dict,
        event: str,
        event_ts_ms: int,
    ) -> None:
        if str(record.get("first_event", "pending")) != "pending":
            return
        record["first_event"] = str(event)
        record["first_event_ts_ns"] = int(event_ts_ms) * 1_000_000
        record["event_time_ms"] = max(
            0,
            int(event_ts_ms) - int(record["decision_ts_ms"]),
        )

    def _queue_value_state_fields(self, state) -> dict[str, Any]:
        fields = {
            "queue_state_active": int(bool(state.active)),
            "queue_state_reason": str(state.reason),
            "maker_expected_ticks": float(state.maker_expected_ticks),
            "empirical_adverse_probability": float(
                state.adverse_probability
            ),
            "empirical_favorable_probability": float(
                state.favorable_probability
            ),
            "market_order_intensity": float(
                state.adverse_flow_intensity
            ),
            "cancel_intensity": float(state.cancel_intensity),
            "refill_intensity": float(state.refill_intensity),
            "adverse_to_refill_ratio": float(
                state.adverse_to_refill_ratio
            ),
            "queue_state_key": str(state.queue_state_key),
            "microprice_state_key": str(state.microprice_state_key),
        }
        for name in (
            "queue_recovery_probability",
            "keep_value_bps",
            "cancel_reenter_value_bps",
            "cancel_advantage_bps",
            "queue_reset_option_cost_bps",
        ):
            fields[name] = float(getattr(state, name, self.services.math.nan))
        cause_intensities = getattr(state, "cause_intensities_per_s", {})
        cause_probabilities = getattr(state, "cause_probabilities", {})
        for cause in (
            "favorable_fill",
            "adverse_fill",
            "cancel",
            "adverse_price_jump",
            "inventory_lifecycle_repair",
            "queue_recovery",
        ):
            fields[f"hazard_{cause}_per_s"] = float(
                cause_intensities.get(cause, self.services.math.nan)
            )
            fields[f"probability_{cause}"] = float(
                cause_probabilities.get(cause, self.services.math.nan)
            )
        return fields

    def _queue_value_estimated_option_cost(self,
        side: str,
        order: dict,
        state,
    ) -> float:
        frozen_cost_bps = float(
            getattr(state, "queue_reset_option_cost_bps", self.services.math.nan)
        )
        if self.services.math.isfinite(frozen_cost_bps):
            return (
                max(0.0, frozen_cost_bps)
                * max(0.0, float(self.state.mid))
                / 10_000.0
                * max(0.0, float(order.get("remaining", 0.0) or 0.0))
            )
        distance_to_mid = (
            max(0.0, float(self.state.mid) - float(order["price"]))
            if side == "BUY"
            else max(0.0, float(order["price"]) - float(self.state.mid))
        )
        expected_value_per_btc = (
            distance_to_mid + float(state.maker_expected_ticks) * float(self.state.TICK)
        )
        queue_init = max(0.0, float(order.get("queue_init", 0.0) or 0.0))
        queue_left = max(0.0, float(order.get("queue_left", 0.0) or 0.0))
        queue_progress = (
            min(1.0, max(0.0, 1.0 - queue_left / queue_init))
            if queue_init > 1e-12
            else 0.0
        )
        return max(0.0, expected_value_per_btc) * max(
            0.0, float(order.get("remaining", 0.0) or 0.0)
        ) * queue_progress

    def _queue_value_request_cancel(self,
        order: dict,
        now_ts: int,
        *,
        reason: str = "queue_value_cancel",
    ) -> None:
        if int(order.get("cancel_ts", -1) or -1) > 0:
            if not order.get("cancel_reason"):
                order["cancel_reason"] = reason
            return
        request_start_ts, effective_ts, ack_ts = self._sample_cancel_deadlines_ms(
            request_ts=int(now_ts),
            side=str(order.get("side", "BUY")),
            operation=self.services._LATENCY_QUEUE_VALUE_CANCEL,
            order_ts=int(order.get("quote_ts", order.get("submit_ts", 0)) or 0),
        )
        order["cancel_effective_ts"] = int(effective_ts)
        order["cancel_ts"] = int(ack_ts)
        order["cancel_request_ts"] = int(request_start_ts)
        order["cancel_reason"] = reason
        if order["state"] == self.state.ORDER_OPEN:
            order["state"] = self.state.ORDER_PENDING_CANCEL
        if self.state.async_rest_gateway:
            self.state.rest_gateway_last_timing["order"] = order
            order["cancel_rest_return_ts"] = self.state.async_rest_unset_ms
        else:
            self._record_order_lifecycle_cancel_request(
                order, int(request_start_ts), reason=str(reason),
            )
        self._ranked_guard_cancel_requested(
            order,
            visibility_ts_ms=int(now_ts if self.state.async_rest_gateway else request_start_ts),
            reason=str(reason),
        )

    def _maybe_apply_queue_value_keep_cancel(self,
        side: str,
        now_ts: int,
        *,
        can_post: bool,
        ref_order: Optional[dict],
        side_ctx: dict,
        block_reasons: list[str],
    ) -> tuple[bool, bool, bool]:
        """Apply one active-order K0/K1 intervention per inventory_lifecycle."""

        if (
            not self.state.queue_value_keep_cancel_enabled
            or side not in self.state.queue_value_sides
            or side not in self.state.queue_value_evaluators
            or self.state.trace_queue_value_keep_cancel is None
        ):
            return bool(can_post), False, False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        episode_id = self.state.queue_value_active_episode_by_side.get(side)
        record = (
            self.state.queue_value_open_records.get(int(episode_id))
            if episode_id is not None
            else None
        )
        active_order = (
            ref_order
            if ref_order is not None
            and ref_order.get("state") in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
            else None
        )
        feature_order = active_order
        if record is not None:
            feature_order = {
                "price": float(record["order_price"]),
                "remaining": float(record.get("order_remaining_at_decision", 0.0)),
                "quote_ts": int(
                    record.get("order_quote_ts_ms", record["decision_ts_ms"])
                ),
                "queue_init": float(
                    record.get(
                        "shadow_policy_queue_init",
                        record.get("queue_init", 0.0),
                    )
                ),
                "queue_left": float(
                    record.get(
                        "shadow_policy_queue_left",
                        record.get("queue_left", 0.0),
                    )
                ),
            }
        if feature_order is None:
            return bool(can_post), False, False
        features = self._queue_value_features(
            side,
            int(now_ts),
            feature_order,
            side_ctx,
        )
        excitation = self.state.queue_value_runtime[side].excitation_at(
            int(now_ts) * 1_000_000
        )
        state = self.state.queue_value_evaluators[side].evaluate(
            side=side,
            features=features,
            excitation=excitation,
            was_active=record is not None,
        )
        if record is not None:
            record.update(
                {
                    f"latest_{key}": value
                    for key, value in self._queue_value_state_fields(state).items()
                }
            )
            action = str(record["action"])
            if action == "cancel_then_baseline_reenter":
                if active_order is None:
                    side_ctx["queue_value_keep_cancel_action"] = action
                    side_ctx["queue_value_state_active"] = False
                    if not can_post:
                        record["reentry_waiting_baseline"] = 1
                        return False, False, False
                    record["reentry_authorized_ts_ns"] = (
                        int(now_ts) * 1_000_000
                    )
                    side_ctx[
                        "queue_value_keep_cancel_intervention_id"
                    ] = int(record["inventory_lifecycle_id"])
                    side_ctx["queue_value_reentry_order"] = True
                    return bool(can_post), False, False
                self._queue_value_request_cancel(
                    active_order,
                    int(now_ts),
                    reason="queue_value_cancel_reenter",
                )
                block_reasons.append("queue_value_cancel_ack_wait")
                side_ctx["queue_value_keep_cancel_action"] = action
                side_ctx["queue_value_state_active"] = True
                return False, False, False
            if not state.active:
                if int(record.get("state_exit_ts_ns", 0) or 0) <= 0:
                    record["state_exit_ts_ns"] = int(now_ts) * 1_000_000
                    record["state_exit_reason"] = str(state.reason)
                if action == "cancel_until_state_exit":
                    side_ctx["queue_value_keep_cancel_action"] = action
                    side_ctx["queue_value_state_active"] = False
                    if active_order is not None:
                        self._queue_value_request_cancel(
                            active_order,
                            int(now_ts),
                        )
                        block_reasons.append(
                            "queue_value_state_exit_cancel_ack_wait"
                        )
                        return False, False, False
                    if not can_post:
                        record["reentry_waiting_baseline"] = 1
                        return False, False, False
                    record["reentry_authorized_ts_ns"] = (
                        int(now_ts) * 1_000_000
                    )
                    side_ctx[
                        "queue_value_keep_cancel_intervention_id"
                    ] = int(record["inventory_lifecycle_id"])
                    side_ctx["queue_value_reentry_order"] = True
                    return bool(can_post), False, False
                self.state.queue_value_active_episode_by_side.pop(side, None)
                return bool(can_post), False, False
            if action == "cancel_until_state_exit":
                if active_order is not None:
                    self._queue_value_request_cancel(active_order, int(now_ts))
                block_reasons.append("queue_value_cancel_wait")
                side_ctx["queue_value_keep_cancel_action"] = action
                side_ctx["queue_value_state_active"] = True
                return False, False, False
            force_keep = bool(
                active_order is not None
                and int(active_order.get("trace_id", -1))
                == int(record["order_id"])
                and can_post
            )
            side_ctx["queue_value_keep_cancel_action"] = action
            side_ctx["queue_value_state_active"] = True
            return bool(can_post), force_keep, False

        exposure_increasing_add = bool(
            self.state.inventory_lifecycle_active
            and abs(self.state.q) > 1e-10
            and (
                (side == "BUY" and self.state.q > 0.0)
                or (side == "SELL" and self.state.q < 0.0)
            )
        )
        if (
            not exposure_increasing_add
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.queue_value_intervened_inventory_lifecycles
            or not can_post
            or active_order is None
            or active_order.get("state") != self.state.ORDER_OPEN
            or not state.active
            or (
                len(self.state.trace_queue_value_keep_cancel)
                + len(self.state.queue_value_open_records)
                >= self.state.trace_queue_value_keep_cancel_max
            )
        ):
            return bool(can_post), False, False
        action, assignment_u = self.state.queue_value_action_chooser(
            self.state.queue_value_rng,
            self.state.queue_value_probabilities,
        )
        behavior_propensity = float(self.state.queue_value_probabilities[action])
        intervention_id = inventory_lifecycle_id
        self.state.queue_value_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.queue_value_active_episode_by_side[side] = intervention_id
        self.state.queue_value_assignment_counts[action] += 1
        active_order["queue_value_keep_cancel_intervention_id"] = intervention_id
        active_order["queue_value_keep_cancel_action"] = action
        queue_cost = (
            self._queue_value_estimated_option_cost(side, active_order, state)
            if action
            in {"cancel_until_state_exit", "cancel_then_baseline_reenter"}
            else 0.0
        )
        distance_to_mid = (
            max(0.0, float(self.state.mid) - float(active_order["price"]))
            if side == "BUY"
            else max(0.0, float(active_order["price"]) - float(self.state.mid))
        )
        native_seed_supported = bool(
            self.state.exchange_book_scheduler is not None
            and str(active_order.get("simulator_queue_source", ""))
            == "native_exchange_book"
            and str(
                active_order.get(
                    "_exchange_book_queue_seed_status",
                    "",
                )
            )
            in {"exact", "known_zero"}
            and bool(
                active_order.get(
                    "exchange_book_queue_path_valid",
                    False,
                )
            )
            and not bool(
                active_order.get(
                    "exchange_book_queue_ambiguous",
                    False,
                )
            )
        )
        native_inventory_lifecycle_predecision_supported = bool(
            native_seed_supported
            and inventory_lifecycle_id
            not in self.state.exchange_book_predecision_invalid_inventory_lifecycles
        )
        record = {
            "schema_version": (
                "queue_value_cancel_reenter_replay.v3"
                if self.state.queue_value_action_family == "queue_value_cancel_reenter"
                else "queue_value_net_keep_cancel_replay.v2"
                if self.state.queue_value_action_family == "queue_value_net_keep_cancel"
                else "queue_value_keep_cancel_replay.v1"
            ),
            "action_family": self.state.queue_value_action_family,
            "decision_id": (
                f"queue_value:{self.services.SYMBOL}:{int(now_ts)}:{inventory_lifecycle_id}:{side}"
            ),
            "day": self.services.pd.to_datetime(
                int(now_ts), unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "decision_ts_ms": int(now_ts),
            "feature_ready_ts_ns": int(now_ts) * 1_000_000,
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "order_id": int(active_order.get("trace_id", -1)),
            "client_order_id": "",
            "side": side,
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": behavior_propensity,
            **{
                f"behavior_prob_{name}": float(
                    self.state.queue_value_probabilities[name]
                )
                for name in self.state.queue_value_actions
            },
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.queue_value_seed),
            "external_reference_used": 0,
            "input_scope": str(self.state.queue_value_input_scope),
            "queue_runtime_event_source": str(
                self.state.queue_value_runtime_event_source.get(side, "")
            ),
            "one_intervention_per_inventory_lifecycle": 1,
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_age_s": max(
                0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1_000.0
            ),
            "inventory_lifecycle_pnl_so_far": float(
                self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl
            ),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_add_count_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "order_price": float(active_order["price"]),
            "order_remaining_at_decision": float(active_order["remaining"]),
            "order_age_ms": max(
                0, int(now_ts) - int(active_order["quote_ts"])
            ),
            "order_quote_ts_ms": int(active_order["quote_ts"]),
            "quote_distance_ticks": float(
                distance_to_mid / max(float(self.state.TICK), 1e-12)
            ),
            "queue_init": float(
                active_order.get(
                    "policy_queue_init",
                    active_order.get("queue_init", 0.0),
                )
                or 0.0
            ),
            "queue_left": float(
                active_order.get(
                    "policy_queue_left",
                    active_order.get("queue_left", 0.0),
                )
                or 0.0
            ),
            "queue_fraction_left": float(features["queue_fraction_left"]),
            "shadow_policy_queue_init": float(
                active_order.get(
                    "policy_queue_init",
                    active_order.get("queue_init", 0.0),
                )
                or 0.0
            ),
            "shadow_policy_queue_left": float(
                active_order.get(
                    "policy_queue_left",
                    active_order.get("queue_left", 0.0),
                )
                or 0.0
            ),
            "state_exit_queue_source": (
                "action_invariant_delayed_trade_depletion"
            ),
            "simulator_queue_init": float(
                active_order.get("queue_init", 0.0) or 0.0
            ),
            "simulator_queue_left": float(
                active_order.get("queue_left", 0.0) or 0.0
            ),
            "queue_local_rank": float(
                active_order.get("queue_local_rank", 0.5) or 0.5
            ),
            "queue_source": str(
                active_order.get(
                    "policy_queue_source",
                    "delayed_policy_topn_or_fitted",
                )
            ),
            "simulator_queue_source": str(
                active_order.get(
                    "simulator_queue_source",
                    "topn_or_fitted_fallback",
                )
            ),
            "exchange_book_queue_mode": str(self.state.exchange_book_queue_mode),
            "exchange_book_queue_status": str(
                active_order.get(
                    "_exchange_book_queue_seed_status",
                    "disabled",
                )
            ),
            "exchange_book_queue_reason": str(
                active_order.get(
                    "_exchange_book_queue_seed_reason",
                    "",
                )
            ),
            "exchange_book_queue_segment_id": int(
                active_order.get(
                    "exchange_book_queue_segment_id",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_asof_ts_ns": int(
                active_order.get(
                    "exchange_book_queue_asof_ts_ns",
                    0,
                )
                or 0
            ),
            "exchange_book_queue_path_valid": int(
                bool(
                    active_order.get(
                        "exchange_book_queue_path_valid",
                        False,
                    )
                )
            ),
            "exchange_book_queue_ambiguous": int(
                bool(
                    active_order.get(
                        "exchange_book_queue_ambiguous",
                        False,
                    )
                )
            ),
            "exchange_book_queue_invalidated": 0,
            "exchange_book_queue_invalidated_reason": "",
            "native_exchange_seed_supported_at_decision": int(
                native_seed_supported
            ),
            "native_inventory_lifecycle_predecision_supported": int(
                native_inventory_lifecycle_predecision_supported
            ),
            "_exchange_book_trade_since_update_qty": 0.0,
            "spread_ticks": float(features["spread_ticks"]),
            "book_imbalance": float(features["book_imbalance"]),
            "best_bid": float(features["best_bid"]),
            "best_ask": float(features["best_ask"]),
            "bid_qty": float(features["bid_qty"]),
            "ask_qty": float(features["ask_qty"]),
            "weighted_mid_proxy_shift_bps": float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_quote_flip_rate": float(
                side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
            ),
            "toxicity": float(
                self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask
            ),
            "markout_ema": float(
                self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask
            ),
            **self._queue_value_state_fields(state),
            "first_event": "pending",
            "first_event_ts_ns": 0,
            "fill_ts_ns": 0,
            "fill_event_seq": 0,
            "cancel_request_ts_ns": 0,
            "cancel_ack_ts_ns": 0,
            "cancel_event_seq": 0,
            "adverse_price_jump_ts_ns": 0,
            "adverse_price_jump_event_seq": 0,
            "repair_ts_ns": 0,
            "inventory_lifecycle_repair_event_seq": 0,
            "state_exit_ts_ns": 0,
            "state_exit_reason": "",
            "reentry_authorized_ts_ns": 0,
            "reentry_submit_ts_ns": 0,
            "reentry_order_id": -1,
            "reentry_order_submit_count": 0,
            "reentry_waiting_baseline": 0,
            "reentry_fill_qty": 0.0,
            "reentry_fill_count": 0,
            "reentry_fill_value": 0.0,
            "reentry_cancel_ack_ts_ns": 0,
            "reentry_cancel_count": 0,
            "fill_value_markout_bps": self.services.math.nan,
            "fill_value_horizon_censored": 0,
            "fill_value_horizon_ms": int(self.state.queue_value_fill_horizon_ms),
            "price_jump_ticks": float(self.state.queue_value_price_jump_ticks),
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "queue_cost": float(queue_cost),
            "queue_cost_definition": (
                "estimated_value_of_existing_queue_priority_at_decision"
            ),
            "reward_definition": "decision_to_flat_or_day_end_mtm",
            "inventory_lifecycle_cost_definition": (
                "accounting_residual_not_separately_identified"
            ),
            "order_active_before": 1,
        }
        self.state.queue_value_open_records[intervention_id] = record
        side_ctx["queue_value_keep_cancel_intervention_id"] = intervention_id
        side_ctx["queue_value_keep_cancel_action"] = action
        side_ctx["queue_value_state_active"] = True
        if action in {
            "cancel_until_state_exit",
            "cancel_then_baseline_reenter",
        }:
            record["cancel_request_ts_ns"] = int(now_ts) * 1_000_000
            self._queue_value_request_cancel(
                active_order,
                int(now_ts),
                reason=(
                    "queue_value_cancel_reenter"
                    if action == "cancel_then_baseline_reenter"
                    else "queue_value_cancel"
                ),
            )
            block_reasons.append(
                "queue_value_cancel_ack_wait"
                if action == "cancel_then_baseline_reenter"
                else "queue_value_cancel_wait"
            )
            return False, False, True
        return bool(can_post), True, True

    def _pending_lifecycle_order(self, orders):
        pending = [
            o for o in orders
            if o["state"] in (self.state.ORDER_PENDING_NEW, self.state.ORDER_PENDING_CANCEL)
            and o["remaining"] >= self.state.LOT_SIZE
        ]
        if not pending:
            return None
        return max(
            pending,
            key=lambda o: (
                o["submit_ts"],
                o["activate_ts"],
                self.services._order_price_tick(o, self.state.TICK),
            ),
        )

    def _maybe_apply_state_conditioned_policy(self,
        side: str,
        now_ts: int,
        *,
        baseline_price: float,
        other_side_price: float,
        final_size: float,
        can_post: bool,
        side_orders: list[dict],
        side_ctx: dict,
        max_pair_spread: float,
    ) -> tuple[float, bool]:
        """Evaluate one frozen state-conditioned action per inventory_lifecycle."""

        if self.state.state_conditioned_policy is None or self.state.trace_state_conditioned_policy is None:
            return float(baseline_price), False
        if (
            len(self.state.trace_state_conditioned_policy)
            + len(self.state.state_conditioned_open_records)
            >= self.state.trace_state_conditioned_policy_max
        ):
            return float(baseline_price), False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        inventory_role = self.services.inventory_role_for_quote(side, float(self.state.q), float(self.state.order_size))
        if (
            not self.state.inventory_lifecycle_active
            or inventory_role != "add"
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.state_conditioned_intervened_inventory_lifecycles
            or not can_post
            or final_size < self.state.LOT_SIZE
            or side_orders
        ):
            return float(baseline_price), False

        features = {
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_age_s": max(
                0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1000.0
            ),
            "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_pnl_so_far": float(
                self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl
            ),
            "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "toxicity": float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask),
            "markout_ema": float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask),
            "weighted_mid_proxy_shift_bps": float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_quote_flip_rate": float(
                side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "l2_near_depth_total": float(
                side_ctx.get(
                    "l2_near_depth_total",
                    side_ctx.get("near_depth_total", 0.0),
                )
                or 0.0
            ),
        }
        decision_ts_ns = int(now_ts) * 1_000_000
        policy_decision = self.state.state_conditioned_policy.decide(
            side=side,
            inventory_role=inventory_role,
            features=features,
            decision_ts_ns=decision_ts_ns,
            feature_ready_ts_ns=decision_ts_ns,
        )
        executed_action = str(policy_decision.action)
        quote = self.services.apply_local_add_action(
            side=side,
            action=executed_action,
            baseline_price=float(baseline_price),
            pre_guard_price=float(
                side_ctx.get("pre_guard_price", baseline_price) or baseline_price
            ),
            other_side_price=float(other_side_price),
            mid=float(self.state.mid),
            best_bid=float(self.state.cur_best_bid),
            best_ask=float(self.state.cur_best_ask),
            weighted_mid_proxy_shift_bps=float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            tick=float(self.state.TICK),
            max_pair_spread=float(max_pair_spread),
        )
        self.state.state_conditioned_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.state_conditioned_candidate_counts[policy_decision.candidate_action] += 1
        self.state.state_conditioned_executed_counts[executed_action] += 1
        record = {
            "schema_version": "state_conditioned_quote_policy_replay.v1",
            "decision_id": (
                f"state_policy:{self.services.SYMBOL}:{int(now_ts)}:{inventory_lifecycle_id}:{side}"
            ),
            "day": self.services.pd.to_datetime(int(now_ts), unit="ms", utc=True).strftime(
                "%Y-%m-%d"
            ),
            "decision_ts_ns": decision_ts_ns,
            "decision_ts_ms": int(now_ts),
            "feature_ready_ts_ns": decision_ts_ns,
            "feature_age_ms": float(policy_decision.feature_age_ms),
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side,
            "inventory_role": inventory_role,
            "policy_id": str(policy_decision.policy_id),
            "policy_mode": str(policy_decision.mode),
            "policy_reason": str(policy_decision.reason),
            "policy_eligible": int(policy_decision.eligible),
            "candidate_action": str(policy_decision.candidate_action),
            "executed_action": executed_action,
            "baseline_value": float(policy_decision.baseline_value),
            "candidate_value": float(policy_decision.candidate_value),
            "estimated_advantage": float(policy_decision.advantage),
            "policy_scores_json": self.services.json.dumps(
                policy_decision.scores, sort_keys=True, separators=(",", ":")
            ),
            "external_reference_used": int(
                self.state.state_conditioned_policy.artifact.input_scope
                == "local_plus_external"
            ),
            "one_intervention_per_inventory_lifecycle": 1,
            **features,
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "base_price": float(baseline_price),
            "pre_guard_price": float(
                side_ctx.get("pre_guard_price", baseline_price) or baseline_price
            ),
            "selected_price": float(quote.selected_price),
            "action_delta_ticks": float(quote.delta_ticks),
            "action_effective": int(quote.effective),
            "action_clamp_reason": str(quote.clamp_reason),
            "base_size": float(final_size),
            "final_size": float(final_size),
            "order_active_before": 0,
            "pending_order_before": 0,
            "queue_reset_cost_identified": 1,
            "queue_cost": 0.0,
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "fill_markout_30s_qty_weighted_bps_sum": 0.0,
            "fill_markout_horizon_censored": 0,
            "reward_definition": "decision_to_inventory_lifecycle_terminal_mtm",
            "inventory_lifecycle_cost_definition": (
                "fill_value_minus_queue_cost_minus_reward_residual"
            ),
        }
        self.state.state_conditioned_open_records[inventory_lifecycle_id] = record
        side_ctx["state_conditioned_policy_intervention_id"] = inventory_lifecycle_id
        side_ctx["state_conditioned_policy_candidate_action"] = str(
            policy_decision.candidate_action
        )
        side_ctx["state_conditioned_policy_executed_action"] = executed_action
        side_ctx["state_conditioned_policy_advantage"] = float(
            policy_decision.advantage
        )
        return float(quote.selected_price), bool(quote.effective)

    def _maybe_apply_local_action_ope(self,
        side: str,
        now_ts: int,
        *,
        baseline_price: float,
        other_side_price: float,
        final_size: float,
        can_post: bool,
        side_orders: list[dict],
        side_ctx: dict,
        max_pair_spread: float,
    ) -> tuple[float, bool]:
        """Randomize one local add-side action per inventory inventory_lifecycle."""

        if self.state.trace_local_action_ope is None or not self.state.local_action_ope_enabled:
            return float(baseline_price), False
        if side not in self.state.local_action_sides:
            return float(baseline_price), False
        if len(self.state.trace_local_action_ope) + len(self.state.local_action_open_records) >= self.state.trace_local_action_ope_max:
            return float(baseline_price), False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        exposure_increasing_add = bool(
            self.state.inventory_lifecycle_active
            and abs(self.state.q) > 1e-10
            and ((side == "BUY" and self.state.q > 0.0) or (side == "SELL" and self.state.q < 0.0))
        )
        if (
            not exposure_increasing_add
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.local_action_intervened_inventory_lifecycles
            or not can_post
            or final_size < self.state.LOT_SIZE
            or side_orders
        ):
            return float(baseline_price), False

        action, assignment_u = self.services.choose_action(self.state.local_action_rng, self.state.local_action_probabilities)
        quote = self.services.apply_local_add_action(
            side=side,
            action=action,
            baseline_price=float(baseline_price),
            pre_guard_price=float(side_ctx.get("pre_guard_price", baseline_price) or baseline_price),
            other_side_price=float(other_side_price),
            mid=float(self.state.mid),
            best_bid=float(self.state.cur_best_bid),
            best_ask=float(self.state.cur_best_ask),
            weighted_mid_proxy_shift_bps=float(side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0),
            tick=float(self.state.TICK),
            max_pair_spread=float(max_pair_spread),
        )
        intervention_id = inventory_lifecycle_id
        behavior_propensity = float(self.state.local_action_probabilities[action])
        if behavior_propensity <= 0.0:
            raise RuntimeError("sampled local action has zero behavior propensity")
        self.state.local_action_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.local_action_assignment_counts[action] += 1
        side_last_fill_ts = self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        causal_path = self.services.compute_causal_path_features(
            side=side,
            start_ts_ms=int(side_last_fill_ts),
            decision_ts_ms=int(now_ts),
            trade_ts_ms=self.state.trade_ts,
            trade_qty=self.state.trade_qty,
            is_buyer_maker=self.state.is_seller,
            l2_ts_ms=(
                self.state.l2_ts
                if self.state.historical_l2 and self.state.l2_ts is not None
                else self.services.np.empty(0)
            ),
            l2_bid_px=(
                self.state.l2_bid_px
                if self.state.historical_l2 and self.state.l2_bid_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_bid_qty=(
                self.state.l2_bid_qty
                if self.state.historical_l2 and self.state.l2_bid_qty is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_px=(
                self.state.l2_ask_px
                if self.state.historical_l2 and self.state.l2_ask_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_qty=(
                self.state.l2_ask_qty
                if self.state.historical_l2 and self.state.l2_ask_qty is not None
                else self.services.np.empty((0, 0))
            ),
            near_levels=max(1, int(self.state.l2_near_levels or 1)),
        )
        distance_to_mid = (
            max(0.0, float(self.state.mid) - float(baseline_price))
            if side == "BUY"
            else max(0.0, float(baseline_price) - float(self.state.mid))
        )
        quote_delta_to_bbo = (
            max(0.0, float(self.state.cur_best_bid) - float(baseline_price))
            if side == "BUY"
            else max(0.0, float(baseline_price) - float(self.state.cur_best_ask))
        )
        visible_queue = self._l2_visible_queue_ahead(
            side, float(baseline_price), target_ts=int(now_ts)
        )
        estimated_queue = self._estimate_queue_ahead(
            side,
            float(baseline_price),
            float(distance_to_mid),
            target_ts=int(now_ts),
        )
        record = {
            "schema_version": "local_action_ope_replay.v1",
            "decision_id": f"local_action:{self.services.SYMBOL}:{int(now_ts)}:{inventory_lifecycle_id}:{side}",
            "day": self.services.pd.to_datetime(int(now_ts), unit="ms", utc=True).strftime("%Y-%m-%d"),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "decision_ts_ms": int(now_ts),
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side,
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": behavior_propensity,
            **{
                f"behavior_prob_{name}": float(self.state.local_action_probabilities[name])
                for name in self.services.LOCAL_ACTIONS
            },
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.local_action_seed),
            "external_reference_used": 0,
            "one_intervention_per_inventory_lifecycle": 1,
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_active": 1,
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_age_s": max(0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1000.0),
            "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_pnl_so_far": float(self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "inventory_lifecycle_add_count_so_far": int(self.state.inventory_lifecycle_exposure_increasing_fills),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "toxicity": float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask),
            "markout_ema": float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask),
            "weighted_mid_proxy_shift_bps": float(side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0),
            "l2_quote_flip_rate": float(side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0),
            "l2_book_refresh_ratio": float(side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0),
            "l2_book_cancel_ratio": float(side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0),
            "l2_near_depth_total": float(
                side_ctx.get("l2_near_depth_total", side_ctx.get("near_depth_total", 0.0))
                or 0.0
            ),
            **causal_path,
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "quote_distance": float(distance_to_mid),
            "quote_distance_bps": float(
                10_000.0 * distance_to_mid / max(float(self.state.mid), 1e-12)
            ),
            "quote_distance_ticks": float(
                distance_to_mid / max(float(self.state.TICK), 1e-12)
            ),
            "quote_delta_to_bbo": float(quote_delta_to_bbo),
            "quote_delta_to_bbo_ticks": float(
                quote_delta_to_bbo / max(float(self.state.TICK), 1e-12)
            ),
            "exact_l2_spread_bps": float(
                10_000.0
                * max(0.0, float(self.state.cur_best_ask) - float(self.state.cur_best_bid))
                / max(float(self.state.mid), 1e-12)
            ),
            "baseline_visible_queue_ahead": float(
                visible_queue if visible_queue is not None else -1.0
            ),
            "baseline_estimated_queue_ahead": float(estimated_queue),
            "base_price": float(baseline_price),
            "pre_guard_price": float(
                side_ctx.get("pre_guard_price", baseline_price) or baseline_price
            ),
            "selected_price": float(quote.selected_price),
            "action_delta_ticks": float(quote.delta_ticks),
            "action_effective": int(quote.effective),
            "action_clamp_reason": str(quote.clamp_reason),
            "base_size": float(final_size),
            "final_size": float(final_size),
            "order_active_before": 0,
            "pending_order_before": 0,
            "queue_reset_cost_identified": 1,
            "queue_cost": 0.0,
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "fill_markout_30s_qty_weighted_bps_sum": 0.0,
            "fill_markout_horizon_censored": 0,
            "reward_definition": "decision_to_inventory_lifecycle_terminal_mtm",
            "inventory_lifecycle_cost_definition": "fill_value_minus_queue_cost_minus_reward_residual",
        }
        self.state.local_action_open_records[intervention_id] = record
        side_ctx["local_action_ope_intervention_id"] = intervention_id
        side_ctx["local_action_ope_action"] = action
        side_ctx["local_action_ope_behavior_propensity"] = behavior_propensity
        side_ctx["local_action_ope_action_effective"] = bool(quote.effective)
        side_ctx["local_action_ope_baseline_price"] = float(baseline_price)
        side_ctx["local_action_ope_selected_price"] = float(quote.selected_price)
        return float(quote.selected_price), True

    def _maybe_apply_sell_add_skip_ope(self,
        side: str,
        now_ts: int,
        *,
        baseline_price: float,
        final_size: float,
        can_post: bool,
        side_orders: list[dict],
        side_ctx: dict,
        block_reasons: list[str],
        repair_probability: float,
        repair_probability_change: float,
    ) -> tuple[bool, bool]:
        """Randomize one baseline/skip add decision per inventory_lifecycle and side."""

        side = str(side).upper()

        if (
            self.state.trace_sell_add_skip_ope is None
            or not self.state.sell_add_skip_ope_enabled
            or side not in self.state.sell_add_skip_sides
        ):
            return bool(can_post), False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        remaining_day_s = max(
            0.0,
            float(86_400_000 - (int(now_ts) % 86_400_000)) / 1_000.0,
        )
        exposure_increasing_add = bool(
            self.state.inventory_lifecycle_active
            and (
                (side == "BUY" and self.state.q > 1e-10)
                or (side == "SELL" and self.state.q < -1e-10)
            )
        )
        existing = self.state.sell_add_skip_open_records.get(inventory_lifecycle_id)
        if existing is not None and exposure_increasing_add:
            side_ctx["sell_add_skip_ope_intervention_id"] = inventory_lifecycle_id
            side_ctx["sell_add_skip_ope_action"] = str(existing["action"])
            if str(existing["action"]) == "stop_add_until_flat":
                existing["blocked_quote_cycles"] = int(
                    existing.get("blocked_quote_cycles", 0) + 1
                )
                block_reasons.append(f"{side.lower()}_stop_add_until_flat")
                return False, False
            return bool(can_post), False
        if (
            len(self.state.trace_sell_add_skip_ope)
            + len(self.state.sell_add_skip_open_records)
            >= self.state.trace_sell_add_skip_ope_max
        ):
            return bool(can_post), False
        if (
            not exposure_increasing_add
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.sell_add_skip_intervened_inventory_lifecycles
            or not can_post
            or final_size < self.state.LOT_SIZE
            or side_orders
            or remaining_day_s < self.state.sell_add_skip_min_followup_s
        ):
            return bool(can_post), False

        action, assignment_u = (
            self.services.choose_inventory_lifecycle_stop_add_action(
                self.state.sell_add_skip_rng,
                self.state.sell_add_skip_probabilities,
            )
            if self.state.sell_add_skip_mode == "until_flat"
            else self.services.choose_sell_add_skip_action(
                self.state.sell_add_skip_rng,
                self.state.sell_add_skip_probabilities,
            )
        )
        behavior_propensity = float(self.state.sell_add_skip_probabilities[action])
        if behavior_propensity <= 0.0:
            raise RuntimeError(
                "sampled add-skip action has zero behavior propensity"
            )
        self.state.sell_add_skip_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.sell_add_skip_assignment_counts[action] += 1
        causal_path = self.services.compute_causal_path_features(
            side=side,
            start_ts_ms=int(
                self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
            ),
            decision_ts_ms=int(now_ts),
            trade_ts_ms=self.state.trade_ts,
            trade_qty=self.state.trade_qty,
            is_buyer_maker=self.state.is_seller,
            l2_ts_ms=(
                self.state.l2_ts
                if self.state.historical_l2 and self.state.l2_ts is not None
                else self.services.np.empty(0)
            ),
            l2_bid_px=(
                self.state.l2_bid_px
                if self.state.historical_l2 and self.state.l2_bid_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_bid_qty=(
                self.state.l2_bid_qty
                if self.state.historical_l2 and self.state.l2_bid_qty is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_px=(
                self.state.l2_ask_px
                if self.state.historical_l2 and self.state.l2_ask_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_qty=(
                self.state.l2_ask_qty
                if self.state.historical_l2 and self.state.l2_ask_qty is not None
                else self.services.np.empty((0, 0))
            ),
            near_levels=max(1, int(self.state.l2_near_levels or 1)),
        )
        distance_to_mid = max(
            0.0,
            float(self.state.mid) - float(baseline_price)
            if side == "BUY"
            else float(baseline_price) - float(self.state.mid),
        )
        quote_delta_to_bbo = max(
            0.0,
            float(self.state.cur_best_bid) - float(baseline_price)
            if side == "BUY"
            else float(baseline_price) - float(self.state.cur_best_ask),
        )
        visible_queue = self._l2_visible_queue_ahead(
            side,
            float(baseline_price),
            target_ts=int(now_ts),
        )
        estimated_queue = self._estimate_queue_ahead(
            side,
            float(baseline_price),
            float(distance_to_mid),
            target_ts=int(now_ts),
        )
        trend_through_price = float(baseline_price) + (
            -float(self.state.TICK) if side == "BUY" else float(self.state.TICK)
        )
        trend_through_distance_bps = (
            10_000.0
            * max(
                0.0,
                float(self.state.mid) - trend_through_price
                if side == "BUY"
                else trend_through_price - float(self.state.mid),
            )
            / max(float(self.state.mid), 1e-12)
        )
        side_toxicity = self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask
        side_markout = self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask
        skip = action in {"skip_one_add_cycle", "stop_add_until_flat"}
        record = {
            "schema_version": (
                "inventory_lifecycle_add_permission_ope_replay.v1"
                if self.state.sell_add_skip_mode == "until_flat"
                else "side_add_skip_ope_replay.v2"
            ),
            "family_id": self.state.sell_add_skip_family_id,
            "decision_id": (
                f"add_skip:{self.state.sell_add_skip_family_id}:{self.services.SYMBOL}:"
                f"{int(now_ts)}:{inventory_lifecycle_id}:{side}"
            ),
            "day": self.services.pd.to_datetime(
                int(now_ts), unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "decision_ts_ms": int(now_ts),
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side,
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": behavior_propensity,
            **{
                f"behavior_prob_{name}": float(
                    self.state.sell_add_skip_probabilities[name]
                )
                for name in self.state.sell_add_skip_actions
            },
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.sell_add_skip_seed),
            "external_reference_used": 0,
            "one_intervention_per_inventory_lifecycle": 1,
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_active": 1,
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_age_s": max(
                0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1_000.0
            ),
            "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_pnl_so_far": float(
                self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl
            ),
            "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "inventory_lifecycle_add_count_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "decision_mid": float(self.state.mid),
            "repair_probability": float(
                repair_probability
                if self.services.math.isfinite(repair_probability)
                else 0.0
            ),
            "repair_probability_valid": int(
                self.services.math.isfinite(repair_probability)
            ),
            "repair_probability_change": float(
                repair_probability_change
                if self.services.math.isfinite(repair_probability_change)
                else 0.0
            ),
            "toxicity": float(side_toxicity),
            "markout_ema": float(side_markout),
            "weighted_mid_proxy_shift_bps": float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_quote_flip_rate": float(
                side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "l2_near_depth_total": float(
                side_ctx.get(
                    "l2_near_depth_total",
                    side_ctx.get("near_depth_total", 0.0),
                )
                or 0.0
            ),
            **causal_path,
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "quote_distance": float(distance_to_mid),
            "quote_distance_bps": float(
                10_000.0
                * distance_to_mid
                / max(float(self.state.mid), 1e-12)
            ),
            "quote_distance_ticks": float(
                distance_to_mid / max(float(self.state.TICK), 1e-12)
            ),
            "quote_delta_to_bbo": float(quote_delta_to_bbo),
            "quote_delta_to_bbo_ticks": float(
                quote_delta_to_bbo / max(float(self.state.TICK), 1e-12)
            ),
            "exact_l2_spread_bps": float(
                10_000.0
                * max(0.0, float(self.state.cur_best_ask) - float(self.state.cur_best_bid))
                / max(float(self.state.mid), 1e-12)
            ),
            "baseline_visible_queue_ahead": float(
                visible_queue if visible_queue is not None else -1.0
            ),
            "baseline_estimated_queue_ahead": float(estimated_queue),
            "base_price": float(baseline_price),
            "selected_price": float(baseline_price),
            "action_delta_ticks": 0.0,
            "action_effective": int(skip),
            "action_clamp_reason": action if skip else "none",
            "allow_post": int(not skip),
            "base_size": float(final_size),
            "final_size": float(final_size),
            "order_active_before": 0,
            "pending_order_before": 0,
            "queue_reset_cost_identified": 1,
            "queue_cost": 0.0,
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "intervention_order_submit_count": 0,
            "blocked_quote_cycles": int(skip),
            "fill_markout_30s_qty_weighted_bps_sum": 0.0,
            "fill_markout_horizon_censored": 0,
            "trend_through_price": trend_through_price,
            "trend_through_distance_bps": trend_through_distance_bps,
            "max_adverse_move_bps": 0.0,
            "competing_event": "pending",
            "competing_event_code": -1,
            "competing_event_ts_ms": 0,
            "competing_event_time_s": 0.0,
            "repair_first": 0,
            "trend_through_first": 0,
            "min_followup_s": float(self.state.sell_add_skip_min_followup_s),
            "remaining_day_s": float(remaining_day_s),
            "reward_definition": "decision_to_inventory_lifecycle_terminal_mtm",
            "inventory_lifecycle_cost_definition": (
                "fill_value_minus_queue_cost_minus_reward_residual"
            ),
        }
        self.state.sell_add_skip_open_records[inventory_lifecycle_id] = record
        side_ctx["sell_add_skip_ope_intervention_id"] = inventory_lifecycle_id
        side_ctx["sell_add_skip_ope_action"] = action
        if skip:
            block_reasons.append(f"{side.lower()}_{action}")
            return False, True
        return bool(can_post), True

    def _state_conditioned_rearm_path(self,
        side: str,
        now_ts: int,
    ) -> tuple[int, dict[str, float]]:
        side_last_fill_ts = (
            self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        )
        return int(side_last_fill_ts), self.services.compute_causal_path_features(
            side=side,
            start_ts_ms=int(side_last_fill_ts),
            decision_ts_ms=int(now_ts),
            trade_ts_ms=self.state.trade_ts,
            trade_qty=self.state.trade_qty,
            is_buyer_maker=self.state.is_seller,
            l2_ts_ms=(
                self.state.l2_ts
                if self.state.historical_l2 and self.state.l2_ts is not None
                else self.services.np.empty(0)
            ),
            l2_bid_px=(
                self.state.l2_bid_px
                if self.state.historical_l2 and self.state.l2_bid_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_bid_qty=(
                self.state.l2_bid_qty
                if self.state.historical_l2 and self.state.l2_bid_qty is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_px=(
                self.state.l2_ask_px
                if self.state.historical_l2 and self.state.l2_ask_px is not None
                else self.services.np.empty((0, 0))
            ),
            l2_ask_qty=(
                self.state.l2_ask_qty
                if self.state.historical_l2 and self.state.l2_ask_qty is not None
                else self.services.np.empty((0, 0))
            ),
            near_levels=max(1, int(self.state.l2_near_levels or 1)),
        )

    def _evaluate_rearm_path(self, causal_path: dict[str, float]) -> dict[str, object]:
        if self.state.state_conditioned_rearm_policy_version == "composite_recovery_v2":
            recovery = self.services.evaluate_recovery_event(
                causal_path,
                self.state.recovery_event_rearm_spec,
            )
            return {
                "entry_active": bool(recovery.hold_add_active),
                "exit_active": bool(recovery.recovery_event),
                "data_valid": bool(recovery.data_valid),
                "exit_reason": (
                    "composite_recovery_event"
                    if recovery.recovery_event
                    else ""
                ),
                "adverse_move_active": False,
                "persistent_flow_active": False,
                "weak_refill_active": bool(recovery.refill_score < 1.0),
                "weak_recovery_active": bool(
                    recovery.microprice_recovery_score < 1.0
                    or recovery.queue_recovery_score < 1.0
                ),
                "recovery_shock_decay_score": float(
                    recovery.shock_decay_score
                ),
                "recovery_refill_score": float(recovery.refill_score),
                "recovery_microprice_score": float(
                    recovery.microprice_recovery_score
                ),
                "recovery_queue_score": float(
                    recovery.queue_recovery_score
                ),
                "recovery_composite_score": float(recovery.recovery_score),
                "recovery_score_threshold": float(
                    self.state.recovery_event_rearm_spec.score_threshold
                ),
            }
        adverse = self.services.evaluate_state_conditioned_rearm(
            causal_path,
            self.state.state_conditioned_rearm_spec,
        )
        return {
            "entry_active": bool(adverse.entry_active),
            "exit_active": bool(adverse.exit_active),
            "data_valid": bool(adverse.data_valid),
            "exit_reason": str(adverse.exit_reason),
            "adverse_move_active": bool(adverse.adverse_move_active),
            "persistent_flow_active": bool(adverse.persistent_flow_active),
            "weak_refill_active": bool(adverse.weak_refill_active),
            "weak_recovery_active": bool(adverse.weak_recovery_active),
            "recovery_shock_decay_score": 0.0,
            "recovery_refill_score": 0.0,
            "recovery_microprice_score": 0.0,
            "recovery_queue_score": 0.0,
            "recovery_composite_score": 0.0,
            "recovery_score_threshold": 0.0,
        }

    def _attach_state_conditioned_rearm_context(self,
        side_ctx: dict,
        record: dict,
    ) -> None:
        side_ctx["state_conditioned_rearm_intervention_id"] = int(
            record["inventory_lifecycle_id"]
        )
        side_ctx["state_conditioned_rearm_action"] = str(record["action"])
        side_ctx["state_conditioned_rearm_behavior_propensity"] = float(
            record["behavior_propensity"]
        )
        side_ctx["state_conditioned_rearm_action_effective"] = bool(
            record.get("action_effective", 0)
        )
        side_ctx["state_conditioned_rearm_state_episode_active"] = bool(
            record.get("state_episode_active", 0)
        )

    def _maybe_apply_state_conditioned_rearm(self,
        side: str,
        now_ts: int,
        *,
        baseline_price: float,
        final_size: float,
        can_post: bool,
        side_orders: list[dict],
        side_ctx: dict,
        block_reasons: list[str],
    ) -> tuple[bool, bool]:
        """Apply one post-cooldown randomized rearm episode per inventory_lifecycle."""

        if (
            self.state.trace_state_conditioned_rearm is None
            or not self.state.state_conditioned_rearm_enabled
            or side not in self.state.state_conditioned_rearm_sides
        ):
            return bool(can_post), False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        exposure_increasing_add = bool(
            self.state.inventory_lifecycle_active
            and abs(self.state.q) > 1e-10
            and ((side == "BUY" and self.state.q > 0.0) or (side == "SELL" and self.state.q < 0.0))
        )
        existing = self.state.state_conditioned_rearm_open_records.get(inventory_lifecycle_id)
        if existing is not None and str(existing.get("side", "")) == side:
            self._attach_state_conditioned_rearm_context(side_ctx, existing)
            if not exposure_increasing_add:
                return bool(can_post), False
            if (
                str(existing.get("action", ""))
                == "continue_block_until_recovery"
                and bool(existing.get("state_episode_active", 0))
            ):
                _, causal_path = self._state_conditioned_rearm_path(side, now_ts)
                decision = self._evaluate_rearm_path(causal_path)
                existing["state_eval_count"] = int(
                    existing.get("state_eval_count", 0) + 1
                )
                existing["last_state_eval_ts_ms"] = int(now_ts)
                existing["last_state_data_valid"] = int(
                    bool(decision["data_valid"])
                )
                existing["last_state_exit_reason"] = str(
                    decision["exit_reason"]
                )
                existing["last_recovery_current_adverse_bps"] = float(
                    causal_path.get("recovery_current_adverse_bps", 0.0)
                )
                existing["last_adverse_flow_5s"] = float(
                    causal_path.get("shock_adverse_flow_imbalance_5s", 0.0)
                )
                existing["last_refill_recovery_ratio"] = float(
                    causal_path.get("refill_recovery_ratio", 0.0)
                )
                existing["last_recovery_price_ratio"] = float(
                    causal_path.get("recovery_price_ratio", 0.0)
                )
                existing["last_recovery_composite_score"] = float(
                    decision["recovery_composite_score"]
                )
                if bool(decision["exit_active"]):
                    existing["state_episode_active"] = 0
                    existing["state_exit_ts_ms"] = int(now_ts)
                    existing["state_exit_reason"] = str(
                        decision["exit_reason"]
                    )
                    existing["state_block_duration_s"] = max(
                        0.0,
                        float(now_ts - int(existing["decision_ts_ms"])) / 1_000.0,
                    )
                    self.state.state_conditioned_rearm_active_episode_by_side.pop(
                        side, None
                    )
                    self._attach_state_conditioned_rearm_context(side_ctx, existing)
                    return bool(can_post), True
                existing["blocked_quote_cycles"] = int(
                    existing.get("blocked_quote_cycles", 0) + 1
                )
                existing["last_block_ts_ms"] = int(now_ts)
                block_reasons.append("state_conditioned_rearm")
                self._attach_state_conditioned_rearm_context(side_ctx, existing)
                return False, True
            return bool(can_post), False

        if (
            len(self.state.trace_state_conditioned_rearm)
            + len(self.state.state_conditioned_rearm_open_records)
            >= self.state.trace_state_conditioned_rearm_max
        ):
            return bool(can_post), False
        side_last_fill_ts = (
            self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        )
        side_cooldown_total_ms = (
            self.state.last_buy_fill_cooldown_total_ms
            if side == "BUY"
            else self.state.last_sell_fill_cooldown_total_ms
        )
        elapsed_ms = max(0, int(now_ts - side_last_fill_ts))
        remaining_day_s = max(
            0.0,
            float(86_400_000 - (int(now_ts) % 86_400_000)) / 1_000.0,
        )
        if (
            not exposure_increasing_add
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.state_conditioned_rearm_intervened_inventory_lifecycles
            or not can_post
            or final_size < self.state.LOT_SIZE
            or side_orders
            or block_reasons
            or side_last_fill_ts <= 0
            or side_cooldown_total_ms <= 0.0
            or elapsed_ms < int(side_cooldown_total_ms)
            or elapsed_ms < self.state.state_conditioned_rearm_min_elapsed_ms
            or remaining_day_s < self.state.state_conditioned_rearm_min_followup_s
        ):
            return bool(can_post), False

        side_last_fill_ts, causal_path = self._state_conditioned_rearm_path(
            side, now_ts
        )
        decision = self._evaluate_rearm_path(causal_path)
        action, assignment_u = self.services.choose_state_conditioned_rearm_action(
            self.state.state_conditioned_rearm_rng,
            self.state.state_conditioned_rearm_probabilities,
        )
        behavior_propensity = float(
            self.state.state_conditioned_rearm_probabilities[action]
        )
        action_effective = bool(
            action == "continue_block_until_recovery"
            and bool(decision["entry_active"])
        )
        self.state.state_conditioned_rearm_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.state_conditioned_rearm_assignment_counts[action] += 1
        if action_effective:
            self.state.state_conditioned_rearm_active_episode_by_side[side] = inventory_lifecycle_id

        distance_to_mid = (
            max(0.0, float(self.state.mid) - float(baseline_price))
            if side == "BUY"
            else max(0.0, float(baseline_price) - float(self.state.mid))
        )
        quote_delta_to_bbo = (
            max(0.0, float(self.state.cur_best_bid) - float(baseline_price))
            if side == "BUY"
            else max(0.0, float(baseline_price) - float(self.state.cur_best_ask))
        )
        visible_queue = self._l2_visible_queue_ahead(
            side,
            float(baseline_price),
            target_ts=int(now_ts),
        )
        estimated_queue = self._estimate_queue_ahead(
            side,
            float(baseline_price),
            float(distance_to_mid),
            target_ts=int(now_ts),
        )
        record = {
            "schema_version": "state_conditioned_rearm_replay.v1",
            "decision_id": (
                f"state_conditioned_rearm:{self.services.SYMBOL}:{int(now_ts)}:"
                f"{inventory_lifecycle_id}:{side}"
            ),
            "day": self.services.pd.to_datetime(
                int(now_ts), unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "decision_ts_ms": int(now_ts),
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side,
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": behavior_propensity,
            **{
                f"behavior_prob_{name}": float(
                    self.state.state_conditioned_rearm_probabilities[name]
                )
                for name in self.services.STATE_CONDITIONED_REARM_ACTIONS
            },
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.state_conditioned_rearm_seed),
            "external_reference_used": 0,
            "one_intervention_per_inventory_lifecycle": 1,
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_active": 1,
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_age_s": max(
                0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1_000.0
            ),
            "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_pnl_so_far": float(
                self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl
            ),
            "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "inventory_lifecycle_add_count_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "decision_mid": float(self.state.mid),
            "toxicity": float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask),
            "markout_ema": float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask),
            "weighted_mid_proxy_shift_bps": float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_quote_flip_rate": float(
                side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "l2_near_depth_total": float(
                side_ctx.get(
                    "l2_near_depth_total",
                    side_ctx.get("near_depth_total", 0.0),
                )
                or 0.0
            ),
            **causal_path,
            "state_conditioned_rearm_policy_version": str(
                self.state.state_conditioned_rearm_policy_version
            ),
            "entry_state_active": int(bool(decision["entry_active"])),
            "entry_state_data_valid": int(bool(decision["data_valid"])),
            "entry_adverse_move_active": int(
                bool(decision["adverse_move_active"])
            ),
            "entry_persistent_flow_active": int(
                bool(decision["persistent_flow_active"])
            ),
            "entry_weak_refill_active": int(
                bool(decision["weak_refill_active"])
            ),
            "entry_weak_recovery_active": int(
                bool(decision["weak_recovery_active"])
            ),
            "recovery_shock_decay_score": float(
                decision["recovery_shock_decay_score"]
            ),
            "recovery_refill_score": float(
                decision["recovery_refill_score"]
            ),
            "recovery_microprice_score": float(
                decision["recovery_microprice_score"]
            ),
            "recovery_queue_score": float(
                decision["recovery_queue_score"]
            ),
            "recovery_composite_score": float(
                decision["recovery_composite_score"]
            ),
            "recovery_score_threshold": float(
                decision["recovery_score_threshold"]
            ),
            "baseline_cooldown_total_ms": float(side_cooldown_total_ms),
            "baseline_rearm_elapsed_ms": int(elapsed_ms),
            "min_rearm_elapsed_ms": int(
                self.state.state_conditioned_rearm_min_elapsed_ms
            ),
            "remaining_day_s": float(remaining_day_s),
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "quote_distance": float(distance_to_mid),
            "quote_distance_bps": float(
                10_000.0 * distance_to_mid / max(float(self.state.mid), 1e-12)
            ),
            "quote_distance_ticks": float(
                distance_to_mid / max(float(self.state.TICK), 1e-12)
            ),
            "quote_delta_to_bbo": float(quote_delta_to_bbo),
            "quote_delta_to_bbo_ticks": float(
                quote_delta_to_bbo / max(float(self.state.TICK), 1e-12)
            ),
            "exact_l2_spread_bps": float(
                10_000.0
                * max(0.0, float(self.state.cur_best_ask) - float(self.state.cur_best_bid))
                / max(float(self.state.mid), 1e-12)
            ),
            "baseline_visible_queue_ahead": float(
                visible_queue if visible_queue is not None else -1.0
            ),
            "baseline_estimated_queue_ahead": float(estimated_queue),
            "base_price": float(baseline_price),
            "selected_price": float(baseline_price),
            "action_delta_ticks": 0.0,
            "action_effective": int(action_effective),
            "action_clamp_reason": (
                "adverse_state_block" if action_effective else "none"
            ),
            "allow_post": int(not action_effective),
            "base_size": float(final_size),
            "final_size": float(final_size),
            "order_active_before": 0,
            "pending_order_before": 0,
            "queue_reset_cost_identified": 1,
            "queue_cost": 0.0,
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "intervention_order_submit_count": 0,
            "fill_markout_30s_qty_weighted_bps_sum": 0.0,
            "fill_markout_horizon_censored": 0,
            "state_episode_active": int(action_effective),
            "state_entry_ts_ms": int(now_ts) if action_effective else 0,
            "state_exit_ts_ms": 0,
            "state_exit_reason": "",
            "state_block_duration_s": 0.0,
            "blocked_quote_cycles": int(action_effective),
            "state_eval_count": 1,
            "last_state_eval_ts_ms": int(now_ts),
            "last_block_ts_ms": int(now_ts) if action_effective else 0,
            "reward_definition": "decision_to_inventory_lifecycle_terminal_mtm",
            "inventory_lifecycle_cost_definition": (
                "fill_value_minus_queue_cost_minus_reward_residual"
            ),
        }
        self.state.state_conditioned_rearm_open_records[inventory_lifecycle_id] = record
        self._attach_state_conditioned_rearm_context(side_ctx, record)
        if action_effective:
            block_reasons.append("state_conditioned_rearm")
            return False, True
        return bool(can_post), True

    def _maybe_apply_safe_add_rearm_randomized(self,
        side: str,
        now_ts: int,
        *,
        baseline_price: float,
        other_side_price: float,
        final_size: float,
        can_post_before_fill_cd: bool,
        can_post_after_fill_cd: bool,
        block_reasons: list[str],
        side_orders: list[dict],
        side_ctx: dict,
        max_pair_spread: float,
    ) -> tuple[float, bool, bool]:
        """Assign and execute one early add-side decision per inventory_lifecycle."""

        if (
            self.state.trace_safe_add_rearm_intervention is None
            or not self.state.safe_add_rearm_randomized_enabled
        ):
            return float(baseline_price), bool(can_post_after_fill_cd), False
        if (
            len(self.state.trace_safe_add_rearm_intervention)
            + len(self.state.safe_add_rearm_open_records)
            >= self.state.trace_safe_add_rearm_intervention_max
        ):
            return float(baseline_price), bool(can_post_after_fill_cd), False
        inventory_lifecycle_id = int(self.state.inventory_lifecycle_count)
        exposure_increasing_add = bool(
            self.state.inventory_lifecycle_active
            and abs(self.state.q) > 1e-10
            and ((side == "BUY" and self.state.q > 0.0) or (side == "SELL" and self.state.q < 0.0))
        )
        side_last_fill_ts = self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        side_cooldown_total_ms = (
            self.state.last_buy_fill_cooldown_total_ms
            if side == "BUY"
            else self.state.last_sell_fill_cooldown_total_ms
        )
        side_cooldown_active_ms = (
            self.state.last_buy_fill_cooldown_ms
            if side == "BUY"
            else self.state.last_sell_fill_cooldown_ms
        )
        elapsed_ms = max(0, int(now_ts - side_last_fill_ts))
        remaining_ms = max(0.0, float(side_cooldown_total_ms) - elapsed_ms)
        unique_block_reasons = set(block_reasons)
        if (
            not exposure_increasing_add
            or inventory_lifecycle_id <= 0
            or inventory_lifecycle_id in self.state.safe_add_rearm_intervened_inventory_lifecycles
            or final_size < self.state.LOT_SIZE
            or side_orders
            or not can_post_before_fill_cd
            or can_post_after_fill_cd
            or unique_block_reasons != {"fill_cd"}
            or side_last_fill_ts <= 0
            or side_cooldown_active_ms <= 0.0
            or remaining_ms <= 0.0
            or elapsed_ms < self.state.safe_add_rearm_randomized_elapsed_ms
        ):
            return float(baseline_price), bool(can_post_after_fill_cd), False

        action, assignment_u = self.services.choose_safe_add_rearm_action(
            self.state.safe_add_rearm_randomized_rng,
            self.state.safe_add_rearm_randomized_probabilities,
        )
        quote = self.services.apply_safe_add_rearm_action(
            side=side,
            action=action,
            baseline_price=float(baseline_price),
            other_side_price=float(other_side_price),
            tick=float(self.state.TICK),
            max_pair_spread=float(max_pair_spread),
        )
        behavior_propensity = float(
            self.state.safe_add_rearm_randomized_probabilities[action]
        )
        intervention_id = inventory_lifecycle_id
        self.state.safe_add_rearm_intervened_inventory_lifecycles.add(inventory_lifecycle_id)
        self.state.safe_add_rearm_assignment_counts[action] += 1
        causal_path = self.services.compute_causal_path_features(
            side=side,
            start_ts_ms=int(side_last_fill_ts),
            decision_ts_ms=int(now_ts),
            trade_ts_ms=self.state.trade_ts,
            trade_qty=self.state.trade_qty,
            is_buyer_maker=self.state.is_seller,
            l2_ts_ms=(self.state.l2_ts if self.state.historical_l2 and self.state.l2_ts is not None else self.services.np.empty(0)),
            l2_bid_px=(self.state.l2_bid_px if self.state.historical_l2 and self.state.l2_bid_px is not None else self.services.np.empty((0, 0))),
            l2_bid_qty=(self.state.l2_bid_qty if self.state.historical_l2 and self.state.l2_bid_qty is not None else self.services.np.empty((0, 0))),
            l2_ask_px=(self.state.l2_ask_px if self.state.historical_l2 and self.state.l2_ask_px is not None else self.services.np.empty((0, 0))),
            l2_ask_qty=(self.state.l2_ask_qty if self.state.historical_l2 and self.state.l2_ask_qty is not None else self.services.np.empty((0, 0))),
            near_levels=max(1, int(self.state.l2_near_levels or 1)),
        )
        record = {
            "schema_version": "safe_add_rearm_randomized_replay.v1",
            "decision_id": (
                f"safe_add_rearm:{self.services.SYMBOL}:{int(now_ts)}:{inventory_lifecycle_id}:{side}"
            ),
            "day": self.services.pd.to_datetime(int(now_ts), unit="ms", utc=True).strftime(
                "%Y-%m-%d"
            ),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "decision_ts_ms": int(now_ts),
            "inventory_lifecycle_id": inventory_lifecycle_id,
            "side": side,
            "inventory_role": "add",
            "action": action,
            "behavior_propensity": behavior_propensity,
            **{
                f"behavior_prob_{name}": float(
                    self.state.safe_add_rearm_randomized_probabilities[name]
                )
                for name in self.services.SAFE_ADD_REARM_ACTIONS
            },
            "assignment_u": float(assignment_u),
            "random_seed": int(self.state.safe_add_rearm_randomized_seed),
            "one_intervention_per_inventory_lifecycle": 1,
            "external_reference_used": 0,
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_active": 1,
            "inventory_lifecycle_start_ts_ms": int(self.state.inventory_lifecycle_start_ts),
            "inventory_lifecycle_start_pnl": float(self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_age_s": max(
                0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1000.0
            ),
            "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
            "inventory_lifecycle_pnl_so_far": float(self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl),
            "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "decision_mtm": float(self._inventory_lifecycle_pnl(self.state.mid)),
            "toxicity": float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask),
            "markout_ema": float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask),
            "weighted_mid_proxy_shift_bps": float(
                side_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
            ),
            "l2_quote_flip_rate": float(
                side_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
            ),
            "l2_book_refresh_ratio": float(
                side_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
            ),
            "l2_book_cancel_ratio": float(
                side_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
            ),
            "l2_near_depth_total": float(
                side_ctx.get(
                    "l2_near_depth_total",
                    side_ctx.get("near_depth_total", 0.0),
                )
                or 0.0
            ),
            **causal_path,
            "fill_cooldown_elapsed_ms": int(elapsed_ms),
            "fill_cooldown_total_ms": float(side_cooldown_total_ms),
            "fill_cooldown_active_ms": float(side_cooldown_active_ms),
            "fill_cooldown_remaining_ms": float(remaining_ms),
            "fill_cooldown_consecutive_units": float(
                self.state.consec_buy if side == "BUY" else self.state.consec_sell
            ),
            "target_elapsed_ms": int(self.state.safe_add_rearm_randomized_elapsed_ms),
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "base_price": float(baseline_price),
            "selected_price": float(quote.selected_price),
            "action_delta_ticks": float(quote.delta_ticks),
            "action_allow_post": int(quote.allow_post),
            "action_effective": int(quote.effective),
            "action_clamp_reason": str(quote.clamp_reason),
            "base_size": float(final_size),
            "final_size": float(final_size),
            "order_active_before": 0,
            "pending_order_before": 0,
            "baseline_block_reason": "fill_cd",
            "queue_reset_cost_identified": 1,
            "queue_cost": 0.0,
            "fill_value": 0.0,
            "intervention_fill_qty": 0.0,
            "intervention_fill_count": 0,
            "intervention_order_submit_count": 0,
            "fill_markout_30s_qty_weighted_bps_sum": 0.0,
            "fill_markout_horizon_censored": 0,
            "reward_definition": "decision_to_inventory_lifecycle_terminal_mtm",
            "inventory_lifecycle_cost_definition": (
                "fill_value_minus_queue_cost_minus_reward_residual"
            ),
        }
        self.state.safe_add_rearm_open_records[intervention_id] = record
        side_ctx["safe_add_rearm_intervention_id"] = intervention_id
        side_ctx["safe_add_rearm_action"] = action
        side_ctx["safe_add_rearm_behavior_propensity"] = behavior_propensity
        side_ctx["safe_add_rearm_action_effective"] = bool(quote.effective)
        side_ctx["safe_add_rearm_baseline_price"] = float(baseline_price)
        side_ctx["safe_add_rearm_selected_price"] = float(quote.selected_price)
        side_ctx["safe_add_rearm_baseline_block_reason"] = "fill_cd"
        return float(quote.selected_price), bool(quote.allow_post), True

    def _apply_quote_recovery_actions(self):
        """Apply declared fork, rearm and add-skip actions in original order."""
        if self.state.ema_add_wait_fork_enabled:
            self.state.decision_visible_trade_index = self._decision_visible_trade_index(
                int(self.state.signal_visible_ts),
                int(self.state.i),
            )
            self.state.current_generation = {
                "bbo_index": int(self.state.decision_bbo_idx),
                "l2_index": int(self.state.decision_l2_idx),
                "trade_index": int(self.state.decision_visible_trade_index),
                "feature_ready_index": int(self.state.var_idx),
                "prediction_index": int(self.state.ml_idx),
                "snapshot_mid_tick_x2": int(
                    round(2.0 * float(self.state.mid) / float(self.state.TICK))
                ),
            }
            self.state.current_market_ready = bool(
                self.state.current_generation["bbo_index"] >= 0
                and self.state.current_generation["l2_index"] >= 0
                and self.state.current_generation["trade_index"] >= 0
                and self.state.current_generation["feature_ready_index"] >= 0
                and self.state.current_generation["prediction_index"] >= 0
                and self.state.current_generation["snapshot_mid_tick_x2"] > 0
            )
            self.state.target_generation_matches = bool(
                int(self.state.t) == self.state.ema_add_wait_fork_target_ts_ms
                and int(self.state.i) == self.state.ema_add_wait_fork_target_market_event_index
                and self.state.current_generation
                == {
                    str(key): int(value)
                    for key, value in self.state.ema_add_wait_fork_target_generation.items()
                }
            )
            if self.state.target_generation_matches:
                if self.state.ema_add_wait_fork_assigned:
                    self.state.ema_add_wait_fork_second_assignment_count += 1
                    raise RuntimeError(
                        "EMA ADD/WAIT fork attempted a second assignment"
                    )
                if not self.state.scheduled_requote_due or not self.state.current_market_ready:
                    raise RuntimeError(
                        "EMA ADD/WAIT target is not a ready external decision"
                    )
                if (
                    not self.state.inventory_lifecycle_active
                    or int(self.state.inventory_lifecycle_count)
                    != self.state.ema_add_wait_fork_target_inventory_lifecycle_id
                    or abs(float(self.state.q)) <= 1e-10
                ):
                    raise RuntimeError(
                        "EMA ADD/WAIT target inventory_lifecycle identity drifted"
                    )
                self.state.target_is_add = bool(
                    (
                        self.state.ema_add_wait_fork_target_side == "BUY"
                        and float(self.state.q) > 0.0
                    )
                    or (
                        self.state.ema_add_wait_fork_target_side == "SELL"
                        and float(self.state.q) < 0.0
                    )
                )
                if not self.state.target_is_add:
                    raise RuntimeError(
                        "EMA ADD/WAIT target side is not exposure-increasing add"
                    )
                self.state.target_orders = (
                    self.state.bid_orders
                    if self.state.ema_add_wait_fork_target_side == "BUY"
                    else self.state.ask_orders
                )
                if self._best_live_order(
                    self.state.target_orders, self.state.ema_add_wait_fork_target_side
                ) is not None or self._pending_lifecycle_order(self.state.target_orders) is not None:
                    raise RuntimeError(
                        "EMA ADD/WAIT target inherited a same-side order"
                    )
                self.state.target_can_post_before = bool(
                    self.state.bid_can_post_before_fill_cd
                    if self.state.ema_add_wait_fork_target_side == "BUY"
                    else self.state.ask_can_post_before_fill_cd
                )
                self.state.target_can_post_after = bool(
                    self.state.bid_can_post_after_fill_cd
                    if self.state.ema_add_wait_fork_target_side == "BUY"
                    else self.state.ask_can_post_after_fill_cd
                )
                if not self.state.target_can_post_before:
                    raise RuntimeError(
                        "EMA ADD/WAIT target has a non-cooldown blocker"
                    )
                self.state.observed_baseline_action = (
                    "ADD_NOW" if self.state.target_can_post_after else "WAIT_ONE_EPOCH"
                )
                if (
                    self.state.observed_baseline_action
                    != self.state.ema_add_wait_fork_target_baseline_action
                ):
                    raise RuntimeError(
                        "EMA ADD/WAIT target baseline action drifted"
                    )

                self.state.ema_add_wait_fork_assigned = True
                self.state.ema_add_wait_fork_assignment_ts_ms = int(self.state.t)
                self.state.ema_add_wait_fork_assignment_equity_usdc = float(self.state.cash) + float(
                    self.state.q
                ) * float(self.state.mid)
                self.state.ema_add_wait_fork_assignment_inventory_btc = float(self.state.q)
                self.state.ema_add_wait_fork_wait_active = bool(
                    self.state.ema_add_wait_fork_action == "WAIT_ONE_EPOCH"
                )
                self.state.ema_add_wait_fork_reason = "assigned"
                self.state.target_context = self.state.quote_context[
                    self.state.ema_add_wait_fork_target_side
                ]
                self.state.target_context["ema_add_wait_fork_id"] = (
                    f"{self.state.ema_add_wait_fork_target_inventory_lifecycle_id}:"
                    f"{self.state.ema_add_wait_fork_target_ts_ms}:"
                    f"{self.state.ema_add_wait_fork_action}"
                )
                self.state.target_context["ema_add_wait_fork_target_order"] = bool(
                    self.state.ema_add_wait_fork_action == "ADD_NOW"
                )
                if self.state.ema_add_wait_fork_target_side == "BUY":
                    if self.state.ema_add_wait_fork_action == "ADD_NOW":
                        self.state.hb_new = True
                        self.state.bid_block_reasons = [
                            reason
                            for reason in self.state.bid_block_reasons
                            if reason != "fill_cd"
                        ]
                    else:
                        self.state.hb_new = False
                        self.state.bid_block_reasons.append("ema_add_wait_fork")
                else:
                    if self.state.ema_add_wait_fork_action == "ADD_NOW":
                        self.state.ha_new = True
                        self.state.ask_block_reasons = [
                            reason
                            for reason in self.state.ask_block_reasons
                            if reason != "fill_cd"
                        ]
                    else:
                        self.state.ha_new = False
                        self.state.ask_block_reasons.append("ema_add_wait_fork")
            elif self.state.ema_add_wait_fork_assigned:
                if (
                    self.state.ema_add_wait_fork_external_epoch_observed
                    and self.state.ema_add_wait_fork_release_clock
                    == "frozen_scheduled_requote_v1"
                    and int(self.state.i) == self.state.ema_add_wait_fork_frozen_release_event_index
                ):
                    self.state.expected_release_generation = {
                        str(key): int(value)
                        for key, value in self.state.ema_add_wait_fork_frozen_release_generation.items()
                    }
                    if not self.state.current_market_ready or self.state.current_generation != self.state.expected_release_generation:
                        raise RuntimeError(
                            "EMA ADD/WAIT frozen external release generation drifted"
                        )
                if (
                    self.state.ema_add_wait_fork_wait_active
                    and self.state.ema_add_wait_fork_target_side == "BUY"
                    and float(self.state.q) > 0.0
                ):
                    self.state.hb_new = False
                    self.state.bid_block_reasons.append("ema_add_wait_fork")
                elif (
                    self.state.ema_add_wait_fork_wait_active
                    and self.state.ema_add_wait_fork_target_side == "SELL"
                    and float(self.state.q) < 0.0
                ):
                    self.state.ha_new = False
                    self.state.ask_block_reasons.append("ema_add_wait_fork")

            if self.state.ema_add_wait_fork_assigned:
                self.state.fork_id = (
                    f"{self.state.ema_add_wait_fork_target_inventory_lifecycle_id}:"
                    f"{self.state.ema_add_wait_fork_target_ts_ms}:"
                    f"{self.state.ema_add_wait_fork_action}"
                )
                self.state.quote_context["BUY"].setdefault(
                    "ema_add_wait_fork_id", self.state.fork_id
                )
                self.state.quote_context["SELL"].setdefault(
                    "ema_add_wait_fork_id", self.state.fork_id
                )

            if self.state.ema_add_wait_fork_quarantine:
                if float(self.state.q) >= -float(self.state.LOT_SIZE) * 0.5:
                    self.state.hb_new = False
                    self.state.bid_block_reasons.append("ema_add_wait_washout_quarantine")
                if float(self.state.q) <= float(self.state.LOT_SIZE) * 0.5:
                    self.state.ha_new = False
                    self.state.ask_block_reasons.append("ema_add_wait_washout_quarantine")

        if self.state.cooldown_duration_fork_quarantine:
            if float(self.state.q) >= -float(self.state.LOT_SIZE) * 0.5:
                self.state.hb_new = False
                self.state.bid_block_reasons.append(
                    "cooldown_duration_washout_quarantine"
                )
            if float(self.state.q) <= float(self.state.LOT_SIZE) * 0.5:
                self.state.ha_new = False
                self.state.ask_block_reasons.append(
                    "cooldown_duration_washout_quarantine"
                )

        self.state.nbid, self.state.hb_new, self.state.bid_safe_rearm_assigned = (
            self._maybe_apply_safe_add_rearm_randomized(
                "BUY",
                int(self.state.t),
                baseline_price=float(self.state.nbid),
                other_side_price=float(self.state.nask),
                final_size=float(self.state.bid_sz),
                can_post_before_fill_cd=self.state.bid_can_post_before_fill_cd,
                can_post_after_fill_cd=bool(self.state.hb_new),
                block_reasons=self.state.bid_block_reasons,
                side_orders=self.state.bid_orders,
                side_ctx=self.state.quote_context["BUY"],
                max_pair_spread=float(self.state.max_spread_post_policy),
            )
        )
        self.state.nask, self.state.ha_new, self.state.ask_safe_rearm_assigned = (
            self._maybe_apply_safe_add_rearm_randomized(
                "SELL",
                int(self.state.t),
                baseline_price=float(self.state.nask),
                other_side_price=float(self.state.nbid),
                final_size=float(self.state.ask_sz),
                can_post_before_fill_cd=self.state.ask_can_post_before_fill_cd,
                can_post_after_fill_cd=bool(self.state.ha_new),
                block_reasons=self.state.ask_block_reasons,
                side_orders=self.state.ask_orders,
                side_ctx=self.state.quote_context["SELL"],
                max_pair_spread=float(self.state.max_spread_post_policy),
            )
        )
        if self.state.bid_safe_rearm_assigned and self.state.hb_new:
            self.state.bid_block_reasons = [
                reason for reason in self.state.bid_block_reasons if reason != "fill_cd"
            ]
        if self.state.ask_safe_rearm_assigned and self.state.ha_new:
            self.state.ask_block_reasons = [
                reason for reason in self.state.ask_block_reasons if reason != "fill_cd"
            ]
        if self.state.bid_safe_rearm_assigned or self.state.ask_safe_rearm_assigned:
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        self.state.hb_new, self.state.bid_state_rearm_assigned = (
            self._maybe_apply_state_conditioned_rearm(
                "BUY",
                int(self.state.t),
                baseline_price=float(self.state.nbid),
                final_size=float(self.state.bid_sz),
                can_post=bool(self.state.hb_new),
                side_orders=self.state.bid_orders,
                side_ctx=self.state.quote_context["BUY"],
                block_reasons=self.state.bid_block_reasons,
            )
        )
        self.state.ha_new, self.state.ask_state_rearm_assigned = (
            self._maybe_apply_state_conditioned_rearm(
                "SELL",
                int(self.state.t),
                baseline_price=float(self.state.nask),
                final_size=float(self.state.ask_sz),
                can_post=bool(self.state.ha_new),
                side_orders=self.state.ask_orders,
                side_ctx=self.state.quote_context["SELL"],
                block_reasons=self.state.ask_block_reasons,
            )
        )
        if self.state.bid_state_rearm_assigned or self.state.ask_state_rearm_assigned:
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        self.state.hb_new, self.state.bid_add_skip_assigned = self._maybe_apply_sell_add_skip_ope(
            "BUY",
            int(self.state.t),
            baseline_price=float(self.state.nbid),
            final_size=float(self.state.bid_sz),
            can_post=bool(self.state.hb_new),
            side_orders=self.state.bid_orders,
            side_ctx=self.state.quote_context["BUY"],
            block_reasons=self.state.bid_block_reasons,
            repair_probability=float(self.state.repair_probability),
            repair_probability_change=float(self.state.repair_probability_change),
        )
        self.state.ha_new, self.state.ask_add_skip_assigned = self._maybe_apply_sell_add_skip_ope(
            "SELL",
            int(self.state.t),
            baseline_price=float(self.state.nask),
            final_size=float(self.state.ask_sz),
            can_post=bool(self.state.ha_new),
            side_orders=self.state.ask_orders,
            side_ctx=self.state.quote_context["SELL"],
            block_reasons=self.state.ask_block_reasons,
            repair_probability=float(self.state.repair_probability),
            repair_probability_change=float(self.state.repair_probability_change),
        )

    def _apply_order_action_policies(self):
        """Resolve retained orders and declared action-policy adjustments."""
        # ── Step 26B: lazy requote ──
        self.state.bid_updated = True
        self.state.ask_updated = True
        self.state.bid_ref_order = self._best_live_order(self.state.bid_orders, "BUY")
        self.state.ask_ref_order = self._best_live_order(self.state.ask_orders, "SELL")
        self.state.bid_pending_order = self._pending_lifecycle_order(self.state.bid_orders)
        self.state.ask_pending_order = self._pending_lifecycle_order(self.state.ask_orders)
        self.state.bid_active_before = self.state.bid_ref_order is not None
        self.state.ask_active_before = self.state.ask_ref_order is not None
        (
            self.state.hb_new,
            self.state.bid_queue_value_force_keep,
            self.state.bid_queue_value_assigned,
        ) = self._maybe_apply_queue_value_keep_cancel(
            "BUY",
            int(self.state.t),
            can_post=bool(self.state.hb_new),
            ref_order=self.state.bid_ref_order,
            side_ctx=self.state.quote_context["BUY"],
            block_reasons=self.state.bid_block_reasons,
        )
        (
            self.state.ha_new,
            self.state.ask_queue_value_force_keep,
            self.state.ask_queue_value_assigned,
        ) = self._maybe_apply_queue_value_keep_cancel(
            "SELL",
            int(self.state.t),
            can_post=bool(self.state.ha_new),
            ref_order=self.state.ask_ref_order,
            side_ctx=self.state.quote_context["SELL"],
            block_reasons=self.state.ask_block_reasons,
        )
        self.state.nbid, self.state.bid_state_policy_applied = self._maybe_apply_state_conditioned_policy(
            "BUY",
            int(self.state.t),
            baseline_price=float(self.state.nbid),
            other_side_price=float(self.state.nask),
            final_size=float(self.state.bid_sz),
            can_post=bool(self.state.hb_new),
            side_orders=self.state.bid_orders,
            side_ctx=self.state.quote_context["BUY"],
            max_pair_spread=float(self.state.max_spread_post_policy),
        )
        self.state.nask, self.state.ask_state_policy_applied = self._maybe_apply_state_conditioned_policy(
            "SELL",
            int(self.state.t),
            baseline_price=float(self.state.nask),
            other_side_price=float(self.state.nbid),
            final_size=float(self.state.ask_sz),
            can_post=bool(self.state.ha_new),
            side_orders=self.state.ask_orders,
            side_ctx=self.state.quote_context["SELL"],
            max_pair_spread=float(self.state.max_spread_post_policy),
        )
        self.state.nbid, self.state.bid_local_action_applied = self._maybe_apply_local_action_ope(
            "BUY",
            int(self.state.t),
            baseline_price=float(self.state.nbid),
            other_side_price=float(self.state.nask),
            final_size=float(self.state.bid_sz),
            can_post=bool(self.state.hb_new),
            side_orders=self.state.bid_orders,
            side_ctx=self.state.quote_context["BUY"],
            max_pair_spread=float(self.state.max_spread_post_policy),
        )
        self.state.nask, self.state.ask_local_action_applied = self._maybe_apply_local_action_ope(
            "SELL",
            int(self.state.t),
            baseline_price=float(self.state.nask),
            other_side_price=float(self.state.nbid),
            final_size=float(self.state.ask_sz),
            can_post=bool(self.state.ha_new),
            side_orders=self.state.ask_orders,
            side_ctx=self.state.quote_context["SELL"],
            max_pair_spread=float(self.state.max_spread_post_policy),
        )
        if (
            self.state.bid_state_policy_applied
            or self.state.ask_state_policy_applied
            or self.state.bid_local_action_applied
            or self.state.ask_local_action_applied
                or self.state.bid_add_skip_assigned
                or self.state.ask_add_skip_assigned
            or self.state.bid_state_rearm_assigned
            or self.state.ask_state_rearm_assigned
            or self.state.bid_queue_value_assigned
            or self.state.ask_queue_value_assigned
        ):
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        # These are live controls, not experimental arm outputs. Apply
        # them after every policy that can reopen a side so a downstream
        # shadow action cannot bypass the production blocker.
        if self.state.bid_route_due:
            self._evaluate_dynamic_fill_hazard_prospective_recovery(
                candidate_price=float(self.state.nbid),
                now_ts=int(self.state.t),
            )
        if self.state.bid_route_due and self._dynamic_fill_hazard_buy_blocked() and self.state.hb_new:
            self.state.hb_new = False
            self.state.bid_block_reasons.append("dynamic_fill_hazard_q90")
            self.state.quote_context["BUY"]["dynamic_fill_hazard_q90_block"] = True
            self.state.dynamic_fill_hazard_blocked_quote_count += 1

        self.state.sync_adjust_active = bool(
            self.state.sync_adjust_degrade_enabled
            and int(self.state.t) < int(self.state.sync_adjust_degrade_until_ms)
        )
        self.state.quote_context["BUY"]["sync_adjust_degrade_block"] = False
        self.state.quote_context["SELL"]["sync_adjust_degrade_block"] = False
        if self.state.sync_adjust_active:
            if (
                self.state.bid_route_due
                and self.state.q >= -max(float(self.state.LOT_SIZE) * 0.5, 1e-12)
                and self.state.hb_new
            ):
                self.state.hb_new = False
                self.state.bid_block_reasons.append("sync_adjust_degrade")
                self.state.quote_context["BUY"]["sync_adjust_degrade_block"] = True
                self.state.sync_adjust_degrade_block_bid_count += 1
            if (
                self.state.ask_route_due
                and self.state.q <= max(float(self.state.LOT_SIZE) * 0.5, 1e-12)
                and self.state.ha_new
            ):
                self.state.ha_new = False
                self.state.ask_block_reasons.append("sync_adjust_degrade")
                self.state.quote_context["SELL"]["sync_adjust_degrade_block"] = True
                self.state.sync_adjust_degrade_block_ask_count += 1

        if self.state.bid_route_due:
            self._post_cooldown_budget_observe_release_surface(
                "BUY",
                now_ts=int(self.state.t),
                can_post=bool(self.state.hb_new),
                side_orders=self.state.bid_orders,
                block_reasons=self.state.bid_block_reasons,
            )
        if self.state.ask_route_due:
            self._post_cooldown_budget_observe_release_surface(
                "SELL",
                now_ts=int(self.state.t),
                can_post=bool(self.state.ha_new),
                side_orders=self.state.ask_orders,
                block_reasons=self.state.ask_block_reasons,
            )

        self.state.bid_multi_short_price_changed = False
        if self.state.bid_route_due:
            self.state.nbid, self.state.bid_multi_short_price_changed = (
                self._apply_multi_short_repair_quote(
                    int(self.state.t),
                    baseline_bid=float(self.state.nbid),
                    best_bid=float(self.state.cur_best_bid),
                    best_ask=float(self.state.cur_best_ask),
                    mark_px=float(self.state.mid),
                    can_post=bool(self.state.hb_new),
                    defense_pause_observed=bool(self.state.bid_defense_hard_pause),
                    defense_override_requested=bool(
                        self.state.bid_multi_short_defense_override_requested
                    ),
                    side_ctx=self.state.quote_context["BUY"],
                )
            )
        if (
            self.state.bid_multi_short_price_changed
            or self.state.bid_multi_short_defense_override_requested
        ):
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        self.state.ask_price_penalty_applied = False
        if self.state.ask_route_due:
            self.state.nask, self.state.ask_price_penalty_applied = (
                self._apply_sell_add_price_penalty_action(
                    int(self.state.t),
                    baseline_bid=float(self.state.nbid),
                    baseline_ask=float(self.state.nask),
                    mark_px=float(self.state.mid),
                    max_pair_spread=float(self.state.max_spread_post_policy),
                    can_post=bool(self.state.ha_new),
                    side_ctx=self.state.quote_context["SELL"],
                )
            )
        if self.state.ask_price_penalty_applied:
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        self.state.fair_center_pair_changed = False
        if self.state.pair_route_due:
            self.state.nbid, self.state.nask, self.state.fair_center_pair_changed = self._apply_fair_center_shift(
                int(self.state.t),
                baseline_bid=float(self.state.nbid),
                baseline_ask=float(self.state.nask),
                best_bid=float(self.state.cur_best_bid),
                best_ask=float(self.state.cur_best_ask),
                mark_px=float(self.state.mid),
                bid_can_post=bool(self.state.hb_new),
                ask_can_post=bool(self.state.ha_new),
            )
        if self.state.fair_center_pair_changed:
            self._refresh_final_quote_context(
                self.state.quote_context,
                self.state.quote_flags,
                self.state.mid,
                self.state.fair,
                self.state.nbid,
                self.state.nask,
            )

        if self.state.conditional_p3_reach_gate_enabled:
            self.state.prediction_ready = bool(
                self.context.ml_data is not None
                and 0 <= int(self.state.ml_idx) < len(self.state.ml_ts)
                and int(self.state.ml_ts[int(self.state.ml_idx)])
                <= int((self.state.signal_visible_ts // 1000) * 1000)
            )
            self.state.opener = bool(abs(float(self.state.q)) <= float(self.state.LOT_SIZE) * 0.5)
            self.state.bid_exposure = bool(self.state.q >= 0.0)
            self.state.ask_exposure = bool(self.state.q <= 0.0)
            self.state.bid_distance_ticks = (
                self.services.executable_price_tick(
                    float(self.state.cur_best_bid), self.state.TICK, name="best_bid"
                )
                - self.services.executable_price_tick(float(self.state.nbid), self.state.TICK, name="bid_price")
            )
            self.state.ask_distance_ticks = (
                self.services.executable_price_tick(float(self.state.nask), self.state.TICK, name="ask_price")
                - self.services.executable_price_tick(
                    float(self.state.cur_best_ask), self.state.TICK, name="best_ask"
                )
            )
            self.state.bid_gate_status = 0
            self.state.ask_gate_status = 0
            if self.state.bid_route_due and self.state.hb_new and self.state.bid_exposure:
                self.state.conditional_p3_reach_gate_eval_count += 1
                self.state.bid_gate_status = self._conditional_p3_status_for(
                    side="BUY",
                    opener=self.state.opener,
                    distance_ticks=self.state.bid_distance_ticks,
                    prediction_index=int(self.state.ml_idx),
                    prediction_ready=self.state.prediction_ready,
                )
                self.state.conditional_p3_reach_gate_supported_count += int(
                    self.state.bid_gate_status > 0
                )
                self.state.conditional_p3_reach_gate_toxicity_trigger_count += int(
                    float(self.state.cur_tox_bid)
                    >= self.state.conditional_p3_reach_gate_buy_toxicity_threshold
                )
            if self.state.ask_route_due and self.state.ha_new and self.state.ask_exposure:
                self.state.conditional_p3_reach_gate_eval_count += 1
                self.state.ask_gate_status = self._conditional_p3_status_for(
                    side="SELL",
                    opener=self.state.opener,
                    distance_ticks=self.state.ask_distance_ticks,
                    prediction_index=int(self.state.ml_idx),
                    prediction_ready=self.state.prediction_ready,
                )
                self.state.conditional_p3_reach_gate_supported_count += int(
                    self.state.ask_gate_status > 0
                )
                self.state.conditional_p3_reach_gate_toxicity_trigger_count += int(
                    float(self.state.cur_tox_ask)
                    >= self.state.conditional_p3_reach_gate_sell_toxicity_threshold
                )

            self.state.p3_quote_decision = self.services.apply_conditional_p3_outward_quote_action(
                bid_price=float(self.state.nbid),
                ask_price=float(self.state.nask),
                best_bid=float(self.state.cur_best_bid),
                best_ask=float(self.state.cur_best_ask),
                inventory_btc=float(self.state.q),
                lot_size=float(self.state.LOT_SIZE),
                tick_size=float(self.state.TICK),
                max_pair_spread=float(self.state.max_spread_post_policy),
                bid_allowed=bool(self.state.bid_route_due and self.state.hb_new),
                ask_allowed=bool(self.state.ask_route_due and self.state.ha_new),
                tox_bid=float(self.state.cur_tox_bid),
                tox_ask=float(self.state.cur_tox_ask),
                bid_toxicity_threshold=(
                    self.state.conditional_p3_reach_gate_buy_toxicity_threshold
                ),
                ask_toxicity_threshold=(
                    self.state.conditional_p3_reach_gate_sell_toxicity_threshold
                ),
                bid_gate_status=self.state.bid_gate_status,
                ask_gate_status=self.state.ask_gate_status,
                outward_ticks=self.state.conditional_p3_reach_gate_outward_ticks,
            )
            self.state.conditional_p3_reach_gate_pass_count += int(
                self.state.p3_quote_decision.bid_requested
            ) + int(self.state.p3_quote_decision.ask_requested)
            self.state.conditional_p3_reach_gate_price_change_count += int(
                self.state.p3_quote_decision.bid_price_changed
            ) + int(self.state.p3_quote_decision.ask_price_changed)
            self.state.conditional_p3_reach_gate_buy_price_change_count += int(
                self.state.p3_quote_decision.bid_price_changed
            )
            self.state.conditional_p3_reach_gate_sell_price_change_count += int(
                self.state.p3_quote_decision.ask_price_changed
            )
            self.state.conditional_p3_reach_gate_spread_cap_noop_count += int(
                self.state.p3_quote_decision.bid_spread_cap_noop
            ) + int(self.state.p3_quote_decision.ask_spread_cap_noop)
            self.state.nbid = float(self.state.p3_quote_decision.bid_price)
            self.state.nask = float(self.state.p3_quote_decision.ask_price)
            for loop_item in (
                (
                    "BUY",
                    self.state.bid_distance_ticks,
                    self.state.bid_gate_status,
                    self.state.p3_quote_decision.bid_requested,
                    self.state.p3_quote_decision.bid_price_changed,
                ),
                (
                    "SELL",
                    self.state.ask_distance_ticks,
                    self.state.ask_gate_status,
                    self.state.p3_quote_decision.ask_requested,
                    self.state.p3_quote_decision.ask_price_changed,
                ),
            ):
                self.state.side, self.state.distance, self.state.status, self.state.requested, self.state.changed = loop_item
                self.state.quote_context[self.state.side]["conditional_p3_baseline_distance_ticks"] = int(
                    self.state.distance
                )
                self.state.quote_context[self.state.side]["conditional_p3_reach_gate_status"] = int(
                    self.state.status
                )
                self.state.quote_context[self.state.side]["conditional_p3_reach_gate_requested"] = bool(
                    self.state.requested
                )
                self.state.quote_context[self.state.side]["conditional_p3_price_changed"] = bool(
                    self.state.changed
                )
            if (
                self.state.p3_quote_decision.bid_price_changed
                or self.state.p3_quote_decision.ask_price_changed
            ):
                self._refresh_final_quote_context(
                    self.state.quote_context,
                    self.state.quote_flags,
                    self.state.mid,
                    self.state.fair,
                    self.state.nbid,
                    self.state.nask,
                )

        self.state.ranked_guard_bid_directive = None
        self.state.ranked_guard_ask_directive = None
        self.state.ranked_guard_bid_decision_id = ""
        self.state.ranked_guard_ask_decision_id = ""
        self.state.ranked_guard_bid_cancel_order_id = ""
        self.state.ranked_guard_ask_cancel_order_id = ""
        if self.state.ranked_toxicity_guard_binding_active:
            self.state.untreated_lineage_ordinal = int(
                self.state.inventory_lifecycle_count if self.state.inventory_lifecycle_active else self.state.inventory_lifecycle_count + 1
            )
            self.state.bid_exposure_increasing = bool(self.state.q >= 0.0)
            self.state.ask_exposure_increasing = bool(self.state.q <= 0.0)
            if self.state.bid_route_due:
                self.state.ranked_guard_bid_decision_id = (
                    f"BTCUSDC:{int(self.state.t)}:{int(self.state.i)}:BUY"
                )
            if self.state.ask_route_due:
                self.state.ranked_guard_ask_decision_id = (
                    f"BTCUSDC:{int(self.state.t)}:{int(self.state.i)}:SELL"
                )
            self.state.bid_active_exposure_order_id = (
                self._ranked_guard_order_id(self.state.bid_ref_order)
                if self.state.bid_exposure_increasing
                and self.state.bid_ref_order is not None
                and int(self.state.bid_ref_order.get("cancel_ts", -1) or -1) <= 0
                else ""
            )
            self.state.ask_active_exposure_order_id = (
                self._ranked_guard_order_id(self.state.ask_ref_order)
                if self.state.ask_exposure_increasing
                and self.state.ask_ref_order is not None
                and int(self.state.ask_ref_order.get("cancel_ts", -1) or -1) <= 0
                else ""
            )
            if self.state.bid_route_due:
                self.state.ranked_guard_bid_directive = self.context.ranked_toxicity_guard_binding.on_quote_decision(
                    decision_id=self.state.ranked_guard_bid_decision_id,
                    decision_ts_ns=int(self.state.t) * 1_000_000,
                    side="BUY",
                    role=self.services.inventory_role_for_quote(
                        "BUY", float(self.state.q), float(self.state.order_size)
                    ),
                    baseline_eligible=bool(
                        self.state.hb_new and self.state.bid_exposure_increasing
                    ),
                    exposure_increasing=self.state.bid_exposure_increasing,
                    can_post=bool(self.state.hb_new),
                    allow_exposure_increase=bool(
                        self.state.hb_new or not self.state.bid_exposure_increasing
                    ),
                    active_exposure_order_id=(
                        self.state.bid_active_exposure_order_id
                    ),
                    quote_price=float(self.state.nbid),
                    quote_quantity=float(self.state.bid_sz),
                    blocker_reasons=tuple(self.state.bid_block_reasons),
                    policy_fingerprint=(
                        self.state.ranked_toxicity_guard_policy_fingerprint
                    ),
                    untreated_lineage_ordinal=self.state.untreated_lineage_ordinal,
                )
            if self.state.ask_route_due:
                self.state.ranked_guard_ask_directive = self.context.ranked_toxicity_guard_binding.on_quote_decision(
                    decision_id=self.state.ranked_guard_ask_decision_id,
                    decision_ts_ns=int(self.state.t) * 1_000_000,
                    side="SELL",
                    role=self.services.inventory_role_for_quote(
                        "SELL", float(self.state.q), float(self.state.order_size)
                    ),
                    baseline_eligible=bool(
                        self.state.ha_new and self.state.ask_exposure_increasing
                    ),
                    exposure_increasing=self.state.ask_exposure_increasing,
                    can_post=bool(self.state.ha_new),
                    allow_exposure_increase=bool(
                        self.state.ha_new or not self.state.ask_exposure_increasing
                    ),
                    active_exposure_order_id=(
                        self.state.ask_active_exposure_order_id
                    ),
                    quote_price=float(self.state.nask),
                    quote_quantity=float(self.state.ask_sz),
                    blocker_reasons=tuple(self.state.ask_block_reasons),
                    policy_fingerprint=(
                        self.state.ranked_toxicity_guard_policy_fingerprint
                    ),
                    untreated_lineage_ordinal=self.state.untreated_lineage_ordinal,
                )
            if (
                self.state.bid_route_due
                and self.state.ranked_guard_bid_directive is not None
                and self.state.bid_exposure_increasing
                and not self.state.ranked_guard_bid_directive.allow_exposure_submission
            ):
                self.state.hb_new = False
                self.state.bid_block_reasons.append("ranked_toxicity_guard")
                self.state.quote_context["BUY"]["ranked_toxicity_guard_block"] = True
            if (
                self.state.ask_route_due
                and self.state.ranked_guard_ask_directive is not None
                and self.state.ask_exposure_increasing
                and not self.state.ranked_guard_ask_directive.allow_exposure_submission
            ):
                self.state.ha_new = False
                self.state.ask_block_reasons.append("ranked_toxicity_guard")
                self.state.quote_context["SELL"]["ranked_toxicity_guard_block"] = True
            if (
                self.state.ranked_guard_bid_directive is not None
                and self.state.ranked_guard_bid_directive.request_cancel_once
            ):
                self.state.ranked_guard_bid_cancel_order_id = str(
                    self.state.ranked_guard_bid_directive.cancel_order_id
                )
            if (
                self.state.ranked_guard_ask_directive is not None
                and self.state.ranked_guard_ask_directive.request_cancel_once
            ):
                self.state.ranked_guard_ask_cancel_order_id = str(
                    self.state.ranked_guard_ask_directive.cancel_order_id
                )

    def _resolve_quote_order_updates(self):
        """Observe candidate updates and coalesce unresolved order ownership."""
        if self.state.target_observation is not None:
            # Final prices consumed below, including KEEP/blocked decisions;
            # not called by pending maintenance or frozen HTTP continuations.
            self.context.compute = self.state.active_quote_compute
            self.context.entry = int(self.context.compute["entry_ts"]) if self.context.compute is not None else int(self.state.t)
            self.context.capture = int(self.context.compute["capture_ts"]) if self.context.compute is not None else int(self.state.t)
            self.context.ready = int(self.context.compute["request_ready_ts"]) if self.context.compute is not None else int(self.state.main_loop_tick_complete_ms)
            for loop_item in (
                ("BUY", self.state.nbid, self.state.bid_route_due),
                ("SELL", self.state.nask, self.state.ask_route_due),
            ):
                self.context.observed_side, self.context.target, self.context.routed = loop_item
                if not self.context.routed:
                    continue
                self.context.row = self.state.target_observation.publish(
                    side=self.context.observed_side, call_id=f"requote:{self.state.target_observation.call_sequence}", price=float(self.context.target),
                    observation_ns=self.context.ready * 1_000_000, logical_ns=int(self.state.t) * 1_000_000,
                    trigger_kind=("terminal" if self.state.continuation_side_routing else "ordinary"),
                    stages=dict(entry_ts=self.context.entry, capture_ts=self.context.capture, request_ready_ts=self.context.ready,
                        compute_mode=(self.state.compute_path if self.state.runtime_compute_by_path else
                                      "pooled_split" if self.context.compute is not None else "original_completion")),
                    source_version=dict(bbo_index=self.state.decision_bbo_idx,
                                        l2_index=self.state.decision_l2_idx),
                )
                if self.context.row is not None and self.state.l2_journal is not None:
                    self.context.token = self.state.l2_production.observe("target_publication", int(self.state.t), self.context.observed_side)
                    self.state.l2_journal.emit("target_publication", int(self.state.t), side=self.context.observed_side,
                                                payload=self.context.row, production_event=self.context.token)
                if self.context.row is not None and self.state.target_journal is not None:
                    self.context.token = self.state.target_production.observe("target_publication", int(self.state.t), self.context.observed_side)
                    self.state.target_journal.emit("target_publication", int(self.state.t), side=self.context.observed_side,
                                                   payload=self.context.row, production_event=self.context.token)
            self.state.target_observation.advance(int(self.state.t) * 1_000_000)

        self.context.response_sequence_before = (
            self.state.response_update_observer.cursor.sequence
            if self.state.response_action_runtime is not None else 0)
        if self.state.response_update_observer is not None:
            for loop_item in (
                ('BUY', self.state.nbid, self.state.bid_sz, self.state.bid_ref_order,
                 self.state.hb_new, self.state.bid_updated,
                 self.state.bid_p3_floor_force_update or self.state.bid_notional_force_update,
                 self.state.bid_queue_value_force_keep),
                ('SELL', self.state.nask, self.state.ask_sz, self.state.ask_ref_order,
                 self.state.ha_new, self.state.ask_updated,
                 self.state.ask_p3_floor_force_update or self.state.ask_notional_force_update,
                 self.state.ask_queue_value_force_keep),
            ):
                self.context.side, self.context.target, self.context.quantity, self.context.order, self.context.route, self.context.needs, self.context.force, self.context.queue_keep = loop_item
                self.context.routed = self.state.bid_route_due if self.context.side == 'BUY' else self.state.ask_route_due
                if not self.context.routed:
                    continue
                self.context.old_price = float(self.context.order['price']) if self.context.order is not None else None
                self.context.ticks, self.context.interval, self.context._ = self._replace_throttle_params(self.context.side, self.state.q)
                self.context.created = int(self.context.order.get('submit_ts', self.context.order.get('activate_ts', self.state.t)) or self.state.t) if self.context.order else self.state.t
                self.context.compute = self.state.active_quote_compute or dict(
                    entry_ts=self.state.t, capture_ts=self.state.t,
                    request_ready_ts=self.state.main_loop_tick_complete_ms)
                self.context.row = self.state.response_update_observer.observe(
                    side=self.context.side, now_ms=int(self.state.t), call_sequence=int(self.state.nrq),
                    inventory=float(self.state.q), target_price=float(self.context.target), order_price=self.context.old_price,
                    order_id=str(self.context.order.get('trace_id', '')) if self.context.order else None,
                    order_active=self.context.order is not None and self.context.order.get('state') == self.state.ORDER_OPEN,
                    needs_update=bool(self.context.needs), force_update=bool(self.context.force),
                    quantity_changed=(self.context.order is not None and abs(float(self.context.quantity)-float(self.context.order['remaining'])) > max(self.state.LOT_SIZE*1e-9, 1e-12)),
                    pending=self.context.order is not None and self.context.order.get('state') == self.state.ORDER_PENDING_CANCEL,
                    age_ms=max(0, int(self.state.t)-self.context.created), interval_ms=self.context.interval,
                    price_delta_ticks=(abs(self.services._price_to_tick(self.context.target, self.state.TICK)-self.services._order_price_tick(self.context.order, self.state.TICK)) if self.context.order else 0),
                    fixed_ticks=self.context.ticks, route_allowed=bool(self.context.route), queue_keep=bool(self.context.queue_keep), compute=self.context.compute,
                    emit_row=self.state.l2_journal is not None or self.state.response_update_branch_enabled)
                if self.state.response_update_branch_enabled:
                    self.context.row['old_quantity'] = float(self.context.order['remaining']) if self.context.order is not None else None
                    from strategy.optional_update import admission_reasons
                    self.context.admission_reasons = admission_reasons
                    self.context.orders = self.state.bid_orders if self.context.side == 'BUY' else self.state.ask_orders
                    self.context.pending_order = self.state.bid_pending_order if self.context.side == 'BUY' else self.state.ask_pending_order
                    self.context.inventory_allowed = (self.state.bid_can_post_after_inventory if self.context.side == 'BUY'
                                         else self.state.ask_can_post_after_inventory)
                    self.context.forced_cancel = (self.state.ranked_guard_bid_cancel_order_id if self.context.side == 'BUY'
                                     else self.state.ranked_guard_ask_cancel_order_id)
                    self.context.capped = self.services.cap_exposure_qty_by_position_value(
                        side=self.context.side, current_qty=self.state.q, mid=self.state.mid,
                        requested_qty=float(self.context.quantity), max_position_value=self.state.max_position_value,
                        lot=self.state.LOT_SIZE)
                    self.context.budget_allowed = True
                    if (self.state.post_cooldown_inventory_budget_enabled
                            and self.services._exposure_increasing(self.context.side, self.state.q, float(self.context.quantity), self.state.LOT_SIZE)):
                        self.context.budget_allowed, self.context._ = self._post_cooldown_budget_prepare_submission(
                            self.context.side, quantity_btc=float(self.context.quantity), exposure_increasing=True,
                            side_ctx=self.state.quote_context[self.context.side], reserve=False)
                    self.context.reasons = self.context.admission_reasons(local_reasons=self.context.row['local_block_reasons'],
                        order_count=len(self.context.orders), pending=self.context.pending_order is not None,
                        inventory_allowed=bool(self.context.inventory_allowed), forced_cancel=bool(self.context.forced_cancel),
                        quantity=float(self.context.quantity), lot_size=self.state.LOT_SIZE,
                        capped_quantity=self.context.capped, budget_allowed=self.context.budget_allowed)
                    self.state.response_update_admission[self.context.side] = dict(
                        call_sequence=int(self.state.nrq), reasons=self.context.reasons,
                        decision_admissible=not self.context.reasons, future_effectiveness='unknown')
                    self.context.row['optional_update_admission'] = dict(self.state.response_update_admission[self.context.side])
                    if (self.state.response_action_sampler is not None
                            or self.state.response_action_runtime is not None):
                        self.context.ctx = self.state.quote_context[self.context.side]
                        self.context.row['action_context'] = {name: self.context.ctx.get(name) for name in (
                            'pred_dir', 'pred_ret', 'tox_bid', 'tox_ask', 'book_imb',
                            'raw_half_spread', 'raw_mid_shift')}
                        self.context.row['action_context'].update(
                            order_age_s=max(0, int(self.state.t)-self.context.created)/1000.,
                            confirmed_fill_qty=(float(self.context.order['quantity'])-float(self.context.order['remaining'])
                                                if self.context.order is not None else None),
                            local_inventory=float(self.state.q),
                            signed_target_delta_ticks=((1 if self.context.side == 'BUY' else -1)*(float(self.context.target)-self.context.old_price)/self.state.TICK
                                                       if self.context.old_price is not None else None),
                            **{'role_'+role: float(self.context.row['role'] == role) for role in ('opener', 'add', 'reducing')})
                        if self.state.response_action_sampler is not None:
                            self.state.response_action_sampler.consider(self.context.row)
                if self.state.l2_journal is not None:
                    self.context.token = self.state.l2_production.observe('optional_update_observation', int(self.state.t), self.context.side)
                    self.state.l2_journal.emit('optional_update_observation', int(self.state.t), side=self.context.side,
                                               payload=self.context.row, production_event=self.context.token)

        if self.state.hb_new:
            self.state.bid_updated = self._apply_replace_throttle(
                "BUY",
                self.state.t,
                self.state.q,
                self.state.nbid,
                self.state.bid_ref_order,
                self.state.bid_updated,
                force_update=self.state.bid_p3_floor_force_update or self.state.bid_notional_force_update,
            )
        if self.state.ha_new:
            self.state.ask_updated = self._apply_replace_throttle(
                "SELL",
                self.state.t,
                self.state.q,
                self.state.nask,
                self.state.ask_ref_order,
                self.state.ask_updated,
                force_update=self.state.ask_p3_floor_force_update or self.state.ask_notional_force_update,
            )
        if self.state.response_action_runtime is not None:
            # Same frozen measured event/call budget for both model arms.
            # No wall-clock sampling or RNG is performed during replay.
            self.context.extra_ms = self.state.response_action_runtime.charge_ms(
                self.state.response_update_observer.cursor.sequence - self.context.response_sequence_before)
            self.state.main_loop_tick_complete_ms += self.context.extra_ms
            self.state.active_quote_compute['request_ready_ts'] += self.context.extra_ms
            for loop_item in ('BUY', 'SELL'):
                self.context.side = loop_item
                self.context.token = self.state.response_update_observer.token(self.context.side, int(self.state.nrq))
                if self.context.token is not None and self.context.token['row'] is not None:
                    self.context.token['row']['compute'].update(self.state.active_quote_compute)
                    self.context.token['row']['compute']['response_extra_ms'] = self.context.extra_ms
        if (
            self.state.bid_queue_value_force_keep
            and not self.state.bid_p3_floor_force_update
            and not self.state.bid_notional_force_update
            and self.state.hb_new
            and self.state.bid_ref_order is not None
        ):
            self.state.bid_updated = False
            self.state.quote_context["BUY"]["queue_value_forced_keep"] = True
        if (
            self.state.ask_queue_value_force_keep
            and not self.state.ask_p3_floor_force_update
            and not self.state.ask_notional_force_update
            and self.state.ha_new
            and self.state.ask_ref_order is not None
        ):
            self.state.ask_updated = False
            self.state.quote_context["SELL"]["queue_value_forced_keep"] = True

        if self.context.public_strategy is not None:
            self.state.hb_new, self.state.bid_updated = self.context.public_strategy.continuation(
                self.state.public_actions["BUY"]["action"], enabled=self.state.hb_new,
                updated=self.state.bid_updated, has_order=self.state.bid_ref_order is not None,
                force_update=self.state.bid_p3_floor_force_update or self.state.bid_notional_force_update,
            )
            self.state.ha_new, self.state.ask_updated = self.context.public_strategy.continuation(
                self.state.public_actions["SELL"]["action"], enabled=self.state.ha_new,
                updated=self.state.ask_updated, has_order=self.state.ask_ref_order is not None,
                force_update=self.state.ask_p3_floor_force_update or self.state.ask_notional_force_update,
            )
        if not self.state.bid_route_due:
            self.state.bid_updated = False
            self.state.ranked_guard_bid_cancel_order_id = ""
        if not self.state.ask_route_due:
            self.state.ask_updated = False
            self.state.ranked_guard_ask_cancel_order_id = ""

        self.state.bid_pending_coalesce = coalesce_pending_replace(
            enabled=self.state.replace_pending_coalesce,
            needs_update=self.state.bid_updated, can_post=self.state.hb_new,
            pending=self.state.bid_pending_order is not None,
        )
        self.state.ask_pending_coalesce = coalesce_pending_replace(
            enabled=self.state.replace_pending_coalesce,
            needs_update=self.state.ask_updated, can_post=self.state.ha_new,
            pending=self.state.ask_pending_order is not None,
        )
        if self.state.bid_pending_coalesce:
            self.state.bid_updated = False
        if self.state.ask_pending_coalesce:
            self.state.ask_updated = False

        if self.state.fair_center_pair_changed and self.state.fair_center_open_record is not None:
            self.state.actual_change = bool(
                (self.state.hb_new and self.state.bid_updated and not self.state.bid_pending_coalesce)
                or (self.state.ha_new and self.state.ask_updated and not self.state.ask_pending_coalesce)
            )
            if self.state.actual_change:
                self.state.fair_center_open_record[
                    "actual_final_action_change_count"
                ] = int(
                    self.state.fair_center_open_record.get(
                        "actual_final_action_change_count", 0
                    )
                ) + 1
                if int(
                    self.state.fair_center_open_record.get(
                        "final_action_change_ts_ms", 0
                    )
                ) <= 0:
                    self.state.fair_center_open_record[
                        "final_action_change_ts_ms"
                    ] = int(self.state.t)
                    self.state.fair_center_open_record[
                        "final_action_change_status"
                    ] = "observed"
                    self.state.fair_center_open_record["final_blocker"] = "none"
                    self._append_fair_center_event(
                        self.state.fair_center_open_record,
                        "first_final_quote_action_change",
                        int(self.state.t),
                    )
            else:
                if self.state.bid_pending_coalesce or self.state.ask_pending_coalesce:
                    self.state.blocker = "pending_coalesce"
                elif not self.state.hb_new and not self.state.ha_new:
                    self.state.blocker = "quote_blocked"
                elif not self.state.bid_updated and not self.state.ask_updated:
                    self.state.blocker = "lazy_requote_or_replace_throttle"
                else:
                    self.state.blocker = "no_final_order_action"
                self.state.fair_center_open_record["final_blocker"] = self.state.blocker

        self.state.bid_cancel_first = self._should_cancel_first_replace("BUY", self.state.q, self.state.bid_ref_order, self.state.bid_updated, self.state.hb_new)
        self.state.ask_cancel_first = self._should_cancel_first_replace("SELL", self.state.q, self.state.ask_ref_order, self.state.ask_updated, self.state.ha_new)

    def _finalize_order_actions(self):
        """Finalize routed actions and their response/guard accounting."""
        if self.context.public_strategy is not None:
            if (self.state.public_actions["BUY"]["action"] == "cancel"
                    and self.state.bid_route_due and self.state.bid_updated
                    and not self.state.hb_new and self.state.bid_orders):
                self.state.bid_action = "cancel"
            if (self.state.public_actions["SELL"]["action"] == "cancel"
                    and self.state.ask_route_due and self.state.ask_updated
                    and not self.state.ha_new and self.state.ask_orders):
                self.state.ask_action = "cancel"
            self.context.public_strategy.resolved(int(self.state.t) * 1_000_000, "BUY",
                action=self.state.bid_action, price=float(self.state.nbid),
                quantity=float(self.state.bid_sz), route_due=bool(self.state.bid_route_due))
            self.context.public_strategy.resolved(int(self.state.t) * 1_000_000, "SELL",
                action=self.state.ask_action, price=float(self.state.nask),
                quantity=float(self.state.ask_sz), route_due=bool(self.state.ask_route_due))
        self.state.bid_update_cancel_reason = (
            "risk_selection_cancel" if "BUY" in self.state.risk_selection_cancel_sides
            else "requote_replace" if self.state.hb_new else "side_disabled"
        )
        self.state.ask_update_cancel_reason = (
            "risk_selection_cancel" if "SELL" in self.state.risk_selection_cancel_sides
            else "requote_replace" if self.state.ha_new else "side_disabled"
        )
        if (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and self.state.ema_add_wait_fork_target_actual_action == "not_reached"
            and int(self.state.t) == self.state.ema_add_wait_fork_assignment_ts_ms
        ):
            self.state.ema_add_wait_fork_target_actual_action = str(
                self.state.bid_action
                if self.state.ema_add_wait_fork_target_side == "BUY"
                else self.state.ask_action
            )
            self.state.expected_target_action = (
                "place"
                if self.state.ema_add_wait_fork_action == "ADD_NOW"
                else "pause"
            )
            if self.state.ema_add_wait_fork_target_actual_action != self.state.expected_target_action:
                raise RuntimeError(
                    "EMA ADD/WAIT target action was not executed exactly: "
                    f"expected={self.state.expected_target_action}, "
                    f"actual={self.state.ema_add_wait_fork_target_actual_action}"
                )
        if self.state.ranked_toxicity_guard_binding_active:
            if self.state.bid_route_due:
                self.context.ranked_toxicity_guard_binding.on_final_quote_action(
                    decision_id=self.state.ranked_guard_bid_decision_id,
                    side="BUY",
                    role=self.services.inventory_role_for_quote(
                        "BUY", float(self.state.q), float(self.state.order_size)
                    ),
                    exposure_increasing=bool(self.state.q >= 0.0),
                    candidate_action=str(self.state.bid_action),
                    candidate_price=float(self.state.nbid),
                    candidate_quantity=float(self.state.bid_sz),
                    candidate_order_id=(
                        self._ranked_guard_order_id(self.state.bid_ref_order)
                        if self.state.bid_ref_order is not None
                        else ""
                    ),
                    event_ts_ns=int(self.state.t) * 1_000_000,
                )
            if self.state.ask_route_due:
                self.context.ranked_toxicity_guard_binding.on_final_quote_action(
                    decision_id=self.state.ranked_guard_ask_decision_id,
                    side="SELL",
                    role=self.services.inventory_role_for_quote(
                        "SELL", float(self.state.q), float(self.state.order_size)
                    ),
                    exposure_increasing=bool(self.state.q <= 0.0),
                    candidate_action=str(self.state.ask_action),
                    candidate_price=float(self.state.nask),
                    candidate_quantity=float(self.state.ask_sz),
                    candidate_order_id=(
                        self._ranked_guard_order_id(self.state.ask_ref_order)
                        if self.state.ask_ref_order is not None
                        else ""
                    ),
                    event_ts_ns=int(self.state.t) * 1_000_000,
                )
        if self.state.response_update_observer is not None:
            for loop_item in (
                ('BUY', self.state.bid_action, self.state.hb_new, self.state.bid_updated,
                 self.state.bid_sz, self.state.bid_pending_order, self.state.bid_pending_coalesce,
                 self.state.bid_cancel_first, self.state.bid_block_reasons, self.state.bid_can_post_after_inventory),
                ('SELL', self.state.ask_action, self.state.ha_new, self.state.ask_updated,
                 self.state.ask_sz, self.state.ask_pending_order, self.state.ask_pending_coalesce,
                 self.state.ask_cancel_first, self.state.ask_block_reasons, self.state.ask_can_post_after_inventory),
            ):
                self.context.side, self.context.action, self.context.enabled, self.context.updated, self.context.quantity, self.context.pending_order, self.context.coalesced, self.context.cancel_first, self.context.reasons, self.context.inventory_allowed = loop_item
                self.state.response_update_observer.resolved(
                    self.state.response_update_observer.token(self.context.side, int(self.state.nrq)),
                    action=str(self.context.action), logical_ns=int(self.state.t)*1_000_000,
                    can_post=bool(self.context.enabled), needs_update=bool(self.context.updated), final_quantity=float(self.context.quantity),
                    lot_size=float(self.state.LOT_SIZE), inventory_allowed=bool(self.context.inventory_allowed),
                    pending_order_id=str(self.context.pending_order['trace_id']) if self.context.pending_order is not None else None,
                    pending_coalesce=bool(self.context.coalesced), cancel_first=bool(self.context.cancel_first),
                    block_reasons=list(self.context.reasons),
                    request_link_scope='serial_rest_only' if self.state.serial_rest_return_enabled else 'not_instrumented')
        if self.state.bid_route_due:
            self._multi_short_repair_record_final_decision(
                int(self.state.t),
                can_post=bool(self.state.hb_new),
                bid_action=str(self.state.bid_action),
                price_changed=bool(self.state.bid_multi_short_price_changed),
                defense_override_requested=bool(
                    self.state.bid_multi_short_defense_override_requested
                ),
                block_reasons=self.state.bid_block_reasons,
            )
            self._count_decision_action(
                self.state.bid_action,
                side="BUY",
                exposure_increasing=self.state.q >= 0.0,
            )
        if self.state.ask_route_due:
            self._count_decision_action(
                self.state.ask_action,
                side="SELL",
                exposure_increasing=self.state.q <= 0.0,
            )
