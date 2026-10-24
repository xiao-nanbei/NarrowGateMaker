"""Replay orders owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional


class OrderState:
    def _ranked_guard_order_id(self, order: Mapping[str, Any]) -> str:
        return str(order.get("trace_id", "")).strip()

    def _ranked_guard_order_submitted(self, order: Mapping[str, Any]) -> None:
        if not self.state.ranked_toxicity_guard_binding_active:
            return
        role = str(order.get("inventory_role_at_submit", "")).strip().lower()
        self.context.ranked_toxicity_guard_binding.on_order_submitted(
            side=str(order.get("side", "")).upper(),
            order_id=self._ranked_guard_order_id(order),
            initial_quantity=float(order.get("quantity", 0.0) or 0.0),
            visibility_ts_ns=int(order.get("submit_ts", 0) or 0) * 1_000_000,
            exposure_increasing=role in {"opener", "add"},
        )

    def _ranked_guard_order_activated(self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        if not self.state.ranked_toxicity_guard_binding_active:
            return
        self.context.ranked_toxicity_guard_binding.on_order_activated(
            side=str(order.get("side", "")).upper(),
            order_id=self._ranked_guard_order_id(order),
            visibility_ts_ns=int(visibility_ts_ms) * 1_000_000,
            exchange_ts_ns=int(exchange_ts_ms) * 1_000_000,
        )

    def _ranked_guard_cancel_requested(self,
        order: Mapping[str, Any],
        *,
        visibility_ts_ms: int,
        reason: str,
        guard_initiated: bool = False,
    ) -> None:
        if not self.state.ranked_toxicity_guard_binding_active:
            return
        self.context.ranked_toxicity_guard_binding.on_cancel_requested(
            side=str(order.get("side", "")).upper(),
            order_id=self._ranked_guard_order_id(order),
            visibility_ts_ns=int(visibility_ts_ms) * 1_000_000,
            reason=str(reason),
            guard_initiated=bool(guard_initiated),
        )

    def _ranked_guard_order_fill(self,
        order: Mapping[str, Any],
        *,
        remaining_after: float,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
        full_fill: bool,
    ) -> None:
        if not self.state.ranked_toxicity_guard_binding_active:
            return
        self.context.ranked_toxicity_guard_binding.on_order_fill(
            side=str(order.get("side", "")).upper(),
            order_id=self._ranked_guard_order_id(order),
            remaining_after=float(remaining_after),
            visibility_ts_ns=int(visibility_ts_ms) * 1_000_000,
            exchange_ts_ns=int(exchange_ts_ms) * 1_000_000,
            full_fill=bool(full_fill),
        )

    def _ranked_guard_exchange_terminal(self,
        order: Mapping[str, Any],
        *,
        reason: str,
        visibility_ts_ms: int,
        exchange_ts_ms: int,
    ) -> None:
        if not self.state.ranked_toxicity_guard_binding_active:
            return
        self.context.ranked_toxicity_guard_binding.on_exchange_terminal(
            side=str(order.get("side", "")).upper(),
            order_id=self._ranked_guard_order_id(order),
            reason=str(reason),
            visibility_ts_ns=int(visibility_ts_ms) * 1_000_000,
            exchange_ts_ns=int(exchange_ts_ms) * 1_000_000,
        )

    def _drop_orders(self, orders, now_ts: int, reason: str):
        if self.state.serial_rest_return_enabled:
            # A safety decision is still a network request. Keep each order
            # fillable until exchange cancellation, then retain local pending
            # state until its callback. Never teleport outstanding orders out
            # of the exchange because local inventory hit a limit.
            self._request_cancel_all(orders, int(now_ts), reason=str(reason))
            return
        for order in orders:
            self._ranked_guard_cancel_requested(
                order,
                visibility_ts_ms=int(now_ts),
                reason=str(reason),
            )
            self._record_order_lifecycle_cancel_request(
                order,
                int(now_ts),
                reason=str(reason),
            )
            q90_lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(
                str(order.get("trace_id", ""))
            )
            if q90_lifecycle is not None and q90_lifecycle.phase in {
                self.services.OrderLifecyclePhase.ACTIVE,
                self.services.OrderLifecyclePhase.PARTIALLY_FILLED,
            }:
                q90_lifecycle.request_cancel(int(now_ts) * 1_000_000)
                self._append_dynamic_fill_hazard_lifecycle_journal(
                    order,
                    "cancel_request",
                )
            self._record_order_lifecycle_cancel_ack(
                order,
                visibility_ts_ms=int(now_ts),
                exchange_ts_ms=int(now_ts),
                reason=str(reason),
            )
            self._record_queue_value_cancel(order, int(now_ts))
            if self.state.trace_orders is not None:
                self._append_order_outcome(order, now_ts, "cancel", reason)
            self._dynamic_fill_hazard_order_terminal(
                order,
                event="administrative_cancel",
                now_ts=int(now_ts),
            )
            self._ranked_guard_exchange_terminal(
                order,
                reason="shutdown",
                visibility_ts_ms=int(now_ts),
                exchange_ts_ms=int(now_ts),
            )
            self._post_cooldown_budget_release_order(
                order,
                now_ts=int(now_ts),
                reason=str(reason),
            )
        orders.clear()
        self._sync_local_order_lifecycle_repair(int(now_ts))

    def _decision_queue_ahead_at_submit(self,
        side: str,
        price: float,
        submit_ts_ms: int,
    ) -> tuple[float, str, int]:
        """Read exact native depth without mutating the activation queue seed."""

        if self.state.exchange_book_scheduler is None:
            return self.services.math.nan, "unavailable", 0
        lookup = self.state.exchange_book_scheduler.lookup(
            str(side),
            self.services._price_to_tick(float(price), self.state.TICK),
        )
        asof_ns = int(lookup.asof_exchange_ts_ns)
        causal = asof_ns <= int(submit_ts_ms) * 1_000_000
        if not causal or not bool(lookup.strict_usable):
            return self.services.math.nan, str(lookup.status), asof_ns
        if str(lookup.status) == "known_zero":
            return 0.0, "native_exchange_book_known_zero", asof_ns
        if str(lookup.status) == "exact" and lookup.quantity is not None:
            visible_quantity = float(lookup.quantity)
            if not self.services.math.isfinite(visible_quantity):
                return self.services.math.nan, "native_exchange_book_nonfinite", asof_ns
            return (
                max(0.0, visible_quantity),
                "native_exchange_book_exact",
                asof_ns,
            )
        return self.services.math.nan, str(lookup.status), asof_ns

    def _causal_taker_flow_imbalance_5s(self,
        submit_ts_ms: int,
        visible_trade_cutoff_ts_ms: int,
    ) -> tuple[float, int]:
        """Return causal individual-trade flow and its latest source time."""

        if (
            self.state.trace_first_add_decision_to_terminal is None
            and self.state.trace_first_opener_decision_to_terminal is None
        ) or self.state.last_processed_event_idx < 0:
            return 0.0, 0
        visible_cutoff = min(
            int(submit_ts_ms),
            int(visible_trade_cutoff_ts_ms),
        )
        right = min(
            int(self.state.last_processed_event_idx) + 1,
            int(self.services.np.searchsorted(self.state.trade_ts, visible_cutoff, side="right")),
        )
        left = int(
            self.services.np.searchsorted(
                self.state.trade_ts,
                visible_cutoff - 5_000,
                side="left",
            )
        )
        if right <= left:
            return 0.0, 0
        buy_qty = float(
            self.state.taker_buy_qty_cumulative[right] - self.state.taker_buy_qty_cumulative[left]
        )
        sell_qty = float(
            self.state.taker_sell_qty_cumulative[right] - self.state.taker_sell_qty_cumulative[left]
        )
        total = buy_qty + sell_qty
        latest_execution_idx = int(self.state.last_execution_event_index[right - 1])
        source_ts_ms = (
            int(self.state.trade_ts[latest_execution_idx])
            if latest_execution_idx >= left
            else 0
        )
        imbalance = (
            (buy_qty - sell_qty) / total if total > 1e-12 else 0.0
        )
        return float(imbalance), int(source_ts_ms)

    def _make_order(self, side: str, price: float, quantity: float, submit_ts: int,
                    submit_mid: float, quote_flags=None, quote_context=None,
                    *, apply_decision_compute: bool = True, restored_order: bool = False,
                    creation_source: str = "quote_decision",
                    creation_opportunity_event_id: Optional[int] = None,
                    response_observation_token=None):
        quote_flags = quote_flags or {}
        quote_context = quote_context or {}
        side_ctx = quote_context.get(side, {}) if isinstance(quote_context, dict) else {}
        creation_clock = int(submit_ts)
        creation_opportunity = creation_opportunity_event_id
        if creation_opportunity is None and creation_source == "quote_decision" and not restored_order:
            creation_opportunity = self.state.l2_decision_links.get((id(side_ctx), creation_clock, side))
        trace_id = self.state.next_trace_order_id
        self.state.next_trace_order_id += 1
        price_penalty_record = self.state.sell_add_price_penalty_open_record
        if (
            price_penalty_record is not None
            and int(price_penalty_record.get("inventory_lifecycle_id", -1))
            == int(self.state.inventory_lifecycle_count)
        ):
            price_penalty_record["order_submit_count"] = int(
                price_penalty_record.get("order_submit_count", 0)
            ) + 1
            if str(side).upper() == "SELL":
                price_penalty_record["sell_order_submit_count"] = int(
                    price_penalty_record.get("sell_order_submit_count", 0)
                ) + 1
        multi_short_record = self._multi_short_repair_inventory_lifecycle_record()
        if multi_short_record is not None:
            multi_short_record["order_submit_count"] = int(
                multi_short_record.get("order_submit_count", 0)
            ) + 1
            if str(side).upper() == "BUY":
                multi_short_record["buy_order_submit_count"] = int(
                    multi_short_record.get("buy_order_submit_count", 0)
                ) + 1
        fair_center_record = self.state.fair_center_open_record
        if fair_center_record is not None:
            fair_center_record["order_submit_count"] = int(
                fair_center_record.get("order_submit_count", 0)
            ) + 1
        safe_add_rearm_intervention_id = int(
            side_ctx.get("safe_add_rearm_intervention_id", -1) or -1
        )
        safe_add_rearm_record = self.state.safe_add_rearm_open_records.get(
            safe_add_rearm_intervention_id
        )
        if safe_add_rearm_record is not None:
            safe_add_rearm_record["intervention_order_submit_count"] = int(
                safe_add_rearm_record.get("intervention_order_submit_count", 0) + 1
            )
        state_conditioned_rearm_intervention_id = int(
            side_ctx.get("state_conditioned_rearm_intervention_id", -1) or -1
        )
        state_conditioned_rearm_record = (
            self.state.state_conditioned_rearm_open_records.get(
                state_conditioned_rearm_intervention_id
            )
        )
        if state_conditioned_rearm_record is not None:
            state_conditioned_rearm_record["intervention_order_submit_count"] = int(
                state_conditioned_rearm_record.get(
                    "intervention_order_submit_count", 0
                )
                + 1
            )
        add_skip_intervention_id = int(
            side_ctx.get("sell_add_skip_ope_intervention_id", -1) or -1
        )
        add_skip_record = self.state.sell_add_skip_open_records.get(
            add_skip_intervention_id
        )
        if add_skip_record is not None:
            add_skip_record["intervention_order_submit_count"] = int(
                add_skip_record.get("intervention_order_submit_count", 0) + 1
            )
        queue_value_intervention_id = int(
            side_ctx.get("queue_value_keep_cancel_intervention_id", -1) or -1
        )
        queue_value_reentry_order = bool(
            side_ctx.get("queue_value_reentry_order", False)
        )
        (
            decision_queue_ahead_btc,
            decision_queue_ahead_source,
            decision_queue_ahead_asof_ts_ns,
        ) = self._decision_queue_ahead_at_submit(side, price, submit_ts)
        decision_equity = float(self.state.cash) + float(self.state.q) * float(submit_mid)
        inventory_lifecycle_start_equity = (
            float(self.state.inventory_lifecycle_start_pnl)
            if self.state.inventory_lifecycle_active
            else float(decision_equity)
        )
        visible_trade_cutoff_ts_ms = int(
            side_ctx.get("decision_visible_trade_cutoff_ts_ms", submit_ts)
            or submit_ts
        )
        legacy_trade_flow_trace = bool(
            self.state.trace_first_add_decision_to_terminal is not None
            or (
                self.state.trace_first_opener_decision_to_terminal is not None
                and not self.state.first_opener_trace_schema_version.endswith(".v2")
            )
        )
        if legacy_trade_flow_trace:
            (
                individual_trade_taker_flow_imbalance_5s,
                individual_trade_source_ts_ms,
            ) = self._causal_taker_flow_imbalance_5s(
                int(submit_ts),
                visible_trade_cutoff_ts_ms,
            )
        else:
            individual_trade_taker_flow_imbalance_5s = float(
                self.state.cur_prediction_features.get(
                    "taker_quote_imbalance_5s",
                    self.state.cur_prediction_features.get("trade_imbalance_5s", 0.0),
                )
                or 0.0
            )
            individual_trade_source_ts_ms = 0
        decision_visible_mid = float(
            side_ctx.get("decision_visible_mid_usdc_per_btc", submit_mid)
            or submit_mid
        )
        decision_visible_best_bid = float(
            side_ctx.get(
                "decision_visible_best_bid_usdc_per_btc",
                self.state.cur_best_bid,
            )
            or self.state.cur_best_bid
        )
        decision_visible_best_ask = float(
            side_ctx.get(
                "decision_visible_best_ask_usdc_per_btc",
                self.state.cur_best_ask,
            )
            or self.state.cur_best_ask
        )
        restore_without_request = bool(restored_order)
        if restore_without_request:
            # The inherited order already exists at the exchange. Bootstrap
            # its ledger entry without consuming a NEW sample or HTTP lane.
            gateway_request_ts = exchange_effective_ts = local_ack_ts = int(submit_ts)
        else:
            gateway_request_ts, exchange_effective_ts, local_ack_ts = (
                self._sample_new_order_deadlines_ms(
                    request_ts=int(submit_ts),
                    side=str(side),
                    order_ts=int(submit_ts),
                    apply_decision_compute=apply_decision_compute,
                )
            )
        order = {
            "trace_id": trace_id,
            "side": side,
            "price": price,
            "price_tick": self.services._price_to_tick(float(price), self.state.TICK),
            "quantity": quantity,
            "remaining": quantity,
            "exchange_remaining": quantity,
            "submit_ts": submit_ts,
            "gateway_request_ts": int(gateway_request_ts),
            "activate_ts": int(exchange_effective_ts),
            "new_ack_ts": int(local_ack_ts),
            **({"restored_order": True} if restored_order else {}),
            **({"new_rest_return_ts": (
                   -1 if restore_without_request
                   else int(self.state.rest_gateway_last_timing["rest_return_ts_ms"])
               ), "restored_order": restore_without_request}
               if self.state.serial_rest_return_enabled else {}),
            "exchange_accepted": False,
            "local_new_ack_published": False,
            "cancel_effective_ts": -1,
            "cancel_ts": -1,
            "state": self.state.ORDER_PENDING_NEW,
            "quote_ts": submit_ts,
            "mid_at_quote": submit_mid,
            "best_bid_at_submit": float(self.state.cur_best_bid),
            "best_ask_at_submit": float(self.state.cur_best_ask),
            "decision_visible_best_bid_usdc_per_btc": (
                decision_visible_best_bid
            ),
            "decision_visible_best_ask_usdc_per_btc": (
                decision_visible_best_ask
            ),
            "inventory_at_submit": float(self.state.q),
            "decision_equity_at_submit": float(decision_equity),
            "inventory_lifecycle_start_equity_at_submit": float(inventory_lifecycle_start_equity),
            "inventory_lifecycle_start_ts_at_submit": int(
                self.state.inventory_lifecycle_start_ts if self.state.inventory_lifecycle_active else submit_ts
            ),
            "inventory_lifecycle_pnl_so_far_at_submit": float(
                decision_equity - inventory_lifecycle_start_equity
            ),
            "inventory_lifecycle_mae_so_far_at_submit": float(
                self.state.inventory_lifecycle_cur_min_pnl if self.state.inventory_lifecycle_active else 0.0
            ),
            "inventory_lifecycle_exposure_increasing_fills_at_submit": int(
                self.state.first_add_inventory_lifecycle_exposure_increasing_fills
                if self.state.inventory_lifecycle_active
                else 0
            ),
            "inventory_lifecycle_reducing_fills_at_submit": int(
                self.state.first_add_inventory_lifecycle_reducing_fills if self.state.inventory_lifecycle_active else 0
            ),
            "decision_queue_ahead_btc": float(decision_queue_ahead_btc),
            "decision_queue_ahead_source": str(decision_queue_ahead_source),
            "decision_queue_ahead_asof_ts_ns": int(
                decision_queue_ahead_asof_ts_ns
            ),
            "decision_parent_aggtrade_flow_imbalance": float(
                individual_trade_taker_flow_imbalance_5s
            ),
            "decision_individual_trade_taker_flow_imbalance_5s": float(
                individual_trade_taker_flow_imbalance_5s
            ),
            "decision_individual_trade_source_ts_ms": int(
                individual_trade_source_ts_ms
            ),
            "decision_local_toxicity": float(
                self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask
            ),
            "decision_visible_mid_usdc_per_btc": float(decision_visible_mid),
            "decision_visible_equity_usdc": float(self.state.cash)
            + float(self.state.q) * float(decision_visible_mid),
            "decision_visible_bbo_cutoff_ts_ms": int(
                side_ctx.get("decision_visible_bbo_cutoff_ts_ms", submit_ts)
                or submit_ts
            ),
            "decision_bbo_source_ts_ms": int(
                side_ctx.get("decision_bbo_source_ts_ms", 0) or 0
            ),
            "decision_bbo_source_kind": str(
                side_ctx.get("decision_bbo_source_kind", "unavailable")
                or "unavailable"
            ),
            "decision_visible_l2_cutoff_ts_ms": int(
                side_ctx.get("decision_visible_l2_cutoff_ts_ms", submit_ts)
                or submit_ts
            ),
            "decision_l2_source_ts_ms": int(
                side_ctx.get("decision_l2_source_ts_ms", 0) or 0
            ),
            "decision_visible_trade_cutoff_ts_ms": int(
                visible_trade_cutoff_ts_ms
            ),
            "decision_bbo_feature_ready_ts_ms": int(
                side_ctx.get("decision_bbo_feature_ready_ts_ms", submit_ts)
                or submit_ts
            ),
            "decision_l2_feature_ready_ts_ms": int(
                side_ctx.get("decision_l2_feature_ready_ts_ms", submit_ts)
                or submit_ts
            ),
            "exec_depth_visibility_source_offset_ms": int(
                side_ctx.get("exec_depth_visibility_source_offset_ms", 0) or 0
            ),
            "decision_visible_feature_ready_ts_max_ms": int(submit_ts),
            "inventory_lifecycle_id_at_submit": (
                int(self.state.inventory_lifecycle_count) if self.state.inventory_lifecycle_active else 0
            ),
            "inventory_role_at_submit": self.services.inventory_role_for_quote(
                side,
                float(self.state.q),
                float(self.state.order_size),
            ),
            "queue_init": 0.0,
            "queue_left": 0.0,
            "queue_visible_prev": -1.0,
            "queue_trade_since_l2": 0.0,
            "queue_l2_seen_idx": -1,
            "policy_visible_cancel_count": 0.0,
            "policy_visible_cancel_size": 0.0,
            "policy_visible_refill_count": 0.0,
            "policy_visible_refill_size": 0.0,
            "fill_eligible": True,
            "quote_final_compressed": bool(quote_flags.get("final_compressed", False)),
            "quote_delta_cap": bool(quote_flags.get("delta_cap", False)),
            "quote_mid_guard": bool(quote_flags.get("mid_guard", False)),
            "quote_post_only": bool(quote_flags.get("post_only", False)),
            "quote_bid_adverse": bool(quote_flags.get("bid_adverse", False)),
            "quote_ask_adverse": bool(quote_flags.get("ask_adverse", False)),
            "quote_side_adverse": bool(quote_flags.get("side_adverse", False)),
            "quote_defense_guard": bool(quote_flags.get("defense_guard", False)),
            "quote_bid_defense": bool(side == "BUY" and side_ctx.get("defense_guard", False)),
            "quote_ask_defense": bool(side == "SELL" and side_ctx.get("defense_guard", False)),
            "quote_local_extreme_guard": bool(side_ctx.get("local_extreme_guard", False)),
            "quote_bid_quote_ev": bool(quote_flags.get("bid_quote_ev", False)),
            "quote_ask_quote_ev": bool(quote_flags.get("ask_quote_ev", False)),
            "local_action_ope_intervention_id": int(
                side_ctx.get("local_action_ope_intervention_id", -1) or -1
            ),
            "local_action_ope_action": str(
                side_ctx.get("local_action_ope_action", "") or ""
            ),
            "sell_add_skip_ope_intervention_id": int(
                side_ctx.get("sell_add_skip_ope_intervention_id", -1) or -1
            ),
            "sell_add_skip_ope_action": str(
                side_ctx.get("sell_add_skip_ope_action", "") or ""
            ),
            "queue_value_keep_cancel_intervention_id": queue_value_intervention_id,
            "queue_value_keep_cancel_action": str(
                side_ctx.get("queue_value_keep_cancel_action", "") or ""
            ),
            "queue_value_reentry_order": queue_value_reentry_order,
            "state_conditioned_policy_intervention_id": int(
                side_ctx.get("state_conditioned_policy_intervention_id", -1)
                or -1
            ),
            "state_conditioned_policy_candidate_action": str(
                side_ctx.get("state_conditioned_policy_candidate_action", "")
                or ""
            ),
            "state_conditioned_policy_executed_action": str(
                side_ctx.get("state_conditioned_policy_executed_action", "")
                or ""
            ),
            "safe_add_rearm_intervention_id": safe_add_rearm_intervention_id,
            "safe_add_rearm_action": str(
                side_ctx.get("safe_add_rearm_action", "") or ""
            ),
            "state_conditioned_rearm_intervention_id": (
                state_conditioned_rearm_intervention_id
            ),
            "state_conditioned_rearm_action": str(
                side_ctx.get("state_conditioned_rearm_action", "") or ""
            ),
            "quote_diag": side_ctx,
            "cancel_reason": "",
            "cancel_request_ts": -1,
            "ttl_ms": int(side_ctx.get("order_ttl_ms", 0) or 0),
            "reduce_only": bool(side_ctx.get("reduce_only", False)),
            "circuit_breaker_close": bool(
                side_ctx.get("circuit_breaker_close", False)
            ),
            "post_cooldown_inventory_budget_preparation_token": str(
                side_ctx.get(
                    "post_cooldown_inventory_budget_preparation_token",
                    "",
                )
                or ""
            ),
            "ema_add_wait_fork_id": str(
                side_ctx.get("ema_add_wait_fork_id", "") or ""
            ),
            "ema_add_wait_fork_target_order": bool(
                side_ctx.get("ema_add_wait_fork_target_order", False)
            ),
        }
        if order["ema_add_wait_fork_id"]:
            self.state.ema_add_wait_fork_descendant_submit_count += 1
            if order["ema_add_wait_fork_target_order"]:
                if self.state.ema_add_wait_fork_target_order_id >= 0:
                    raise RuntimeError(
                        "EMA ADD/WAIT fork created more than one target order"
                    )
                self.state.ema_add_wait_fork_target_order_id = int(trace_id)
        if queue_value_reentry_order:
            record = self.state.queue_value_open_records.get(queue_value_intervention_id)
            if (
                record is not None
                and str(record.get("action", ""))
                in {
                    "cancel_then_baseline_reenter",
                    "cancel_until_state_exit",
                }
                and int(record.get("reentry_order_id", -1) or -1) < 0
            ):
                submit_ts_ns = int(submit_ts) * 1_000_000
                record["reentry_order_id"] = int(trace_id)
                record["reentry_submit_ts_ns"] = submit_ts_ns
                record["reentry_order_submit_count"] = int(
                    record.get("reentry_order_submit_count", 0) + 1
                )
                record["state_exit_ts_ns"] = submit_ts_ns
                record["state_exit_reason"] = (
                    str(record.get("state_exit_reason", ""))
                    or (
                        "cancel_ack_reenter"
                        if int(record.get("cancel_ack_ts_ns", 0) or 0) > 0
                        else "fill_before_cancel_ack_reenter"
                    )
                )
                if (
                    self.state.queue_value_active_episode_by_side.get(side)
                    == queue_value_intervention_id
                ):
                    self.state.queue_value_active_episode_by_side.pop(side, None)
        self._post_cooldown_budget_attach_prepared_order(
            order,
            exposure_increasing=(
                (side == "BUY" and self.state.q >= -1e-12)
                or (side == "SELL" and self.state.q <= 1e-12)
            ),
        )
        creation_event = self.state.l2_production.observe("order_created", creation_clock, side)
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("order_created", creation_clock, side=side,
                payload={**self._order_trace_fields(order), "creation_source": "restored_initial_state" if restored_order else creation_source,
                         "creation_event_id": creation_event['sequence'],
                         "creation_opportunity_event_id": creation_opportunity,
                         "restored_order": restored_order,
                         "cause_kind": "risk_close" if side_ctx.get('circuit_breaker_close') else creation_source},
                production_event=creation_event)
        if not restored_order:
            if self.state.response_update_observer is not None:
                self.state.response_update_observer.bind_request(
                    response_observation_token, 'new', trace_id, gateway_request_ts)
            if self.state.async_rest_gateway:
                self.state.rest_gateway_last_timing["order"] = order
                if side_ctx.get("circuit_breaker_close", False):
                    if self.state.main_loop_waiting_request is not None:
                        raise RuntimeError("overlapping synchronous GLOBAL FIFO futures")
                    self.state.main_loop_waiting_request = self.state.rest_gateway_last_timing
                    self.state.main_loop_synchronous_request_count += 1
                    self.state.main_loop_next_wake_ms = None
            else:
                self._record_order_lifecycle_submit(order, int(gateway_request_ts))
                self._ranked_guard_order_submitted(order)
        return order

    def _apply_order_safety_constraints(self):
        """Apply final side floor, notional and replacement thresholds."""
        # Research hooks below the operational cooldown must never reopen
        # the held side before the common external epoch, or start a new
        # inventory_lifecycle while this arm is draining toward joint washout.
        if self.state.ema_add_wait_fork_enabled and self.state.ema_add_wait_fork_assigned:
            if self.state.ema_add_wait_fork_wait_active:
                if self.state.ema_add_wait_fork_target_side == "BUY" and float(self.state.q) > 0.0:
                    self.state.hb_new = False
                    if "ema_add_wait_fork" not in self.state.bid_block_reasons:
                        self.state.bid_block_reasons.append("ema_add_wait_fork")
                if self.state.ema_add_wait_fork_target_side == "SELL" and float(self.state.q) < 0.0:
                    self.state.ha_new = False
                    if "ema_add_wait_fork" not in self.state.ask_block_reasons:
                        self.state.ask_block_reasons.append("ema_add_wait_fork")
            if self.state.ema_add_wait_fork_quarantine:
                if float(self.state.q) >= -float(self.state.LOT_SIZE) * 0.5:
                    self.state.hb_new = False
                    if (
                        "ema_add_wait_washout_quarantine"
                        not in self.state.bid_block_reasons
                    ):
                        self.state.bid_block_reasons.append(
                            "ema_add_wait_washout_quarantine"
                        )
                if float(self.state.q) <= float(self.state.LOT_SIZE) * 0.5:
                    self.state.ha_new = False
                    if (
                        "ema_add_wait_washout_quarantine"
                        not in self.state.ask_block_reasons
                    ):
                        self.state.ask_block_reasons.append(
                            "ema_add_wait_washout_quarantine"
                        )

        # Re-assert the side-BBO contract after every replay price action.
        # An already-live order inside the floor must bypass both lazy
        # drift suppression and the generic replace throttle.
        self.state.p3_floor_enabled = bool(
            self.state.diag.get("p3_side_bbo_floor_enabled", False)
        )
        self.state.p3_distance_touch_product_argmax = float(self.state.diag.get("p3_distance_touch_product_argmax", 0.0) or 0.0)
        self.state.p3_floor_active = bool(
            self.state.p3_floor_enabled
            and self.services.math.isfinite(self.state.p3_distance_touch_product_argmax)
            and self.state.p3_distance_touch_product_argmax > 0.0
        )
        (
            self.state.nbid,
            self.state.nask,
            self.state.p3_buy_floor_price,
            self.state.p3_sell_floor_price,
            self.state.p3_final_bid_changed,
            self.state.p3_final_ask_changed,
        ) = self.services.apply_p3_side_bbo_floor(
            self.state.nbid,
            self.state.nask,
            enabled=self.state.p3_floor_active,
            delta_star=self.state.p3_distance_touch_product_argmax,
            best_bid=float(self.state.cur_best_bid),
            best_ask=float(self.state.cur_best_ask),
            tick_size=float(self.state.TICK),
        )
        self.state.p3_tolerance = max(float(self.state.TICK) * 1e-9, 1e-12)
        self.state.bid_p3_floor_force_update = bool(
            self.state.p3_floor_active
            and self.state.bid_ref_order is not None
            and self.state.bid_ref_order.get("state") in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
            and float(self.state.bid_ref_order.get("price", 0.0) or 0.0)
            > self.state.p3_buy_floor_price + self.state.p3_tolerance
        )
        self.state.ask_p3_floor_force_update = bool(
            self.state.p3_floor_active
            and self.state.ask_ref_order is not None
            and self.state.ask_ref_order.get("state") in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
            and float(self.state.ask_ref_order.get("price", 0.0) or 0.0)
            < self.state.p3_sell_floor_price - self.state.p3_tolerance
        )
        self.state.quote_context["BUY"]["p3_final_side_floor_changed"] = bool(
            self.state.p3_final_bid_changed
        )
        self.state.quote_context["SELL"]["p3_final_side_floor_changed"] = bool(
            self.state.p3_final_ask_changed
        )
        self.state.quote_context["BUY"]["p3_active_order_floor_unsafe"] = bool(
            self.state.bid_p3_floor_force_update
        )
        self.state.quote_context["SELL"]["p3_active_order_floor_unsafe"] = bool(
            self.state.ask_p3_floor_force_update
        )


        self.state.bid_notional_force_update = self._kept_order_exceeds_notional("BUY", self.state.bid_ref_order, self.state.q, self.state.mid)
        self.state.ask_notional_force_update = self._kept_order_exceeds_notional("SELL", self.state.ask_ref_order, self.state.q, self.state.mid)
        if self.state.bid_notional_force_update:
            self.state.bid_updated = True
        if self.state.ask_notional_force_update:
            self.state.ask_updated = True

        if self.state.rq_threshold > 0.0:
            if self.state.bid_ref_order is not None and self.state.hb_new and self.state.bid_ref_order["price"] > 0.0:
                self.state.drift_bid = abs(self.state.nbid - self.state.bid_ref_order["price"]) / self.state.bid_ref_order["price"]
                if (self.state.drift_bid <= self.state.rq_threshold and not self.state.bid_p3_floor_force_update
                        and not self.state.bid_notional_force_update):
                    self.state.bid_updated = False
            if self.state.ask_ref_order is not None and self.state.ha_new and self.state.ask_ref_order["price"] > 0.0:
                self.state.drift_ask = abs(self.state.nask - self.state.ask_ref_order["price"]) / self.state.ask_ref_order["price"]
                if (self.state.drift_ask <= self.state.rq_threshold and not self.state.ask_p3_floor_force_update
                        and not self.state.ask_notional_force_update):
                    self.state.ask_updated = False
        if self.state.bid_multi_short_price_changed:
            # The treatment targets the most aggressive valid maker price.
            # It may bypass lazy-drift suppression, but normal replace
            # throttle, cancel ACK, queue reset, and pending coalescing remain.
            self.state.bid_updated = True
