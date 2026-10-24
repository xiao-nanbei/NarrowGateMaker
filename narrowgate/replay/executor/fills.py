"""Replay fills owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional


class FillExecution:
    def _publish_passive_fill(self,
        *,
        side: str,
        order: dict[str, Any],
        fill_qty: float,
        exchange_ts_ms: int,
        visibility_ts_ms: Optional[int] = None,
        trade_idx: int,
        trade_price: float,
        trade_qty: float,
        deplete_mult: float,
        trade_remaining_before: float,
        trade_remaining: float,
        queue_before: float,
        remaining_before: float,
        exchange_reserved: bool = False,
        economic_match_sequence: Optional[int] = None,
    ) -> tuple[float, bool, bool]:
        """Publish one already-matched passive fill to all local consumers."""

        exchange_t = int(exchange_ts_ms)
        t = int(exchange_t if visibility_ts_ms is None else visibility_ts_ms)
        i = int(trade_idx)
        p = float(trade_price)
        qty = float(trade_qty)
        trade_before_order = float(trade_remaining_before)
        rem_before = float(remaining_before)
        filled_buy = False
        filled_sell = False
        if exchange_reserved:
            self.state.private_fill_visible_count += 1
            order["last_private_fill_visible_ts_ms"] = int(t)
        # A later exchange match on the same partially filled order may already
        # be queued. Bind this callback to its own match, not the newest match
        # on the order, and never relabel local visibility as exchange time.
        order["_callback_exchange_ts_ms"] = int(exchange_t)
        if side == "BUY":
            q_before_fill = self.state.q
            self.state.cash -= order["price"] * fill_qty * (1.0 + self.state.maker_fee)
            self.state.q += fill_qty
            self._loss_cooldown_on_fill(
                "BUY",
                float(fill_qty),
                float(order["price"]),
                float(self.state.maker_fee),
            )
            order["remaining"] = self.services.subtract_lot_quantity(order["remaining"], fill_qty, self.state.LOT_SIZE)
            q90_lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(
                str(order.get("trace_id", ""))
            )
            if q90_lifecycle is not None and q90_lifecycle.fill_risk_active:
                q90_full_fill = bool(order["remaining"] < self.state.LOT_SIZE)
                q90_lifecycle.observe_fill(
                    remaining_after=max(0.0, float(order["remaining"])),
                    visibility_ts_ns=int(t) * 1_000_000,
                    exchange_ts_ns=int(exchange_t) * 1_000_000,
                    full_fill=q90_full_fill,
                )
                self._append_dynamic_fill_hazard_lifecycle_journal(
                    order,
                    "full_fill" if q90_full_fill else "partial_fill",
                )
            self._post_cooldown_budget_on_fill(
                order,
                side="BUY",
                fill_qty_btc=float(fill_qty),
                now_ts=int(t),
            )
            if self.state.dynamic_fill_hazard_cpp_runtime is not None:
                hazard_id = str(order.get("trace_id", ""))
                if hazard_id in self.state.dynamic_fill_hazard_paths:
                    matching_hold = bool(
                        self.state.dynamic_fill_hazard_hold is not None
                        and str(
                            self.state.dynamic_fill_hazard_hold["client_order_id"]
                        )
                        == hazard_id
                    )
                    expected_transition = (
                        "terminal_complete_no_reentry"
                        if matching_hold
                        and float(order["remaining"])
                        < float(self.state.LOT_SIZE)
                        else "none"
                    )
                    cpp_transition = (
                        self.state.dynamic_fill_hazard_cpp_runtime.on_fill(
                            hazard_id,
                            float(order["remaining"]),
                            int(t) * 1_000_000,
                        )
                    )
                    self.state.dynamic_fill_hazard_cpp_lifecycle_count += 1
                    self._dynamic_fill_hazard_cpp_check(
                        cpp_transition == expected_transition,
                        "fill_transition",
                        now_ts_ms=int(t),
                        client_order_id=hazard_id,
                        remaining_after=float(order["remaining"]),
                        python_transition=expected_transition,
                        cpp_transition=cpp_transition,
                    )
            if not exchange_reserved:
                trade_remaining = self.services.subtract_lot_quantity(trade_remaining, fill_qty, self.state.LOT_SIZE)
            (
                physical_fill_identity,
                economic_fill_legs,
            ) = self._inventory_lifecycle_legs_for_order_fill(
                order,
                ts_ms=int(t),
                side="BUY",
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                fill_qty=float(fill_qty),
                fill_price=float(order["price"]),
                fee_rate=float(self.state.maker_fee),
                remaining_before=float(rem_before),
                remaining_after=float(order["remaining"]),
            )
            self._record_order_lifecycle_fill(
                order,
                int(t),
                exchange_ts_ms=int(exchange_t),
                fill_qty=float(fill_qty),
                remaining_before=float(rem_before),
                remaining_after=float(order["remaining"]),
                fill_price=float(order["price"]),
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                inventory_lifecycle_id=int(economic_fill_legs[0]["inventory_lifecycle_id"]),
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            ranked_guard_full_fill = float(order["remaining"]) < self.state.LOT_SIZE
            self._ranked_guard_order_fill(
                order,
                remaining_after=(
                    0.0
                    if ranked_guard_full_fill
                    else float(order["remaining"])
                ),
                visibility_ts_ms=int(t),
                exchange_ts_ms=int(exchange_t),
                full_fill=bool(ranked_guard_full_fill),
            )
            self.state.nfb += 1
            if order.get("circuit_breaker_close", False):
                self.state.circuit_breaker_close_fill_count += 1
            self.state.buy_fill_qty_total += fill_qty
            self.state.buy_fill_notional += order["price"] * fill_qty
            filled_buy = True
            if order.get("quote_final_compressed", False):
                self.state.fills_bid_final_compressed += 1
            else:
                self.state.fills_bid_not_final_compressed += 1
            if order.get("quote_mid_guard", False):
                self.state.fills_bid_mid_guard += 1
            if order.get("quote_post_only", False):
                self.state.fills_bid_post_only += 1
            if order.get("quote_bid_adverse", False):
                self.state.fills_bid_adverse_guard += 1
            if order.get("quote_bid_defense", False):
                self.state.fills_bid_defense_guard += 1
            if order.get("quote_local_extreme_guard", False):
                self.state.fills_bid_local_extreme_guard += 1
            if order["state"] == self.state.ORDER_PENDING_CANCEL:
                self.state.fills_while_pending_cancel += 1
                if (
                    self.state.dynamic_fill_hazard_hold is not None
                    and str(
                        self.state.dynamic_fill_hazard_hold["client_order_id"]
                    )
                    == str(order.get("trace_id", ""))
                ):
                    self.state.dynamic_fill_hazard_pre_ack_fill_count += 1
                    if (
                        self.state.variance_time_lineage_randomized_enabled
                        and self.state.variance_time_lineage_fail_on_q90_pre_ack_fill
                    ):
                        raise RuntimeError(
                            "randomized lineage replay reached the first "
                            "historical BUY q90 fill-before-cancel-ACK branch; "
                            "C++ lifecycle lockstep authority is synthetic-only"
                        )
            order["queue_deplete_mult"] = float(deplete_mult)
            if not exchange_reserved:
                self._append_queue_event(
                    event="fill",
                    trade_idx=i,
                    order=order,
                    trade_price=p,
                    trade_qty=qty,
                    deplete_mult=deplete_mult,
                    trade_remaining_before=trade_before_order,
                    trade_remaining_after=trade_remaining,
                    queue_before=queue_before,
                    queue_after=float(order["queue_left"]),
                    fill_qty=fill_qty,
                )
            self._append_fill_trace(
                "BUY", i, order["price"], fill_qty,
                order["quote_ts"], order["mid_at_quote"], order["queue_init"],
                queue_before, rem_before, q_before_fill, order=order,
                economic_match_sequence=economic_match_sequence,
                clock_context={"path": "passive", "match_ts_ms": exchange_t,
                               "visible_ts_ms": t, "processed_ts_ms": t,
                               "exchange_reserved": bool(exchange_reserved)},
            )
            self._append_order_outcome(order, t, "fill", "fill", fill_qty)
            previous_fill_ts = int(self.state.last_buy_fill_ts)
            previous_cooldown_ms = float(self.state.last_buy_fill_cooldown_ms)
            self.state.consec_buy += fill_qty / max(self.state.order_size, self.state.LOT_SIZE)
            self.state.consec_sell = 0.0
            self.state.last_buy_fill_ts = t
            new_cooldown_ms = self._cooldown_ms_for_fill(
                "BUY",
                q_before_fill,
                self.state.consec_buy,
                int(t),
                previous_fill_ts_ms=previous_fill_ts,
                previous_cooldown_ms=previous_cooldown_ms,
                order=order,
                queue_ahead_before_fill_btc=float(queue_before),
            )
            self.state.last_buy_fill_cooldown_ms = self.services.cooldown_duration_after_same_side_fill(
                previous_fill_ts_ms=previous_fill_ts,
                previous_cooldown_ms=previous_cooldown_ms,
                current_fill_ts_ms=int(t),
                new_cooldown_ms=new_cooldown_ms,
            )
            self.state.last_buy_fill_cooldown_total_ms = self.state.last_buy_fill_cooldown_ms
            self._cooldown_rearm_on_fill('BUY', int(t), new_cooldown_ms)
            self._cooldown_lineage_on_fill(
                "BUY",
                prev_q=q_before_fill,
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
                fill_qty=float(fill_qty),
                fee_rate=float(self.state.maker_fee),
                consecutive_units=float(self.state.consec_buy),
                baseline_cooldown_ms=float(new_cooldown_ms),
            )
            self._variance_time_on_fill(
                "BUY",
                prev_q=q_before_fill,
                fill_ts_ms=int(t),
                consecutive_units=self.state.consec_buy,
                baseline_cooldown_ms=new_cooldown_ms,
            )
            response_add_fill = self.state.post_fill_quote_response.record_fill(
                side="BUY",
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                fill_qty=float(fill_qty),
                order_size=float(self.state.order_size),
                ts_ms=int(t),
            )
            if response_add_fill:
                self.state.post_fill_quote_response_add_fill_count += 1
                self.state.post_fill_quote_response_bid_add_count += 1
            self.state.last_sell_fill_cooldown_ms = 0.0
            self.state.last_sell_fill_cooldown_total_ms = 0.0
            if self.services.inventory_cooldown_blocks_action(
                    self._fill_cooldown_active("BUY", int(t)), self.context.inventory_cooldown_mode):
                # Live MakerEngine calls _cancel_cooldown_side_order(side)
                # after an exposure-increasing fill.  Replay must cancel any
                # residual/pending same-side orders too; otherwise old
                # same-side orders can keep filling during a period where
                # live would already be in fill_cd pause.
                self._request_cancel_all(self.state.bid_orders, int(t), reason="fill_cooldown")
            elif response_add_fill:
                # Reprice the old add-side order through the normal cancel
                # ACK/pending lifecycle. This removes the arbitrary hard
                # silence while retaining real queue-reset and REST costs.
                self._request_cancel_all(
                    self.state.bid_orders,
                    int(t),
                    reason="post_fill_quote_response",
                )
                self.state.post_fill_quote_response_force_bid_requote = self.state.q < self.state.max_inv - 1e-12
            if q_before_fill <= 0.0 and self.state.q > 0.0:
                self.state.entry_price = order["price"]
            elif q_before_fill > 0.0:
                self.state.entry_price = (self.state.entry_price * q_before_fill + order["price"] * fill_qty) / self.state.q
            if abs(self.state.q) < 1e-10:
                self.state.entry_price = 0.0
            self._record_local_action_fill(order, "BUY", int(t), float(fill_qty))
            self._record_local_order_value_fill(order, "BUY", int(t))
            self._record_queue_value_fill(
                order,
                "BUY",
                int(t),
                float(fill_qty),
            )
            self._record_state_conditioned_fill(
                order, "BUY", int(t), float(fill_qty)
            )
            self._record_safe_add_rearm_fill(order, "BUY", int(t), float(fill_qty))
            self._record_state_conditioned_rearm_fill(
                order, "BUY", int(t), float(fill_qty)
            )
            self._record_first_add_decision_fill(
                order,
                "BUY",
                inventory_before_fill=float(q_before_fill),
                fill_qty=float(fill_qty),
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
            )
            self._record_first_opener_decision_fill(
                order,
                "BUY",
                inventory_before_fill=float(q_before_fill),
                fill_qty=float(fill_qty),
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
            )
            self._inventory_lifecycle_on_fill(
                "BUY",
                q_before_fill,
                fill_qty,
                int(t),
                float(order["price"]),
                self.state.maker_fee,
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            self._sync_local_order_lifecycle_repair(int(t))
            if order["remaining"] < self.state.LOT_SIZE:
                self._dynamic_fill_hazard_order_terminal(
                    order,
                    event="fill",
                    now_ts=int(t),
                )
            if self.state.markout_ema_span_fills > 0:
                self._queue_markout(exchange_t, order["price"], True, {
                    "final_compressed": order.get("quote_final_compressed", False),
                }, fill_qty, int(order["trace_id"]))
        elif side == "SELL":
            q_before_fill = self.state.q
            self.state.cash += order["price"] * fill_qty * (1.0 - self.state.maker_fee)
            self.state.q -= fill_qty
            self._loss_cooldown_on_fill(
                "SELL",
                float(fill_qty),
                float(order["price"]),
                float(self.state.maker_fee),
            )
            order["remaining"] = self.services.subtract_lot_quantity(order["remaining"], fill_qty, self.state.LOT_SIZE)
            self._post_cooldown_budget_on_fill(
                order,
                side="SELL",
                fill_qty_btc=float(fill_qty),
                now_ts=int(t),
            )
            if not exchange_reserved:
                trade_remaining = self.services.subtract_lot_quantity(trade_remaining, fill_qty, self.state.LOT_SIZE)
            (
                physical_fill_identity,
                economic_fill_legs,
            ) = self._inventory_lifecycle_legs_for_order_fill(
                order,
                ts_ms=int(t),
                side="SELL",
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                fill_qty=float(fill_qty),
                fill_price=float(order["price"]),
                fee_rate=float(self.state.maker_fee),
                remaining_before=float(rem_before),
                remaining_after=float(order["remaining"]),
            )
            self._record_order_lifecycle_fill(
                order,
                int(t),
                exchange_ts_ms=int(exchange_t),
                fill_qty=float(fill_qty),
                remaining_before=float(rem_before),
                remaining_after=float(order["remaining"]),
                fill_price=float(order["price"]),
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                inventory_lifecycle_id=int(economic_fill_legs[0]["inventory_lifecycle_id"]),
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            ranked_guard_full_fill = float(order["remaining"]) < self.state.LOT_SIZE
            self._ranked_guard_order_fill(
                order,
                remaining_after=(
                    0.0
                    if ranked_guard_full_fill
                    else float(order["remaining"])
                ),
                visibility_ts_ms=int(t),
                exchange_ts_ms=int(exchange_t),
                full_fill=bool(ranked_guard_full_fill),
            )
            self.state.nfa += 1
            if order.get("circuit_breaker_close", False):
                self.state.circuit_breaker_close_fill_count += 1
            self.state.sell_fill_qty_total += fill_qty
            self.state.sell_fill_notional += order["price"] * fill_qty
            filled_sell = True
            if order.get("quote_final_compressed", False):
                self.state.fills_ask_final_compressed += 1
            else:
                self.state.fills_ask_not_final_compressed += 1
            if order.get("quote_mid_guard", False):
                self.state.fills_ask_mid_guard += 1
            if order.get("quote_post_only", False):
                self.state.fills_ask_post_only += 1
            if order.get("quote_ask_adverse", False):
                self.state.fills_ask_adverse_guard += 1
            if order.get("quote_ask_defense", False):
                self.state.fills_ask_defense_guard += 1
            if order.get("quote_local_extreme_guard", False):
                self.state.fills_ask_local_extreme_guard += 1
            if order["state"] == self.state.ORDER_PENDING_CANCEL:
                self.state.fills_while_pending_cancel += 1
            order["queue_deplete_mult"] = float(deplete_mult)
            if not exchange_reserved:
                self._append_queue_event(
                    event="fill",
                    trade_idx=i,
                    order=order,
                    trade_price=p,
                    trade_qty=qty,
                    deplete_mult=deplete_mult,
                    trade_remaining_before=trade_before_order,
                    trade_remaining_after=trade_remaining,
                    queue_before=queue_before,
                    queue_after=float(order["queue_left"]),
                    fill_qty=fill_qty,
                )
            self._append_fill_trace(
                "SELL", i, order["price"], fill_qty,
                order["quote_ts"], order["mid_at_quote"], order["queue_init"],
                queue_before, rem_before, q_before_fill, order=order,
                economic_match_sequence=economic_match_sequence,
                clock_context={"path": "passive", "match_ts_ms": exchange_t,
                               "visible_ts_ms": t, "processed_ts_ms": t,
                               "exchange_reserved": bool(exchange_reserved)},
            )
            self._append_order_outcome(order, t, "fill", "fill", fill_qty)
            previous_fill_ts = int(self.state.last_sell_fill_ts)
            previous_cooldown_ms = float(self.state.last_sell_fill_cooldown_ms)
            self.state.consec_sell += fill_qty / max(self.state.order_size, self.state.LOT_SIZE)
            self.state.consec_buy = 0.0
            self.state.last_sell_fill_ts = t
            new_cooldown_ms = self._cooldown_ms_for_fill(
                "SELL",
                q_before_fill,
                self.state.consec_sell,
                int(t),
                previous_fill_ts_ms=previous_fill_ts,
                previous_cooldown_ms=previous_cooldown_ms,
                order=order,
                queue_ahead_before_fill_btc=float(queue_before),
            )
            self.state.last_sell_fill_cooldown_ms = self.services.cooldown_duration_after_same_side_fill(
                previous_fill_ts_ms=previous_fill_ts,
                previous_cooldown_ms=previous_cooldown_ms,
                current_fill_ts_ms=int(t),
                new_cooldown_ms=new_cooldown_ms,
            )
            self.state.last_sell_fill_cooldown_total_ms = self.state.last_sell_fill_cooldown_ms
            self._cooldown_rearm_on_fill('SELL', int(t), new_cooldown_ms)
            self._cooldown_lineage_on_fill(
                "SELL",
                prev_q=q_before_fill,
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
                fill_qty=float(fill_qty),
                fee_rate=float(self.state.maker_fee),
                consecutive_units=float(self.state.consec_sell),
                baseline_cooldown_ms=float(new_cooldown_ms),
            )
            self._variance_time_on_fill(
                "SELL",
                prev_q=q_before_fill,
                fill_ts_ms=int(t),
                consecutive_units=self.state.consec_sell,
                baseline_cooldown_ms=new_cooldown_ms,
            )
            response_add_fill = self.state.post_fill_quote_response.record_fill(
                side="SELL",
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                fill_qty=float(fill_qty),
                order_size=float(self.state.order_size),
                ts_ms=int(t),
            )
            if response_add_fill:
                self.state.post_fill_quote_response_add_fill_count += 1
                self.state.post_fill_quote_response_ask_add_count += 1
            self.state.last_buy_fill_cooldown_ms = 0.0
            self.state.last_buy_fill_cooldown_total_ms = 0.0
            if self.services.inventory_cooldown_blocks_action(
                    self._fill_cooldown_active("SELL", int(t)), self.context.inventory_cooldown_mode):
                # Match live _cancel_cooldown_side_order("SELL") semantics.
                self._request_cancel_all(self.state.ask_orders, int(t), reason="fill_cooldown")
            elif response_add_fill:
                self._request_cancel_all(
                    self.state.ask_orders,
                    int(t),
                    reason="post_fill_quote_response",
                )
                self.state.post_fill_quote_response_force_ask_requote = self.state.q > -self.state.max_inv + 1e-12
            if q_before_fill >= 0.0 and self.state.q < 0.0:
                self.state.entry_price = order["price"]
            elif q_before_fill < 0.0:
                self.state.entry_price = (self.state.entry_price * (-q_before_fill) + order["price"] * fill_qty) / (-self.state.q)
            if abs(self.state.q) < 1e-10:
                self.state.entry_price = 0.0
            self._record_local_action_fill(order, "SELL", int(t), float(fill_qty))
            self._record_local_order_value_fill(order, "SELL", int(t))
            self._record_queue_value_fill(
                order,
                "SELL",
                int(t),
                float(fill_qty),
            )
            self._record_sell_add_skip_fill(order, int(t), float(fill_qty))
            self._record_state_conditioned_fill(
                order, "SELL", int(t), float(fill_qty)
            )
            self._record_safe_add_rearm_fill(order, "SELL", int(t), float(fill_qty))
            self._record_state_conditioned_rearm_fill(
                order, "SELL", int(t), float(fill_qty)
            )
            self._record_first_add_decision_fill(
                order,
                "SELL",
                inventory_before_fill=float(q_before_fill),
                fill_qty=float(fill_qty),
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
            )
            self._record_first_opener_decision_fill(
                order,
                "SELL",
                inventory_before_fill=float(q_before_fill),
                fill_qty=float(fill_qty),
                fill_ts_ms=int(t),
                fill_price=float(order["price"]),
            )
            self._inventory_lifecycle_on_fill(
                "SELL",
                q_before_fill,
                fill_qty,
                int(t),
                float(order["price"]),
                self.state.maker_fee,
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            self._sync_local_order_lifecycle_repair(int(t))
            if self.state.markout_ema_span_fills > 0:
                self._queue_markout(exchange_t, order["price"], False, {
                    "final_compressed": order.get("quote_final_compressed", False),
                }, fill_qty, int(order["trace_id"]))
        else:
            raise ValueError(f"unsupported passive fill side={side!r}")
        return float(trade_remaining), bool(filled_buy), bool(filled_sell)

    def _apply_visible_fill_inventory_state(self,
        *,
        q_before_visible: float,
        visibility_ts_ms: int,
        filled_buy: bool,
        filled_sell: bool,
    ) -> None:
        if not (filled_buy or filled_sell):
            return
        timestamp = int(visibility_ts_ms)
        if abs(self.state.q) < 1e-10:
            self.state.post_fill_quote_response.reset()
            self.state.post_fill_quote_response_force_bid_requote = False
            self.state.post_fill_quote_response_force_ask_requote = False
            self.state.pos_open_ts = timestamp
        elif (
            abs(float(q_before_visible)) < 1e-10
            or (float(q_before_visible) <= 0.0 < self.state.q)
            or (float(q_before_visible) >= 0.0 > self.state.q)
        ):
            self.state.pos_open_ts = timestamp
        if self.state.q >= self.state.max_inv:
            self._drop_orders(self.state.bid_orders, timestamp, "inventory_limit")
        elif self.state.q <= -self.state.max_inv:
            self._drop_orders(self.state.ask_orders, timestamp, "inventory_limit")

    def _reserve_or_publish_passive_fill(self,
        *,
        side: str,
        order: dict[str, Any],
        fill_qty: float,
        exchange_ts_ms: int,
        trade_idx: int,
        trade_price: float,
        trade_qty: float,
        deplete_mult: float,
        trade_remaining_before: float,
        trade_remaining: float,
        queue_before: float,
        remaining_before: float,
    ) -> tuple[float, bool, bool]:
        if not self.state.private_fill_visibility_enabled:
            return self._publish_passive_fill(
                side=side,
                order=order,
                fill_qty=fill_qty,
                exchange_ts_ms=exchange_ts_ms,
                trade_idx=trade_idx,
                trade_price=trade_price,
                trade_qty=trade_qty,
                deplete_mult=deplete_mult,
                trade_remaining_before=trade_remaining_before,
                trade_remaining=trade_remaining,
                queue_before=queue_before,
                remaining_before=remaining_before,
            )

        exchange_remaining_before = float(
            order.get("exchange_remaining", order.get("remaining", 0.0))
        )
        exchange_remaining_after = self.services.subtract_lot_quantity(
            exchange_remaining_before, float(fill_qty), self.state.LOT_SIZE
        )
        order["exchange_remaining"] = exchange_remaining_after
        order["last_exchange_fill_ts_ms"] = int(exchange_ts_ms)
        self.state.private_fill_exchange_match_count += 1
        economic_match_sequence = self._record_economic_match(
            side, order, exchange_ts_ms, fill_qty, order["price"], self.state.maker_fee,
        )
        if exchange_remaining_after < self.state.LOT_SIZE:
            order["exchange_fill_terminal"] = True
        if side == "BUY":
            self.state.exchange_inventory += float(fill_qty)
        else:
            self.state.exchange_inventory -= float(fill_qty)
        remaining_trade = self.services.subtract_lot_quantity(float(trade_remaining), float(fill_qty), self.state.LOT_SIZE)
        order["queue_deplete_mult"] = float(deplete_mult)
        self._append_queue_event(
            event="fill",
            trade_idx=int(trade_idx),
            order=order,
            trade_price=float(trade_price),
            trade_qty=float(trade_qty),
            deplete_mult=float(deplete_mult),
            trade_remaining_before=float(trade_remaining_before),
            trade_remaining_after=float(remaining_trade),
            queue_before=float(queue_before),
            queue_after=float(order["queue_left"]),
            fill_qty=float(fill_qty),
        )
        visible_ts_ms = self._sample_private_fill_visible_ts(
            exchange_ts=int(exchange_ts_ms),
            side=side,
            order_ts=int(order.get("submit_ts", 0) or 0),
        )
        visible_ts_ms = max(
            int(visible_ts_ms),
            int(order.get("_last_private_fill_visible_ts", exchange_ts_ms) or 0),
        )
        order["_last_private_fill_visible_ts"] = int(visible_ts_ms)
        payload = {
            "side": str(side),
            "economic_match_sequence": economic_match_sequence,
            "order": order,
            "fill_qty": float(fill_qty),
            "exchange_ts_ms": int(exchange_ts_ms),
            "trade_idx": int(trade_idx),
            "trade_price": float(trade_price),
            "trade_qty": float(trade_qty),
            "deplete_mult": float(deplete_mult),
            "trade_remaining_before": float(trade_remaining_before),
            "trade_remaining": float(remaining_trade),
            "queue_before": float(queue_before),
            "remaining_before": float(remaining_before),
        }
        if int(visible_ts_ms) == int(exchange_ts_ms):
            q_before_visible = float(self.state.q)
            _, filled_buy, filled_sell = self._publish_passive_fill(
                visibility_ts_ms=int(visible_ts_ms),
                exchange_reserved=True,
                **payload,
            )
            self._apply_visible_fill_inventory_state(
                q_before_visible=q_before_visible,
                visibility_ts_ms=int(visible_ts_ms),
                filled_buy=filled_buy,
                filled_sell=filled_sell,
            )
            return remaining_trade, filled_buy, filled_sell
        if self.state.local_lifecycle_boundary_scheduler is None:
            raise RuntimeError("private fill visibility scheduler is unavailable")
        trace_id = str(order.get("trace_id", ""))
        event_id = (
            f"private-fill:{trace_id}:{int(exchange_ts_ms)}:"
            f"{int(visible_ts_ms)}:{exchange_remaining_after:.12f}"
        )
        self.state.local_lifecycle_boundary_scheduler.schedule(
            ts_ms=int(visible_ts_ms),
            phase="private_fill_visible",
            event_id=event_id,
            payload=payload,
        )
        return remaining_trade, False, False

    def _match_event_orders(self):
        """Match IOC and resting orders before same-time terminal acknowledgements."""
        self.state.filled_buy = False
        self.state.filled_sell = False

        self.state.q_before_trade = self.state.q
        self.state.ioc_filled_buy = self._process_circuit_breaker_ioc_orders(
            self.state.bid_orders,
            "BUY",
            int(self.state.t),
            int(self.state.i),
            float(self.state.p),
        )
        self.state.ioc_filled_sell = self._process_circuit_breaker_ioc_orders(
            self.state.ask_orders,
            "SELL",
            int(self.state.t),
            int(self.state.i),
            float(self.state.p),
        )
        self.state.filled_buy = self.state.filled_buy or self.state.ioc_filled_buy
        self.state.filled_sell = self.state.filled_sell or self.state.ioc_filled_sell
        if (self.state.ioc_filled_buy or self.state.ioc_filled_sell) and abs(self.state.q) < self.state.LOT_SIZE:
            self._drop_orders(self.state.bid_orders, int(self.state.t), "circuit_breaker_ioc_flat")
            self._drop_orders(self.state.ask_orders, int(self.state.t), "circuit_breaker_ioc_flat")

        if self.state.has_executable_trade and self.state.is_sell:
            self._record_same_price_trade_for_l2_queue(self.state.bid_orders, float(self.state.p), float(self.state.qty))
            self._record_exchange_book_same_price_trade(
                self.state.bid_orders,
                float(self.state.p),
                float(self.state.qty),
                int(self.state.t),
            )
            self.state.deplete_mult = self._queue_deplete_multiplier("BUY", self.state.t)
            self._deplete_policy_visible_queue(
                self.state.bid_orders,
                "BUY",
                float(self.state.p),
                float(self.state.qty),
                float(self.state.deplete_mult),
            )
            self.state.trade_remaining = self.state.qty * self.state.deplete_mult
            for loop_item in self.state.bid_orders:
                self.state.order = loop_item
                if (
                    not self.services._trade_tick_crosses_order_tick(
                        "BUY", self.state.trade_price_tick, self.services._order_price_tick(self.state.order, self.state.TICK)
                    )
                    or not self._should_trace_queue_event(self.state.order, int(self.state.t))
                ):
                    continue
                self.state.skip_reason = ""
                if self.state.order["state"] not in (
                    self.state.ORDER_PENDING_NEW,
                    self.state.ORDER_OPEN,
                    self.state.ORDER_PENDING_CANCEL,
                ):
                    self.state.skip_reason = f"state_{self.state.order['state']}"
                elif (
                    self.state.order["state"] == self.state.ORDER_PENDING_NEW
                    and not bool(self.state.order.get("exchange_accepted", False))
                ):
                    self.state.skip_reason = "pending_exchange_accept"
                elif not self._order_exchange_fillable(self.state.order, int(self.state.t)):
                    self.state.skip_reason = "cancel_effective"
                elif not self.state.order.get("fill_eligible", True):
                    self.state.skip_reason = "fill_ineligible"
                elif self.state.order.get("remaining", 0.0) < self.state.LOT_SIZE:
                    self.state.skip_reason = "remaining_below_lot"
                self._append_queue_event(
                    event="candidate",
                    trade_idx=self.state.i,
                    order=self.state.order,
                    trade_price=self.state.p,
                    trade_qty=self.state.qty,
                    deplete_mult=self.state.deplete_mult,
                    trade_remaining_before=self.state.trade_remaining,
                    trade_remaining_after=self.state.trade_remaining,
                    queue_before=float(self.state.order.get("queue_left", 0.0)),
                    queue_after=float(self.state.order.get("queue_left", 0.0)),
                    skip_reason=self.state.skip_reason,
                )
            self.state.live_bid_orders = sorted(
                [o for o in self.state.bid_orders
                 if o["state"] in (self.state.ORDER_PENDING_NEW, self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
                 and (
                     o["state"] != self.state.ORDER_PENDING_NEW
                     or bool(o.get("exchange_accepted", False))
                 )
                 and self._order_exchange_fillable(o, int(self.state.t))
                 and o["fill_eligible"]
                 and float(
                     o.get("exchange_remaining", o["remaining"])
                     if self.state.private_fill_visibility_enabled
                     else o["remaining"]
                 ) >= self.state.LOT_SIZE
                 and self.services._trade_tick_crosses_order_tick(
                     "BUY", self.state.trade_price_tick, self.services._order_price_tick(o, self.state.TICK)
                 )],
                key=lambda o: self.services._order_price_tick(o, self.state.TICK),
                reverse=True,
            )
            for loop_item in self.state.live_bid_orders:
                self.state.order = loop_item
                if self.state.trade_remaining < self.state.LOT_SIZE:
                    break
                self.state.trade_before_order = self.state.trade_remaining
                self.state.queue_before = self.state.order["queue_left"]
                self.state.rem_before = self.state.order["remaining"]
                if self.state.order["queue_left"] > 0.0:
                    self.state.eat_ahead = min(self.state.order["queue_left"], self.state.trade_remaining)
                    self.state.order["queue_left"] = self.services.subtract_lot_quantity(
                        self.state.order["queue_left"], self.state.eat_ahead, self.state.LOT_SIZE
                    )
                    self.state.trade_remaining = self.services.subtract_lot_quantity(self.state.trade_remaining, self.state.eat_ahead, self.state.LOT_SIZE)
                    self._append_queue_event(
                        event="deplete_queue",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                    )
                if self.state.trade_remaining < self.state.LOT_SIZE or self.state.order["remaining"] < self.state.LOT_SIZE:
                    self._append_queue_event(
                        event="skip_after_deplete",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                        skip_reason="trade_remaining_below_lot" if self.state.trade_remaining < self.state.LOT_SIZE else "remaining_below_lot",
                    )
                    continue
                self.state.match_remaining = float(
                    self.state.order.get("exchange_remaining", self.state.order["remaining"])
                    if self.state.private_fill_visibility_enabled
                    else self.state.order["remaining"]
                )
                self.state.fill_qty = min(self.state.match_remaining, self.state.trade_remaining)
                if self.state.order.get("reduce_only", False):
                    self.state.fill_qty = min(
                        self.state.fill_qty,
                        max(
                            0.0,
                            -self.state.exchange_inventory
                            if self.state.private_fill_visibility_enabled
                            else -self.state.q,
                        ),
                    )
                self.state.fill_qty = self.services.math.floor(self.state.fill_qty / self.state.LOT_SIZE) * self.state.LOT_SIZE
                if self.state.fill_qty < self.state.LOT_SIZE:
                    self._append_queue_event(
                        event="skip_fill_rounding",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                        skip_reason="fill_qty_below_lot",
                    )
                    continue

                if (
                    not self.state.private_fill_visibility_enabled
                    and not bool(self.state.order.get("local_new_ack_published", True))
                ):
                    raise RuntimeError(
                        "pre-ACK exchange fill requires private-fill visibility scheduling"
                    )

                self.state.trade_remaining, self.state.filled_buy_now, self.state.filled_sell_now = (
                    self._reserve_or_publish_passive_fill(
                        side="BUY",
                        order=self.state.order,
                        fill_qty=float(self.state.fill_qty),
                        exchange_ts_ms=int(self.state.t),
                        trade_idx=int(self.state.i),
                        trade_price=float(self.state.p),
                        trade_qty=float(self.state.qty),
                        deplete_mult=float(self.state.deplete_mult),
                        trade_remaining_before=float(self.state.trade_before_order),
                        trade_remaining=float(self.state.trade_remaining),
                        queue_before=float(self.state.queue_before),
                        remaining_before=float(self.state.rem_before),
                    )
                )
                self.state.filled_buy = self.state.filled_buy or self.state.filled_buy_now
                self.state.filled_sell = self.state.filled_sell or self.state.filled_sell_now

            self.state.bid_orders = [o for o in self.state.bid_orders if o["remaining"] >= self.state.LOT_SIZE]

        elif self.state.has_executable_trade:
            self._record_same_price_trade_for_l2_queue(self.state.ask_orders, float(self.state.p), float(self.state.qty))
            self._record_exchange_book_same_price_trade(
                self.state.ask_orders,
                float(self.state.p),
                float(self.state.qty),
                int(self.state.t),
            )
            self.state.deplete_mult = self._queue_deplete_multiplier("SELL", self.state.t)
            self._deplete_policy_visible_queue(
                self.state.ask_orders,
                "SELL",
                float(self.state.p),
                float(self.state.qty),
                float(self.state.deplete_mult),
            )
            self.state.trade_remaining = self.state.qty * self.state.deplete_mult
            for loop_item in self.state.ask_orders:
                self.state.order = loop_item
                if (
                    not self.services._trade_tick_crosses_order_tick(
                        "SELL", self.state.trade_price_tick, self.services._order_price_tick(self.state.order, self.state.TICK)
                    )
                    or not self._should_trace_queue_event(self.state.order, int(self.state.t))
                ):
                    continue
                self.state.skip_reason = ""
                if self.state.order["state"] not in (
                    self.state.ORDER_PENDING_NEW,
                    self.state.ORDER_OPEN,
                    self.state.ORDER_PENDING_CANCEL,
                ):
                    self.state.skip_reason = f"state_{self.state.order['state']}"
                elif (
                    self.state.order["state"] == self.state.ORDER_PENDING_NEW
                    and not bool(self.state.order.get("exchange_accepted", False))
                ):
                    self.state.skip_reason = "pending_exchange_accept"
                elif not self._order_exchange_fillable(self.state.order, int(self.state.t)):
                    self.state.skip_reason = "cancel_effective"
                elif not self.state.order.get("fill_eligible", True):
                    self.state.skip_reason = "fill_ineligible"
                elif self.state.order.get("remaining", 0.0) < self.state.LOT_SIZE:
                    self.state.skip_reason = "remaining_below_lot"
                self._append_queue_event(
                    event="candidate",
                    trade_idx=self.state.i,
                    order=self.state.order,
                    trade_price=self.state.p,
                    trade_qty=self.state.qty,
                    deplete_mult=self.state.deplete_mult,
                    trade_remaining_before=self.state.trade_remaining,
                    trade_remaining_after=self.state.trade_remaining,
                    queue_before=float(self.state.order.get("queue_left", 0.0)),
                    queue_after=float(self.state.order.get("queue_left", 0.0)),
                    skip_reason=self.state.skip_reason,
                )
            self.state.live_ask_orders = sorted(
                [o for o in self.state.ask_orders
                 if o["state"] in (self.state.ORDER_PENDING_NEW, self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
                 and (
                     o["state"] != self.state.ORDER_PENDING_NEW
                     or bool(o.get("exchange_accepted", False))
                 )
                 and self._order_exchange_fillable(o, int(self.state.t))
                 and o["fill_eligible"]
                 and float(
                     o.get("exchange_remaining", o["remaining"])
                     if self.state.private_fill_visibility_enabled
                     else o["remaining"]
                 ) >= self.state.LOT_SIZE
                 and self.services._trade_tick_crosses_order_tick(
                     "SELL", self.state.trade_price_tick, self.services._order_price_tick(o, self.state.TICK)
                 )],
                key=lambda o: self.services._order_price_tick(o, self.state.TICK),
            )
            for loop_item in self.state.live_ask_orders:
                self.state.order = loop_item
                if self.state.trade_remaining < self.state.LOT_SIZE:
                    break
                self.state.trade_before_order = self.state.trade_remaining
                self.state.queue_before = self.state.order["queue_left"]
                self.state.rem_before = self.state.order["remaining"]
                if self.state.order["queue_left"] > 0.0:
                    self.state.eat_ahead = min(self.state.order["queue_left"], self.state.trade_remaining)
                    self.state.order["queue_left"] = self.services.subtract_lot_quantity(
                        self.state.order["queue_left"], self.state.eat_ahead, self.state.LOT_SIZE
                    )
                    self.state.trade_remaining = self.services.subtract_lot_quantity(self.state.trade_remaining, self.state.eat_ahead, self.state.LOT_SIZE)
                    self._append_queue_event(
                        event="deplete_queue",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                    )
                if self.state.trade_remaining < self.state.LOT_SIZE or self.state.order["remaining"] < self.state.LOT_SIZE:
                    self._append_queue_event(
                        event="skip_after_deplete",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                        skip_reason="trade_remaining_below_lot" if self.state.trade_remaining < self.state.LOT_SIZE else "remaining_below_lot",
                    )
                    continue
                self.state.match_remaining = float(
                    self.state.order.get("exchange_remaining", self.state.order["remaining"])
                    if self.state.private_fill_visibility_enabled
                    else self.state.order["remaining"]
                )
                self.state.fill_qty = min(self.state.match_remaining, self.state.trade_remaining)
                if self.state.order.get("reduce_only", False):
                    self.state.fill_qty = min(
                        self.state.fill_qty,
                        max(
                            0.0,
                            self.state.exchange_inventory
                            if self.state.private_fill_visibility_enabled
                            else self.state.q,
                        ),
                    )
                self.state.fill_qty = self.services.math.floor(self.state.fill_qty / self.state.LOT_SIZE) * self.state.LOT_SIZE
                if self.state.fill_qty < self.state.LOT_SIZE:
                    self._append_queue_event(
                        event="skip_fill_rounding",
                        trade_idx=self.state.i,
                        order=self.state.order,
                        trade_price=self.state.p,
                        trade_qty=self.state.qty,
                        deplete_mult=self.state.deplete_mult,
                        trade_remaining_before=self.state.trade_before_order,
                        trade_remaining_after=self.state.trade_remaining,
                        queue_before=self.state.queue_before,
                        queue_after=float(self.state.order["queue_left"]),
                        skip_reason="fill_qty_below_lot",
                    )
                    continue

                if (
                    not self.state.private_fill_visibility_enabled
                    and not bool(self.state.order.get("local_new_ack_published", True))
                ):
                    raise RuntimeError(
                        "pre-ACK exchange fill requires private-fill visibility scheduling"
                    )

                self.state.trade_remaining, self.state.filled_buy_now, self.state.filled_sell_now = (
                    self._reserve_or_publish_passive_fill(
                        side="SELL",
                        order=self.state.order,
                        fill_qty=float(self.state.fill_qty),
                        exchange_ts_ms=int(self.state.t),
                        trade_idx=int(self.state.i),
                        trade_price=float(self.state.p),
                        trade_qty=float(self.state.qty),
                        deplete_mult=float(self.state.deplete_mult),
                        trade_remaining_before=float(self.state.trade_before_order),
                        trade_remaining=float(self.state.trade_remaining),
                        queue_before=float(self.state.queue_before),
                        remaining_before=float(self.state.rem_before),
                    )
                )
                self.state.filled_buy = self.state.filled_buy or self.state.filled_buy_now
                self.state.filled_sell = self.state.filled_sell or self.state.filled_sell_now

            self.state.ask_orders = [o for o in self.state.ask_orders if o["remaining"] >= self.state.LOT_SIZE]

        if self.state.exchange_book_scheduler is not None and self.state.has_executable_trade:
            # Native exchange-time trades and REST cancel ACKs do not share a
            # sequence key. At an equal millisecond, let the crossing trade
            # consume the resting order first, then apply the ACK. The path is
            # marked ambiguous above and excluded from formal action evidence.
            self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
            self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)

        self._update_local_order_value_price_jump(int(self.state.t), float(self.state.mid))
        self._update_queue_value_price_jump(int(self.state.t), float(self.state.mid))
        self._cooldown_duration_fork_path_update(int(self.state.t), float(self.state.mid))

    def _publish_event_inventory(self):
        """Publish private-fill inventory and advance the visible hazard boundary."""
        self._apply_visible_fill_inventory_state(
            q_before_visible=float(self.state.q_before_trade),
            visibility_ts_ms=int(self.state.t),
            filled_buy=bool(self.state.filled_buy),
            filled_sell=bool(self.state.filled_sell),
        )

        if self.state.circuit_breaker_closing and abs(self.state.q) < self.state.LOT_SIZE:
            self.state.circuit_breaker_closing = False
            self.state.circuit_breaker_close_start_ts = -1
            self.state.circuit_breaker_close_gtx_reject_streak = 0
            self._drop_orders(self.state.bid_orders, int(self.state.t), "circuit_breaker_flat")
            self._drop_orders(self.state.ask_orders, int(self.state.t), "circuit_breaker_flat")
            self.state.post_fill_quote_response.reset()
            self.state.post_fill_quote_response_force_bid_requote = False
            self.state.post_fill_quote_response_force_ask_requote = False
            self.state.pos_open_ts = int(self.state.t)
            self.state.entry_price = 0.0


        if (
            self.state.dynamic_fill_hazard_visibility_scheduler is None
            or self.state.dynamic_fill_hazard_last_event_at_timestamp
        ):
            self._advance_dynamic_fill_hazard_visibility(
                int(self.state.t) * 1_000_000,
                inclusive=True,
            )
            self._evaluate_dynamic_fill_hazard_action(int(self.state.t))
