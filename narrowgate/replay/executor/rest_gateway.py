"""Replay rest gateway owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional

from strategy.replace_threshold import (
    cancel_first_replace,
    replace_throttle_blocks,
    replace_throttle_parameters,
)


class RestGatewayAdapter:
    def _replace_throttle_params(self, side: str, inv_qty: float) -> tuple[float, float, bool]:
        return replace_throttle_parameters(
            side=side, inventory=inv_qty,
            ordinary_ticks=self.state.replace_min_price_change_ticks,
            ordinary_interval_ms=self.state.replace_min_interval_ms,
            reducing_ticks=self.state.replace_min_price_change_ticks_reducing,
            reducing_interval_ms=self.state.replace_min_interval_ms_reducing,
        )

    def _apply_replace_throttle(self,
        side: str,
        now_ts: int,
        inv_qty: float,
        target_price: float,
        order,
        needs_update: bool,
        force_update: bool = False,
    ) -> bool:
        """Replay/live parity: small or too-frequent active-order replaces become keep."""
        self.state.directional_replace_pending.pop(side, None)
        if self.state.quote_schedule is not None:
            self.state.quote_schedule.deadlines.pop(f"replace_age:{side}", None)
        if self.state.replace_price_threshold.mode != "fixed":
            funnel = self.state.directional_replace_counts.setdefault(f"{side}:entry", {})
            for key, present in (("calls", True), ("needs_update", needs_update),
                                 ("force_update", force_update),
                                 ("no_order", order is None),
                                 ("pending_cancel", order is not None and order.get("state") == self.state.ORDER_PENDING_CANCEL)):
                funnel[key] = funnel.get(key, 0) + int(present)
        if not needs_update or force_update:
            return needs_update
        if order is None or order.get("state") not in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL):
            return needs_update
        order_price = float(order.get("price", 0.0) or 0.0)
        if order_price <= 0.0:
            return needs_update
        min_ticks, min_interval_ms, ordinary = self._replace_throttle_params(side, inv_qty)
        min_ticks = self.state.replace_price_threshold.select(
            ordinary=ordinary, side=side, old_price=order_price,
            target_price=float(target_price), fixed=min_ticks,
            tick_size=self.state.TICK, observation=self.state.target_observation)
        price_delta_ticks = abs(
            self.services._price_to_tick(float(target_price), self.state.TICK)
            - self.services._order_price_tick(order, self.state.TICK)
        )
        create_ts = int(order.get("submit_ts", order.get("activate_ts", now_ts)) or now_ts)
        age_ms = max(0.0, float(now_ts - create_ts))
        throttle_by_price, throttle_by_age = replace_throttle_blocks(
            price_delta_ticks=price_delta_ticks, age_ms=age_ms,
            min_ticks=min_ticks, min_interval_ms=min_interval_ms,
        )
        if self.context.response_update_fork is not None and self.context.response_update_fork.matches(
                now_ms=int(now_ts), sequence=int(self.state.nrq), side=side,
                order_id=order.get('trace_id', '')):
            if self.state.response_update_branch_evidence is not None:
                raise ValueError('optional-update decision consumed twice')
            admission = self.state.response_update_admission.get(side)
            if admission is None or admission['call_sequence'] != int(self.state.nrq):
                raise ValueError('optional-update decision has no current admission')
            baseline = not (throttle_by_price or throttle_by_age)
            selected = self.context.response_update_fork.select(
                old_price=order_price, target_price=float(target_price),
                quantity=float(order['remaining']), baseline=baseline,
                reasons=admission['reasons'])
            self.state.response_update_branch_evidence = dict(
                **admission, action=self.context.response_update_fork.action,
                baseline_update=baseline, selected_update=selected,
                decision_ns=int(now_ts)*1_000_000, side=side,
                order_id=str(order['trace_id']), old_price=order_price,
                target_price=float(target_price), future_effective_ns=None)
            # Only the price choice changes. Time and all downstream lifecycle
            # checks remain in the original consumer and request path.
            throttle_by_price = not selected
        if (self.state.response_action_runtime is not None
                and (self.state.bid_route_due if side == 'BUY' else self.state.ask_route_due)):
            token = self.state.response_update_observer.token(side, int(self.state.nrq))
            selected = self.state.response_action_runtime.select(
                token['row'] if token else None, sequence=int(self.state.nrq), side=side,
                order_id=order.get('trace_id', ''), old_price=order_price,
                target_price=float(target_price), quantity=float(order['remaining']),
                baseline=not (throttle_by_price or throttle_by_age))
            # Unsupported/admission-blocked decisions retain the *price* gate,
            # not the combined price-or-age result. Otherwise an age-only block
            # would become a price block and suppress its original wake deadline.
            if token['row']['response_model_intent']['value'] is not None:
                throttle_by_price = not selected
        if self.state.response_update_observer is not None:
            self.state.response_update_observer.checked(
                self.state.response_update_observer.token(side, int(self.state.nrq)),
                'replace_throttle', price_blocked=bool(throttle_by_price),
                age_blocked=bool(throttle_by_age), min_ticks=float(min_ticks),
                min_interval_ms=float(min_interval_ms), age_ms=age_ms,
                price_delta_ticks=price_delta_ticks)
        if self.state.quote_schedule is not None:
            ledger = self.state.quote_schedule
            key = f"replace_age:{side}"
            queue_keep = (self.state.bid_queue_value_force_keep if side == "BUY"
                          else self.state.ask_queue_value_force_keep)
            if (throttle_by_age and not throttle_by_price and not queue_keep
                    and order.get("state") == self.state.ORDER_OPEN):
                ledger.deadline(key, int(self.services.math.ceil(create_ts + min_interval_ms)) * 1_000_000,
                    side, f"{order['trace_id']}:{int(ordinary)}")
            ledger.count("price_blocked", int(throttle_by_price))
            ledger.count("age_blocked", int(throttle_by_age))
        if self.state.replace_price_threshold.mode != "fixed":
            signed_move = (1 if side == "BUY" else -1) * (float(target_price) - order_price) / self.state.TICK
            direction = "inward" if signed_move > 0 else "outward" if signed_move < 0 else "unchanged"
            group = f"{side}:{'ordinary' if ordinary else 'reducing'}:{direction}"
            counts = self.state.directional_replace_counts.setdefault(group, {})
            if ordinary and direction == "outward" and self.state.replace_price_threshold.mode in ("dynamic_outward", "static_outward"):
                ready = self.state.target_observation.sides[side].ready
                key = "estimator_ready" if ready else "estimator_fallback"
                counts[key] = counts.get(key, 0) + 1
                counts["threshold_sum"] = counts.get("threshold_sum", 0) + min_ticks
                counts["threshold_min"] = min(counts.get("threshold_min", min_ticks), min_ticks)
                counts["threshold_max"] = max(counts.get("threshold_max", min_ticks), min_ticks)
            band = 10 <= price_delta_ticks < 15
            for key, present in (("eligible", True), ("price_blocked", throttle_by_price),
                                 ("age_blocked", throttle_by_age), ("band_10_15", band),
                                 ("band_price_only_blocked", band and throttle_by_price and not throttle_by_age),
                                 ("band_passed", band and not throttle_by_price and not throttle_by_age),
                                 ("passed", not (throttle_by_price or throttle_by_age))):
                counts[key] = counts.get(key, 0) + int(present)
            self.state.directional_replace_pending[side] = group
            if (band or self.state.replace_price_threshold.mode in ("dynamic_outward", "static_outward")) and len(self.state.directional_replace_samples) < 128:
                self.state.directional_replace_samples.append(dict(
                    ts=now_ts, side=side, inventory=inv_qty, ordinary=ordinary,
                    old_price=order_price, target_price=float(target_price),
                    signed_move_ticks=signed_move, threshold=min_ticks,
                    needs_update=needs_update, force_update=force_update,
                    pending=order.get("state") == self.state.ORDER_PENDING_CANCEL,
                    price_blocked=throttle_by_price, age_blocked=throttle_by_age))
        if not (throttle_by_price or throttle_by_age):
            return needs_update
        self.state.replace_throttle_count += 1
        if side == "BUY":
            self.state.replace_throttle_bid_count += 1
        else:
            self.state.replace_throttle_ask_count += 1
        self.state.replace_throttle_price_count += int(throttle_by_price)
        self.state.replace_throttle_age_count += int(throttle_by_age)
        return False

    def _should_cancel_first_replace(self,
        side: str,
        inv_qty: float,
        order,
        needs_update: bool,
        can_post: bool,
    ) -> bool:
        _, _, exposure_increasing = self._replace_throttle_params(side, inv_qty)
        return cancel_first_replace(
            enabled=self.state.replace_cancel_first_exposure_increasing,
            needs_update=needs_update, can_post=can_post,
            active=order is not None and order.get("state") in (
                self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL,
            ),
            exposure_increasing=exposure_increasing,
        )

    def _count_decision_action(self,
        action: str,
        *,
        side: str,
        exposure_increasing: bool,
    ) -> None:
        role = "exposure" if exposure_increasing else "reducing"
        group = self.state.directional_replace_pending.pop(side, None)
        if group is not None:
            counts = self.state.directional_replace_counts[group]
            key = "decision_" + action
            counts[key] = counts.get(key, 0) + 1
        diagnostic_key = f"decision_{side.lower()}_{role}_{action}_count"
        self.state.decision_side_role_action_counts[diagnostic_key] = (
            self.state.decision_side_role_action_counts.get(diagnostic_key, 0) + 1
        )
        if action == "place":
            self.state.decision_place_count += 1
        elif action == "replace":
            self.state.decision_replace_count += 1
        elif action == "keep":
            self.state.decision_keep_count += 1
        elif action == "pause":
            self.state.decision_pause_count += 1
        elif action == "skip_filter":
            self.state.decision_skip_filter_count += 1
        elif action == "pending_coalesce":
            self.state.decision_pending_coalesce_count += 1
        elif action == "cancel_first":
            self.state.decision_cancel_first_count += 1
        else:
            self.state.decision_none_count += 1

    def _request_bulk_cancel(self, now_ts: int, reason: str) -> None:
        """Drain accepted FIFO writes before one symbol-level safety request.

        The strategy waits for HTTP completion, while market/private events
        keep advancing. Bulk HTTP success never supplies per-order terminals.
        """
        # Existing callers visit BUY and SELL separately; one live cancel-all
        # owns both sides, so the second visit must not create another request.
        if self.state.async_bulk_request is not None:
            return
        current = [order for order in (*self.state.bid_orders, *self.state.ask_orders)
                   if float(order.get("remaining", 0.0)) >= self.state.LOT_SIZE]
        for order in current:
            order["replace_terminal_continuation_armed"] = False
        if not any(order["state"] != self.state.ORDER_PENDING_CANCEL for order in current):
            return
        if self.state.bulk_cancel_samples is None:
            raise NotImplementedError(
                "sampled_async_fifo bulk safety cancel requires a cancel-all barrier "
                "and its own HTTP/visibility samples"
            )
        if self.state.main_loop_waiting_request is not None:
            raise NotImplementedError(
                "bulk safety interruption of an active synchronous close is not modeled"
            )
        effective_ms, private_ms, service_ms = (
            self._sample_latency_ms(
                0, self.state.bulk_cancel_samples[:, column], event_ts=int(now_ts),
                side="BUY", operation=self.services._LATENCY_CANCEL, order_ts=0,
            ) for column in (0, 1, 2)
        )
        self.state.async_rest_sequence += 1
        request = {
            "origin_ts_ms": int(now_ts), "is_bulk": True, "is_new": False,
            "effective_ms": effective_ms, "ack_ms": private_ms,
            "service_ms": service_ms, "sequence": self.state.async_rest_sequence,
            "orders": current, "reason": str(reason),
        }
        # This is an admission barrier, not another bounded lane submission.
        # It follows every accepted write without revoking queued NEWs.
        self.state.async_bulk_request = request
        self.state.async_rest_queue.append(request)
        self.state.main_loop_waiting_request = request
        self.state.main_loop_next_wake_ms = None
        self.state.main_loop_synchronous_request_count += 1
        self.state.bulk_cancel_request_count += 1
        for order in current:
            if order["state"] == self.state.ORDER_OPEN:
                order["state"] = self.state.ORDER_PENDING_CANCEL
            if int(order.get("cancel_ts", 0) or 0) <= 0:
                order["cancel_ts"] = self.state.async_rest_unset_ms
                order["cancel_effective_ts"] = self.state.async_rest_unset_ms
                order["cancel_reason"] = str(reason)
        self.state.local_lifecycle_boundary_scheduler.schedule(
            ts_ms=max(int(now_ts), self.state.rest_gateway_busy_until_ms), phase="rest_return",
            event_id=f"async-bulk:{request['sequence']}",
            payload={"async_rest_worker": True},
        )

    def _request_cancel_all(self,
        orders,
        now_ts: int,
        reason: str = "requote_replace",
        *,
        target_order_id: str = "",
        guard_initiated_order_id: str = "",
        request_key_ts: Optional[int] = None,
        response_observation_token=None,
    ):
        if self.state.async_rest_gateway and str(reason) in {
            "planned_maintenance", "stale_book", "message_source_not_ready",
            "sync_adjust_degrade", "position_timeout", "circuit_breaker",
            "daily_loss", "position_value", "emergency_drawdown",
            "consecutive_loss_cooldown",
        }:
            self._request_bulk_cancel(int(now_ts), str(reason))
            return
        normalized_target = str(target_order_id).strip()
        normalized_guard_target = str(guard_initiated_order_id).strip()
        replacement_reason = str(reason) in {
            "requote_replace",
            "cancel_first_replace",
        }
        for order in orders:
            if float(order.get("remaining", 0.0) or 0.0) < self.state.LOT_SIZE:
                continue
            order_id = self._ranked_guard_order_id(order)
            if normalized_target and order_id != normalized_target:
                continue
            if self.state.replace_terminal_continuation and not replacement_reason:
                order["replace_terminal_continuation_armed"] = False
            if order["state"] == self.state.ORDER_PENDING_NEW and (
                self.state.serial_rest_return_enabled or reason in {
                    "requote_replace", "side_disabled", "cancel_first_replace",
                    "ranked_toxicity_guard",
                }
            ):
                # Live cannot cancel an order whose submit ACK is unresolved.
                # The next control boundary revisits it; no automatic cancel
                # is invented at a future NEW ACK.
                continue
            if order["cancel_ts"] > 0:
                if not order.get("cancel_reason"):
                    order["cancel_reason"] = reason
                continue
            lineage_record = self._cooldown_lineage_record(
                str(order.get("side", "")).upper()
            )
            if lineage_record is not None:
                lineage_record["queue_reset_count"] = int(
                    lineage_record.get("queue_reset_count", 0)
                ) + 1
                reason_counts = lineage_record.setdefault(
                    "_queue_reset_reason_counts", {}
                )
                reason_counts[str(reason)] = int(
                    reason_counts.get(str(reason), 0)
                ) + 1
            price_penalty_record = self.state.sell_add_price_penalty_open_record
            if (
                price_penalty_record is not None
                and int(price_penalty_record.get("inventory_lifecycle_id", -1))
                == int(self.state.inventory_lifecycle_count)
            ):
                price_penalty_record["cancel_request_count"] = int(
                    price_penalty_record.get("cancel_request_count", 0)
                ) + 1
                price_penalty_record["queue_reset_count"] = int(
                    price_penalty_record.get("queue_reset_count", 0)
                ) + 1
                if str(reason) in {"requote_replace", "cancel_first_replace"}:
                    price_penalty_record["replace_cancel_request_count"] = int(
                        price_penalty_record.get(
                            "replace_cancel_request_count", 0
                        )
                    ) + 1
            multi_short_record = self._multi_short_repair_inventory_lifecycle_record()
            if multi_short_record is not None:
                multi_short_record["cancel_request_count"] = int(
                    multi_short_record.get("cancel_request_count", 0)
                ) + 1
                multi_short_record["queue_reset_count"] = int(
                    multi_short_record.get("queue_reset_count", 0)
                ) + 1
                if str(reason) in {"requote_replace", "cancel_first_replace"}:
                    multi_short_record["replace_cancel_request_count"] = int(
                        multi_short_record.get("replace_cancel_request_count", 0)
                    ) + 1
            fair_center_record = self.state.fair_center_open_record
            if fair_center_record is not None:
                fair_center_record["cancel_request_count"] = int(
                    fair_center_record.get("cancel_request_count", 0)
                ) + 1
                fair_center_record["queue_reset_count"] = int(
                    fair_center_record.get("queue_reset_count", 0)
                ) + 1
                if str(reason) in {"requote_replace", "cancel_first_replace"}:
                    fair_center_record["replace_cancel_request_count"] = int(
                        fair_center_record.get(
                            "replace_cancel_request_count", 0
                        )
                    ) + 1
            request_start_ts, effective_ts, ack_ts = self._sample_cancel_deadlines_ms(
                request_ts=int(now_ts if request_key_ts is None else request_key_ts),
                side=str(order.get("side", "BUY")),
                operation=self.services._LATENCY_CANCEL,
                order_ts=int(order.get("quote_ts", order.get("submit_ts", 0)) or 0),
                apply_decision_compute=str(reason) in {
                    "requote_replace", "side_disabled", "cancel_first_replace",
                    "ranked_toxicity_guard",
                },
            )
            order["cancel_effective_ts"] = int(effective_ts)
            order["cancel_ts"] = int(ack_ts)
            order["cancel_request_ts"] = int(request_start_ts)
            if self.state.response_update_observer is not None:
                self.state.response_update_observer.bind_request(
                    response_observation_token, 'cancel', order['trace_id'], request_start_ts)
            if self.state.serial_rest_return_enabled:
                order["cancel_rest_return_ts"] = int(
                    self.state.rest_gateway_last_timing["rest_return_ts_ms"]
                )
                if self.state.async_rest_gateway:
                    self.state.rest_gateway_last_timing["order"] = order
                    if str(reason) in {
                        "circuit_breaker_close_requote", "circuit_breaker_opening_side",
                    }:
                        if self.state.main_loop_waiting_request is not None:
                            raise RuntimeError("overlapping synchronous GLOBAL FIFO futures")
                        self.state.rest_gateway_last_timing["synchronous_close_cancel"] = True
                        self.state.main_loop_waiting_request = self.state.rest_gateway_last_timing
                        self.state.main_loop_synchronous_request_count += 1
                        self.state.main_loop_next_wake_ms = None
            order["cancel_reason"] = reason
            if self.state.replace_terminal_continuation and replacement_reason:
                order["replace_terminal_continuation_armed"] = True
            if order["state"] == self.state.ORDER_OPEN:
                order["state"] = self.state.ORDER_PENDING_CANCEL
            if not self.state.async_rest_gateway:
                self._record_order_lifecycle_cancel_request(
                    order,
                    int(request_start_ts),
                    reason=str(reason),
                )
            self._ranked_guard_cancel_requested(
                order,
                visibility_ts_ms=int(now_ts if self.state.async_rest_gateway else request_start_ts),
                reason=str(reason),
                guard_initiated=bool(
                    normalized_guard_target
                    and order_id == normalized_guard_target
                ),
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

    def _resume_maker_close(self, continuation: dict[str, Any], ready_ts: int) -> None:
        """Run a saved close-call phase; pending requests contain data, not closures.

        Keep the original decision's price, quantity and order-list references.
        A delayed HTTP return is not a new quote decision. The complete replay
        checkpoint must serialize this graph together with its order ownership.
        """
        action = continuation["action"]
        if action == "start_clock":
            self.state.circuit_breaker_close_start_ts = int(ready_ts)
            return
        if action == "emergency":
            self._attempt_async_emergency_close(int(ready_ts))
            return
        if action not in {"continue", "submit"}:
            raise ValueError(f"unknown maker-close continuation: {action!r}")
        close_orders = continuation["close_orders"]
        opening_orders = continuation["opening_orders"]
        close_price = continuation["price"]
        use_ioc = continuation["use_ioc"]
        if action == "continue":
            if not close_orders and opening_orders:
                return
            if close_orders:
                existing = close_orders[0]
                if existing.get("state") != self.state.ORDER_OPEN:
                    return
                drift_bps = (
                    abs(close_price - float(existing["price"]))
                    / max(float(existing["price"]), self.state.TICK) * 10_000.0
                )
                if not use_ioc and drift_bps <= max(
                    0.0, float(self.context.params.get("requote_threshold_bps", 0.0) or 0.0)
                ):
                    self.state.circuit_breaker_close_keep_count += 1
                    return
                self._request_cancel_all(
                    close_orders, int(ready_ts), reason="circuit_breaker_close_requote",
                )
                if self.state.main_loop_waiting_request is not None:
                    self.state.main_loop_waiting_request["resume_close"] = {
                        **continuation, "action": "submit",
                    }
                return
        # Sync cancel ignores its HTTP body in live; only a locally
        # observed terminal clears the side. No inferred terminal.
        if close_orders:
            return
        if continuation["would_cross"] and not use_ioc:
            self.state.gtx_rejects += 1
            self.state.circuit_breaker_close_gtx_reject_count += 1
            self.state.circuit_breaker_close_gtx_reject_streak += 1
            return
        order = self._make_order(
            continuation["side"], float(close_price), float(continuation["quantity"]),
            int(ready_ts), float(continuation["mid"]),
            quote_context=continuation["quote_context"], apply_decision_compute=False,
            creation_source="resume_maker_close",
        )
        order["time_in_force"] = "IOC" if use_ioc else "GTX"
        order["emergency_market"] = continuation["emergency_market"]
        close_orders.append(order)
        self.state.circuit_breaker_close_place_count += 1
        self.state.circuit_breaker_close_ioc_place_count += int(use_ioc)

    def _start_maker_close(self, now_ts: int, *, reason: str) -> None:
        """Start both live timeout causes after the same cancel-all return."""
        self.state.circuit_breaker_closing = True
        self.state.circuit_breaker_close_start_ts = -1 if self.state.async_rest_gateway else int(now_ts)
        self.state.circuit_breaker_close_gtx_reject_streak = 0
        self._request_cancel_all(self.state.bid_orders, int(now_ts), reason=reason)
        self._request_cancel_all(self.state.ask_orders, int(now_ts), reason=reason)
        if self.state.async_rest_gateway:
            # Live assigns _close_start_time after synchronous cancel-all
            # returns for both position timeout and the circuit breaker.
            if self.state.main_loop_waiting_request is not None:
                self.state.main_loop_waiting_request["resume_close"] = {"action": "start_clock"}
            else:
                self.state.circuit_breaker_close_start_ts = int(now_ts)

    def _complete_synchronous_rest_request(self, request: dict[str, Any], now_ts: int) -> None:
        """Resume a close caller after HTTP and any positive-fill proof.

        Live fetches account trades after a positive IOC RESULT. We do not
        have that query's timing: waiting for both HTTP and the modeled private
        fill is a conservative visibility proxy, not measured accountTrades
        latency. The GLOBAL worker itself is released by HTTP independently.
        """
        if request is not self.state.main_loop_waiting_request or not request.get("http_returned", False):
            return
        order = request.get("order")
        if (
            order is not None and order.get("time_in_force") == "IOC"
            and not order.get("ioc_local_terminal", False)
        ):
            return
        self.state.main_loop_waiting_request = None
        self.state.main_loop_synchronous_wait_ms += int(now_ts) - int(request["origin_ts_ms"])
        resume_close = request.get("resume_close")
        if resume_close is not None:
            self._resume_maker_close(resume_close, int(now_ts))
        self._finish_main_loop_tick(int(now_ts))

    def _advance_async_rest_worker(self,
        now_ts: int, completed: dict[str, Any] | None = None,
    ) -> None:
        """One FIFO worker; HTTP return releases it, never a private callback.

        Only an explicitly validated order RESULT may contribute a local
        authority timestamp. A bare HTTP response frees the worker but never
        clears ownership. Neither may recreate an already-terminal order.
        """
        if completed is not None:
            if completed is not self.state.async_rest_active:
                return
            self.state.async_rest_active = None
            completed["http_returned"] = True
            if completed is self.state.async_bulk_request:
                self.state.async_bulk_request = None
            self._complete_synchronous_rest_request(completed, int(now_ts))
        if self.state.async_rest_active is not None:
            return
        while self.state.async_rest_queue:
            request = self.state.async_rest_queue[0]
            if int(request["origin_ts_ms"]) > int(now_ts):
                self.state.local_lifecycle_boundary_scheduler.schedule(
                    ts_ms=int(request["origin_ts_ms"]), phase="rest_return",
                    event_id=f"async-ready:{request['sequence']}",
                    payload={"async_rest_worker": True},
                )
                return
            self.state.async_rest_queue.pop(0)
            is_bulk = bool(request.get("is_bulk", False))
            order = None if is_bulk else request["order"]
            side_orders = [] if is_bulk else (
                self.state.bid_orders if order["side"] == "BUY" else self.state.ask_orders
            )
            # An already-admitted live FIFO write survives later admission
            # revocation. Safety cancel-all first drains those writes; it
            # cannot silently delete them. That separate bulk path is not
            # modeled by the ordinary per-request samples above.
            start = int(now_ts)
            self.state.rest_gateway_request_count += 1
            self.state.rest_gateway_request_wait_ms += start - int(request["origin_ts_ms"])
            self.state.rest_gateway_busy_ms += int(request["service_ms"])
            self.state.rest_gateway_busy_until_ms = start + int(request["service_ms"])
            request.update({
                "request_ts_ms": start,
                "exchange_effective_ts_ms": start + int(request["effective_ms"]),
                "local_visibility_ts_ms": start + int(request["ack_ms"]),
                "rest_return_ts_ms": self.state.rest_gateway_busy_until_ms,
            })
            operation = "new" if request["is_new"] else "cancel"
            if (
                operation in self.state.http_result_status_by_operation
                and not is_bulk
                and not request.get("synchronous_close_cancel", False)
                and order.get("time_in_force") != "IOC"
            ):
                # This is a schema/order-identity-validated NEW or CANCELED
                # RESULT assumption, not generic HTTP success. Keep the
                # separately sampled private callback timestamp in the trace.
                request["local_visibility_ts_ms"] = min(
                    request["local_visibility_ts_ms"], self.state.rest_gateway_busy_until_ms,
                )
            if is_bulk:
                for target in request["orders"]:
                    if not any(target is active for active in (*self.state.bid_orders, *self.state.ask_orders)):
                        continue
                    if float(
                        target.get("exchange_remaining", target.get("remaining", 0.0))
                    ) < self.state.LOT_SIZE:
                        continue
                    existing_effective = int(target.get("cancel_effective_ts", 0) or 0)
                    if 0 < existing_effective <= start:
                        # Already canceled physically by an earlier individual
                        # request. This bulk produces no second terminal event
                        # and cannot accelerate its still-pending callback.
                        continue
                    target.update({
                        "cancel_request_ts": start,
                        "cancel_effective_ts": request["exchange_effective_ts_ms"],
                        "cancel_ts": request["local_visibility_ts_ms"],
                        "cancel_rest_return_ts": self.state.rest_gateway_busy_until_ms,
                        "cancel_private_visibility_ts": request["local_visibility_ts_ms"],
                        "cancel_reason": request["reason"],
                    })
                    if target["state"] == self.state.ORDER_OPEN:
                        target["state"] = self.state.ORDER_PENDING_CANCEL
                    self._record_order_lifecycle_cancel_request(
                        target, start, reason=request["reason"],
                    )
            elif request["is_new"]:
                order.update({
                    "gateway_request_ts": start,
                    "activate_ts": request["exchange_effective_ts_ms"],
                    "new_ack_ts": request["local_visibility_ts_ms"],
                    "new_rest_return_ts": self.state.rest_gateway_busy_until_ms,
                    "new_private_visibility_ts": start + int(request["ack_ms"]),
                })
                self._capture_zero_service_execution_context(order, request, start)
                self._record_order_lifecycle_submit(order, start, request_sequence=request['sequence'])
                self._ranked_guard_order_submitted(order)
            else:
                order.update({
                    "cancel_request_ts": start,
                    "cancel_effective_ts": request["exchange_effective_ts_ms"],
                    "cancel_ts": request["local_visibility_ts_ms"],
                    "cancel_rest_return_ts": self.state.rest_gateway_busy_until_ms,
                    "cancel_private_visibility_ts": start + int(request["ack_ms"]),
                })
                if order in side_orders:
                    self._record_order_lifecycle_cancel_request(
                        order, start, reason=str(order.get("cancel_reason", "")),
                        request_sequence=request['sequence'],
                    )
            self.state.async_rest_active = request
            self.state.local_lifecycle_boundary_scheduler.schedule(
                ts_ms=self.state.rest_gateway_busy_until_ms, phase="rest_return",
                event_id=f"async-return:{request['sequence']}",
                payload={"async_rest_worker": True, "completed": request},
            )
            return

    def _serial_rest_coalesce(self, plan: dict[str, Any], side: str, now_ts: int) -> None:
        if side in plan["blocked_sides"]:
            return
        plan["blocked_sides"].add(side)
        if not plan["new_requested"].get(side, False):
            return
        self.state.rest_return_pending_coalesce_count += 1
        action = plan["actions"][side]
        exposure = plan["exposure"][side]
        if action == "replace":
            self.state.decision_replace_count -= 1
        elif action == "place":
            self.state.decision_place_count -= 1
        if action in {"replace", "place"}:
            role = "exposure" if exposure else "reducing"
            key = f"decision_{side.lower()}_{role}_{action}_count"
            self.state.decision_side_role_action_counts[key] -= 1
            self._count_decision_action("pending_coalesce", side=side, exposure_increasing=exposure)
        self._record_l2_action_revision(side, now_ts, action,
                                   plan['creation_opportunities'].get(side))
        for row in plan["trace_rows"]:
            if row["side"] == side:
                row["action"] = "pending_coalesce"
                row["needs_update"] = 0
                row["rest_return_ts_ms"] = int(now_ts)

    def _record_l2_action_revision(self, side, now_ts, previous_action, opportunity_id):
        event = self.state.l2_production.observe('decision_action_revision', now_ts, side)
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit('decision_action_revision', now_ts, side=side,
                payload={'opportunity_event_id': opportunity_id, 'previous_action': previous_action,
                         'action': 'pending_coalesce', 'reason': 'unresolved_order_ownership',
                         'managed_order_ids': [o['trace_id'] for o in
                            (self.state.bid_orders if side == 'BUY' else self.state.ask_orders)]},
                production_event=event)

    def _schedule_main_loop_tick(self, loop_start_ms: int) -> None:
        """Run measured outer-loop work before entering the strategy tick.

        Both columns use one keyed row; no measured tick work means no
        invented scheduling jitter. Market and private events keep advancing
        while this thread is occupied.
        """
        self.state.main_loop_next_start_ms = int(loop_start_ms)
        if self.state.quote_schedule is not None:
            # An interruptible sleep has not consumed the next loop's work
            # sample. Sample only after its actual start is selected below.
            self.state.main_loop_pre_pending = True
            self.state.main_loop_next_wake_ms = int(loop_start_ms)
            return
        self._begin_main_loop_work(loop_start_ms)

    def _begin_main_loop_work(self, loop_start_ms: int) -> None:
        before_ms = 0
        self.state.main_loop_after_tick_ms = 0
        if self.state.main_loop_work_enabled:
            before_ms, self.state.main_loop_after_tick_ms = (
                self.services._deterministic_decision_to_gateway_latency_ms(
                    self.state.main_loop_work_samples_ms[:, column],
                    seed=self.state.decision_to_gateway_latency_seed, decision_ts_ms=int(loop_start_ms),
                ) for column in (0, 1)
            )
            self.state.main_loop_work_pre_sum_ms += before_ms
        self.state.main_loop_next_wake_ms = int(loop_start_ms) + before_ms

    def _replacement_continuation_pending(self, side: str) -> bool:
        return bool(
            self.state.replace_terminal_continuation
            and self.state.replacement_terminal_due_ts[side] >= 0
            and not (self.state.bid_orders if side == "BUY" else self.state.ask_orders)
        )

    def _finish_main_loop_tick(self, completed_ms: int) -> None:
        """Account for non-REST work once, after the final request/compute.

        A requote residual placed here preserves measured total busy time,
        but does not claim its unobserved between-request placement is exact.
        The same completion rule applies to keep/skip calls with no requests.
        """
        if self.state.main_loop_waiting_request is not None:
            return
        self.state.main_loop_work_post_sum_ms += self.state.main_loop_after_tick_ms
        loop_end_ms = (
            max(int(completed_ms), self.state.main_loop_tick_complete_ms,
                0 if self.state.async_rest_gateway else self.state.rest_gateway_busy_until_ms)
            + self.state.main_loop_quote_tail_ms + self.state.main_loop_after_tick_ms
        )
        self.state.main_loop_quote_tail_ms = 0
        self.state.main_loop_idle_start_ms = loop_end_ms
        ledger = self.state.quote_schedule
        if ledger is not None and ledger.inflight is not None:
            ledger.finish(ledger.inflight.sequence, busy_until_ns=loop_end_ms * 1_000_000)
        if ledger is not None:
            # A retained terminal flag is not runnable while that side still
            # owns orders. Preserve the flag; lifecycle events may unblock it.
            continuation_ready = self.state.async_rest_gateway and any(
                self._replacement_continuation_pending(side) for side in ("BUY", "SELL")
            )
            next_start = loop_end_ms + (
                0 if continuation_ready or ledger.unhandled() else self.state.main_loop_sleep_ms
            )
            # claim uses now >= busy_until, unlike strict input ready < now.
            next_start = max(next_start, (ledger.busy_until_ns + 999_999) // 1_000_000)
            self._schedule_main_loop_tick(next_start)
            return
        # Keep the completed existing-mode execution contract unchanged.
        continuation_ready = self.state.async_rest_gateway and any(
            due >= 0 for due in self.state.replacement_terminal_due_ts.values()
        )
        state_ready = ledger is not None and ledger.unhandled()
        if getattr(self.state, "main_loop_no_poll_wait", False) and not (continuation_ready or state_ready):
            # Preserve existing quote eligibility. This changes wake timing,
            # not quote_schedule_mode, quote interval, cooldown or risk rules.
            deadlines = [
                self.state.last_rq_ts + self.state.cur_rq_ms,
                self.state.local_lifecycle_boundary_scheduler.next_timestamp(),
                *self._quote_schedule_risk_deadlines(),
            ]
            input_arrived_during_work = False
            for feed, _ in self._quote_schedule_sources():
                _, version, due = self._quote_schedule_position(feed, loop_end_ms)
                start_version = self._quote_schedule_position(
                    feed, self.state.main_loop_next_start_ms)[1]
                input_arrived_during_work |= version > start_version
                deadlines.append(due)
            if input_arrived_during_work:
                # Match an input notification retained by live while tick was
                # busy. A zero-work next tick consumes it once, without
                # advancing the original requote eligibility.
                self._schedule_main_loop_tick(loop_end_ms)
                return
            self._schedule_main_loop_tick(self.services.next_no_poll_wait_ms(loop_end_ms, deadlines))
            return
        self._schedule_main_loop_tick(loop_end_ms + (0 if continuation_ready or state_ready else self.state.main_loop_sleep_ms))

    def _continue_serial_rest_decision(self,
        plan: dict[str, Any], now_ts: int, completed: Optional[dict[str, Any]] = None,
    ) -> None:
        """Resume a frozen quote call at HTTP return, not at private ACK.

        Only intents are retained between boundaries. A NEW enters the local
        order ledger when its actual request slot is reached, after observing
        fills/cancel callbacks that arrived during the preceding REST call.
        """
        if plan is not self.state.serial_rest_decision:
            return
        if completed is not None and completed["kind"] == "cancel":
            old = completed["order"]
            side_orders = self.state.bid_orders if completed["side"] == "BUY" else self.state.ask_orders
            terminal = old not in side_orders or float(old["remaining"]) < self.state.LOT_SIZE
            if not terminal:
                self._serial_rest_coalesce(plan, completed["side"], int(now_ts))
        elif completed is not None and self.state.planned_quote_stop_triggered:
            # A stop that arrived during submit could not cancel PENDING_NEW.
            # Once the response converges, drain that accepted order without
            # resuming the remaining NEW intents or normal quote computation.
            order = completed.get("order")
            if order is not None:
                self._request_cancel_all([order], int(now_ts), "planned_maintenance")
        while plan["next"] < len(plan["steps"]):
            if not self.state.async_rest_gateway and int(now_ts) < self.state.rest_gateway_busy_until_ms:
                self.state.serial_rest_sequence += 1
                self.state.local_lifecycle_boundary_scheduler.schedule(
                    ts_ms=self.state.rest_gateway_busy_until_ms, phase="rest_return",
                    event_id=f"rest-resume:{self.state.serial_rest_sequence}",
                    payload={"serial_rest_plan": plan},
                )
                return
            step = plan["steps"][plan["next"]]
            plan["next"] += 1
            side = step["side"]
            side_orders = self.state.bid_orders if side == "BUY" else self.state.ask_orders
            response_token = plan.get('response_observations', {}).get(side)
            if step["kind"] == "new":
                if (side in plan["blocked_sides"] or self.state.planned_quote_stop_triggered
                        or self.state.risk_emergency_latched):
                    if self.state.response_update_observer is not None:
                        self.state.response_update_observer.blocked(response_token,
                            'blocked_side' if side in plan['blocked_sides'] else
                            'planned_quote_stop' if self.state.planned_quote_stop_triggered else
                            'risk_emergency', now_ts)
                    continue
                if any(float(o["remaining"]) >= self.state.LOT_SIZE for o in side_orders):
                    if self.state.response_update_observer is not None:
                        self.state.response_update_observer.blocked(response_token, 'remaining_same_side_order', now_ts)
                    self._serial_rest_coalesce(plan, side, int(now_ts))
                    continue
                if self.services.math.isfinite(self.state.max_position_value) and self.services._exposure_increasing(
                    side, self.state.q, step["quantity"], self.state.LOT_SIZE,
                ):
                    capped = self.services.cap_exposure_qty_by_position_value(
                        side=side, current_qty=self.state.q, mid=plan["submit_mid"],
                        requested_qty=step["quantity"],
                        max_position_value=self.state.max_position_value, lot=self.state.LOT_SIZE,
                    )
                    self.state.risk_notional_cap_count += int(capped < step["quantity"])
                    step["quantity"] = capped
                    if self.state.response_update_observer is not None:
                        self.state.response_update_observer.checked(response_token,
                            'request_notional_cap', logical_ns=int(now_ts)*1_000_000,
                            final_quantity=float(capped), lot_size=float(self.state.LOT_SIZE))
                    if capped < self.state.LOT_SIZE:
                        if self.state.response_update_observer is not None:
                            self.state.response_update_observer.blocked(response_token, 'notional_cap_below_lot', now_ts)
                        self._serial_rest_coalesce(plan, side, int(now_ts))
                        continue
            else:
                order = step["order"]
                if order not in side_orders or float(order["remaining"]) < self.state.LOT_SIZE:
                    if self.state.response_update_observer is not None:
                        self.state.response_update_observer.blocked(response_token, 'cancel_order_absent_or_below_lot', now_ts)
                    continue
                if order["state"] in (self.state.ORDER_PENDING_NEW, self.state.ORDER_PENDING_CANCEL):
                    if self.state.response_update_observer is not None:
                        self.state.response_update_observer.blocked(response_token, 'cancel_order_pending', now_ts)
                    self._serial_rest_coalesce(plan, side, int(now_ts))
                    continue
            slot = f"{step['kind']}_{side.lower()}"
            self._prepare_rest_gateway_timing(
                decision_ts=max(int(now_ts), int(plan["ready_ts"])),
                requested_slots=(slot,),
            )
            try:
                if step["kind"] == "cancel":
                    self._request_cancel_all(
                        [step["order"]], int(now_ts), step["reason"],
                        guard_initiated_order_id=step.get("guard_id", ""),
                        request_key_ts=plan["decision_ts"],
                        response_observation_token=response_token,
                    )
                else:
                    order = self._make_order(
                        side, step["price"], step["quantity"],
                        plan["decision_ts"], plan["submit_mid"],
                        plan["quote_flags"], plan["quote_context"],
                        apply_decision_compute=False,
                        creation_opportunity_event_id=plan["creation_opportunities"].get(side),
                        response_observation_token=response_token,
                    )
                    step["order"] = order
                    side_orders.append(order)
            finally:
                self._clear_rest_gateway_timing()
            if self.state.async_rest_gateway:
                # Async admission returns pending immediately. In particular
                # a same-side NEW following CANCEL coalesces above until a
                # fresh terminal-driven decision, not until HTTP return.
                continue
            self.state.serial_rest_sequence += 1
            self.state.local_lifecycle_boundary_scheduler.schedule(
                ts_ms=self.state.rest_gateway_busy_until_ms, phase="rest_return",
                event_id=f"rest-return:{self.state.serial_rest_sequence}",
                payload={"serial_rest_plan": plan, "completed": step},
            )
            return
        self.state.serial_rest_decision = None
        if self.state.main_loop_enabled:
            self._finish_main_loop_tick(int(now_ts))

    def _begin_serial_rest_decision(self,
        *, decision_ts: int, steps: list[dict[str, Any]],
        new_requested: dict[str, bool], actions: dict[str, str],
        exposure: dict[str, bool], submit_mid: float,
        quote_flags: dict[str, Any], quote_context: dict[str, Any],
        request_ready_ts: Optional[int] = None,
    ) -> None:
        if not steps:
            return
        if self.state.serial_rest_decision is not None:
            raise RuntimeError("overlapping synchronous quote decisions")
        ready_ts = (
            self._decision_gateway_request_ts(int(decision_ts))
            if request_ready_ts is None else int(request_ready_ts)
        )
        plan = {
            "decision_ts": int(decision_ts), "ready_ts": int(ready_ts),
            "steps": steps, "next": 0, "blocked_sides": set(),
            "new_requested": new_requested, "actions": actions, "exposure": exposure,
            "submit_mid": float(submit_mid), "quote_flags": dict(quote_flags),
            "creation_opportunities": {
                side: self.state.l2_decision_links.get((id(context), int(decision_ts), side))
                for side, context in quote_context.items()
            },
            "quote_context": {
                side: dict(context) if isinstance(context, dict) else context
                for side, context in quote_context.items()
            },
            "trace_rows": [row for row in (self.state.trace_decisions or [])[-2:]
                           if row["ts_ms"] == int(decision_ts)],
        }
        if self.state.response_update_observer is not None:
            plan['response_observations'] = {
                side: self.state.response_update_observer.token(side, int(self.state.nrq))
                for side in ('BUY', 'SELL')}
        self.state.serial_rest_decision = plan
        if self.state.main_loop_enabled:
            self.state.main_loop_next_wake_ms = None
        self.state.serial_rest_sequence += 1
        self.state.local_lifecycle_boundary_scheduler.schedule(
            ts_ms=ready_ts, phase="rest_return",
            event_id=f"rest-start:{self.state.serial_rest_sequence}",
            payload={"serial_rest_plan": plan},
        )

    def _has_cancel_request_candidate(self,
        orders: list[dict[str, Any]],
        *,
        target_order_id: str = "",
    ) -> bool:
        normalized_target = str(target_order_id).strip()
        for order in orders:
            if float(order.get("remaining", 0.0) or 0.0) < self.state.LOT_SIZE:
                continue
            if normalized_target and self._ranked_guard_order_id(order) != normalized_target:
                continue
            if int(order.get("cancel_ts", -1) or -1) > 0:
                continue
            return True
        return False

    def _execute_order_decisions(self):
        """Execute original REST ordering and reconcile blocked replacements."""
        self.state.replacement_blocked_sides = set()
        if self.state.serial_rest_return_enabled:
            self.state.serial_steps = []
            self.state.serial_new_requested = {}
            for loop_item in (
                ("BUY", self.state.bid_orders, self.state.ranked_guard_bid_cancel_order_id,
                 self.state.bid_cancel_first, self.state.bid_updated, self.state.hb_new),
                ("SELL", self.state.ask_orders, self.state.ranked_guard_ask_cancel_order_id,
                 self.state.ask_cancel_first, self.state.ask_updated, self.state.ha_new),
            ):
                self.state.side, self.state.orders, self.state.guard_id, self.state.cancel_first, self.state.updated, self.state.can_new = loop_item
                self.state.serial_new_requested[self.state.side] = bool(
                    not self.state.guard_id and not self.state.cancel_first and self.state.updated and self.state.can_new
                )
                if self.state.guard_id or self.state.cancel_first or self.state.updated:
                    for loop_item in self.state.orders:
                        self.state.old = loop_item
                        if self.state.guard_id and self._ranked_guard_order_id(self.state.old) != self.state.guard_id:
                            continue
                        self.state.reason = (
                            "ranked_toxicity_guard" if self.state.guard_id else
                            "cancel_first_replace" if self.state.cancel_first else
                            "risk_selection_cancel" if self.state.side in self.state.risk_selection_cancel_sides else
                            "requote_replace" if self.state.can_new else "side_disabled"
                        )
                        self.state.serial_steps.append({
                            "kind": "cancel", "side": self.state.side, "order": self.state.old,
                            "reason": self.state.reason, "guard_id": self.state.guard_id,
                        })
            for loop_item in (
                ("BUY", self.state.nbid, self.state.bid_sz), ("SELL", self.state.nask, self.state.ask_sz),
            ):
                self.state.side, self.state.price, self.state.quantity = loop_item
                if self.state.serial_new_requested[self.state.side]:
                    self.state.serial_steps.append({
                        "kind": "new", "side": self.state.side,
                        "price": float(self.state.price), "quantity": float(self.state.quantity),
                    })
            self._begin_serial_rest_decision(
                decision_ts=int(self.state.t), steps=self.state.serial_steps,
                new_requested=self.state.serial_new_requested,
                actions={"BUY": self.state.bid_action, "SELL": self.state.ask_action},
                exposure={"BUY": self.state.q >= 0.0, "SELL": self.state.q <= 0.0},
                submit_mid=float(self.state.p), quote_flags=self.state.quote_flags,
                quote_context=self.state.quote_context,
                request_ready_ts=(
                    int(self.state.active_quote_compute["request_ready_ts"])
                    if self.state.active_quote_compute is not None else None
                ),
            )
            self._advance_exchange_book_through_order_lifecycle(int(self.state.t), float(self.state.p))
        elif self.state.rest_gateway_timing_enabled:
            self.state.bid_cancel_requested = bool(
                (
                    self.state.ranked_guard_bid_cancel_order_id
                    and self._has_cancel_request_candidate(
                        self.state.bid_orders,
                        target_order_id=self.state.ranked_guard_bid_cancel_order_id,
                    )
                )
                or (
                    not self.state.ranked_guard_bid_cancel_order_id
                    and (self.state.bid_cancel_first or self.state.bid_updated)
                    and self._has_cancel_request_candidate(self.state.bid_orders)
                )
            )
            self.state.ask_cancel_requested = bool(
                (
                    self.state.ranked_guard_ask_cancel_order_id
                    and self._has_cancel_request_candidate(
                        self.state.ask_orders,
                        target_order_id=self.state.ranked_guard_ask_cancel_order_id,
                    )
                )
                or (
                    not self.state.ranked_guard_ask_cancel_order_id
                    and (self.state.ask_cancel_first or self.state.ask_updated)
                    and self._has_cancel_request_candidate(self.state.ask_orders)
                )
            )
            self.state.bid_new_requested = bool(
                not self.state.ranked_guard_bid_cancel_order_id
                and not self.state.bid_cancel_first
                and self.state.bid_updated
                and self.state.hb_new
            )
            self.state.ask_new_requested = bool(
                not self.state.ranked_guard_ask_cancel_order_id
                and not self.state.ask_cancel_first
                and self.state.ask_updated
                and self.state.ha_new
            )
            self.state.requested_gateway_slots = tuple(
                slot
                for slot, requested in (
                    ("cancel_buy", self.state.bid_cancel_requested),
                    ("cancel_sell", self.state.ask_cancel_requested),
                    ("new_buy", self.state.bid_new_requested),
                    ("new_sell", self.state.ask_new_requested),
                )
                if requested
            )
            self._prepare_rest_gateway_timing(
                decision_ts=int(self.state.t),
                requested_slots=self.state.requested_gateway_slots,
                apply_decision_compute=True,
            )

            # Live sends the synchronous requests in this slot order.  Do
            # not let Python's historical side-by-side update order change
            # the paired empirical row's chronology.
            if self.state.ranked_guard_bid_cancel_order_id:
                self._request_cancel_all(
                    self.state.bid_orders,
                    self.state.t,
                    "ranked_toxicity_guard",
                    target_order_id=self.state.ranked_guard_bid_cancel_order_id,
                    guard_initiated_order_id=self.state.ranked_guard_bid_cancel_order_id,
                )
            elif self.state.bid_cancel_first:
                if self.state.bid_orders:
                    self._request_cancel_all(
                        self.state.bid_orders, self.state.t, "cancel_first_replace"
                    )
            elif self.state.bid_updated and self.state.bid_orders:
                self._request_cancel_all(
                    self.state.bid_orders,
                    self.state.t,
                    self.state.bid_update_cancel_reason,
                )

            if self.state.ranked_guard_ask_cancel_order_id:
                self._request_cancel_all(
                    self.state.ask_orders,
                    self.state.t,
                    "ranked_toxicity_guard",
                    target_order_id=self.state.ranked_guard_ask_cancel_order_id,
                    guard_initiated_order_id=self.state.ranked_guard_ask_cancel_order_id,
                )
            elif self.state.ask_cancel_first:
                if self.state.ask_orders:
                    self._request_cancel_all(
                        self.state.ask_orders, self.state.t, "cancel_first_replace"
                    )
            elif self.state.ask_updated and self.state.ask_orders:
                self._request_cancel_all(
                    self.state.ask_orders,
                    self.state.t,
                    self.state.ask_update_cancel_reason,
                )

            self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
            self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
            if self.state.bid_new_requested and self.state.bid_orders:
                self.state.replacement_blocked_sides.add("BUY")
            if self.state.ask_new_requested and self.state.ask_orders:
                self.state.replacement_blocked_sides.add("SELL")
            if self.state.bid_new_requested and not self.state.bid_orders:
                self.state.bid_orders.append(
                    self._make_order(
                        "BUY",
                        self.state.nbid,
                        self.state.bid_sz,
                        self.state.t,
                        self.state.p,
                        self.state.quote_flags,
                        self.state.quote_context,
                    )
                )
            if self.state.ask_new_requested and not self.state.ask_orders:
                self.state.ask_orders.append(
                    self._make_order(
                        "SELL",
                        self.state.nask,
                        self.state.ask_sz,
                        self.state.t,
                        self.state.p,
                        self.state.quote_flags,
                        self.state.quote_context,
                    )
                )
            self._clear_rest_gateway_timing()
        else:
            if self.state.ranked_guard_bid_cancel_order_id:
                self._request_cancel_all(
                    self.state.bid_orders,
                    self.state.t,
                    "ranked_toxicity_guard",
                    target_order_id=self.state.ranked_guard_bid_cancel_order_id,
                    guard_initiated_order_id=self.state.ranked_guard_bid_cancel_order_id,
                )
            elif self.state.bid_cancel_first:
                if self.state.bid_orders:
                    self._request_cancel_all(self.state.bid_orders, self.state.t, "cancel_first_replace")
            elif self.state.bid_updated:
                if self.state.bid_orders:
                    self._request_cancel_all(
                        self.state.bid_orders, self.state.t,
                        self.state.bid_update_cancel_reason,
                )
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                if self.state.hb_new and self.state.bid_orders:
                    self.state.replacement_blocked_sides.add("BUY")
                if self.state.hb_new and not self.state.bid_orders:
                    self.state.bid_orders.append(
                        self._make_order(
                            "BUY",
                            self.state.nbid,
                            self.state.bid_sz,
                            self.state.t,
                            self.state.p,
                            self.state.quote_flags,
                            self.state.quote_context,
                        )
                    )
            if self.state.ranked_guard_ask_cancel_order_id:
                self._request_cancel_all(
                    self.state.ask_orders,
                    self.state.t,
                    "ranked_toxicity_guard",
                    target_order_id=self.state.ranked_guard_ask_cancel_order_id,
                    guard_initiated_order_id=self.state.ranked_guard_ask_cancel_order_id,
                )
            elif self.state.ask_cancel_first:
                if self.state.ask_orders:
                    self._request_cancel_all(self.state.ask_orders, self.state.t, "cancel_first_replace")
            elif self.state.ask_updated:
                if self.state.ask_orders:
                    self._request_cancel_all(
                        self.state.ask_orders, self.state.t,
                        self.state.ask_update_cancel_reason,
                )
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
                if self.state.ha_new and self.state.ask_orders:
                    self.state.replacement_blocked_sides.add("SELL")
                if self.state.ha_new and not self.state.ask_orders:
                    self.state.ask_orders.append(
                        self._make_order(
                            "SELL",
                            self.state.nask,
                            self.state.ask_sz,
                            self.state.t,
                            self.state.p,
                            self.state.quote_flags,
                            self.state.quote_context,
                        )
                    )

        # A cancel becoming exchange-effective is not a local terminal
        # observation. Do not pre-create a replacement while ownership
        # remains unresolved; report the actual coalesced action too.
        for loop_item in self.state.replacement_blocked_sides:
            self.state.side = loop_item
            self.state.action = self.state.bid_action if self.state.side == "BUY" else self.state.ask_action
            self.state.exposure = self.state.q >= 0.0 if self.state.side == "BUY" else self.state.q <= 0.0
            if self.state.action == "replace":
                self.state.decision_replace_count -= 1
            elif self.state.action == "place":
                self.state.decision_place_count -= 1
            if self.state.action in {"replace", "place"}:
                self.state.role = "exposure" if self.state.exposure else "reducing"
                self.state.decision_side_role_action_counts[f"decision_{self.state.side.lower()}_{self.state.role}_{self.state.action}_count"] -= 1
                self._count_decision_action("pending_coalesce", side=self.state.side, exposure_increasing=self.state.exposure)
                self._record_l2_action_revision(self.state.side, self.state.t, self.state.action,
                    self.state.l2_decision_links.get((
                        id(self.state.quote_context.get(self.state.side, {})),
                        int(self.state.t), self.state.side)))
                for loop_item in reversed(self.state.trace_decisions or []):
                    self.state.row = loop_item
                    if self.state.row["ts_ms"] != int(self.state.t):
                        break
                    if self.state.row["side"] == self.state.side:
                        self.state.row["action"] = "pending_coalesce"
                        self.state.row["needs_update"] = 0
        self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
        self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
        self.state.max_pending_new_orders = max(
            self.state.max_pending_new_orders,
            sum(order["state"] == self.state.ORDER_PENDING_NEW for order in self.state.bid_orders) +
            sum(order["state"] == self.state.ORDER_PENDING_NEW for order in self.state.ask_orders),
        )
        self.state.max_pending_cancel_orders = max(
            self.state.max_pending_cancel_orders,
            sum(order["state"] == self.state.ORDER_PENDING_CANCEL for order in self.state.bid_orders) +
            sum(order["state"] == self.state.ORDER_PENDING_CANCEL for order in self.state.ask_orders),
        )
