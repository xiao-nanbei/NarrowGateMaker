"""Replay exchange owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional


class ExchangeAdapter:
    def _trace_exchange_book_queue_ambiguity(self,
        order: dict,
        *,
        reason: str,
        event_ts_ms: int | None,
        ambiguous: bool,
    ) -> None:
        if (
            self.state.exchange_book_queue_ambiguity_trace_max <= 0
            or len(self.state.exchange_book_queue_ambiguity_trace)
            >= self.state.exchange_book_queue_ambiguity_trace_max
        ):
            return
        self.state.exchange_book_queue_ambiguity_trace.append(
            {
                "reason": str(reason),
                "ambiguous": bool(ambiguous),
                "event_ts_ms": (
                    None if event_ts_ms is None else int(event_ts_ms)
                ),
                "order_id": int(order.get("trace_id", -1) or -1),
                "side": str(order.get("side", "")),
                "state": str(order.get("state", "")),
                "price_tick": int(
                    order.get(
                        "exchange_book_queue_price_tick",
                        self.services._order_price_tick(order, self.state.TICK),
                    )
                ),
                "activate_ts_ms": int(order.get("activate_ts", -1) or -1),
                "cancel_ts_ms": int(order.get("cancel_ts", -1) or -1),
                "cancel_effective_ts_ms": int(
                    self._order_cancel_effective_ts(order)
                ),
                "queue_seed_status": str(
                    order.get("_exchange_book_queue_seed_status", "")
                ),
            }
        )

    def _invalidate_exchange_book_order_path(self,
        order: dict,
        *,
        reason: str,
        ambiguous: bool = False,
        event_ts_ms: int | None = None,
    ) -> None:

        was_valid = bool(
            order.get("exchange_book_queue_path_valid", False)
        )
        new_ambiguity = bool(
            ambiguous
            and not order.get("exchange_book_queue_ambiguous", False)
        )
        if not was_valid and not new_ambiguity:
            return
        self._trace_exchange_book_queue_ambiguity(
            order,
            reason=reason,
            event_ts_ms=event_ts_ms,
            ambiguous=bool(ambiguous),
        )
        if was_valid:
            order["exchange_book_queue_path_valid"] = False
            order["exact_queue_path_valid"] = False
            order["exchange_book_queue_invalidated"] = True
            order["exchange_book_queue_invalidated_reason"] = str(reason)
            self.state.exchange_book_queue_invalidated_order_count += 1
        if new_ambiguity:
            order["exchange_book_queue_ambiguous"] = True
            self.state.exchange_book_queue_ambiguous_event_count += 1
        hazard_path = self.state.dynamic_fill_hazard_paths.get(
            str(order.get("trace_id", ""))
        )
        if (
            hazard_path is not None
            and self.state.dynamic_fill_hazard_visibility_scheduler is None
        ):
            hazard_path.invalidate(str(reason))
            if self.state.dynamic_fill_hazard_cpp_runtime is not None:
                self.state.dynamic_fill_hazard_cpp_runtime.invalidate_order(
                    str(order.get("trace_id", "")),
                    str(reason),
                )
        inventory_lifecycle_id = int(order.get("inventory_lifecycle_id_at_submit", 0) or 0)
        if (
            inventory_lifecycle_id > 0
            and int(
                order.get(
                    "queue_value_keep_cancel_intervention_id",
                    -1,
                )
                or -1
            )
            < 0
            and inventory_lifecycle_id not in self.state.queue_value_intervened_inventory_lifecycles
        ):
            self.state.exchange_book_predecision_invalid_inventory_lifecycles.add(inventory_lifecycle_id)
        record = self.state.local_order_value_open_records.get(
            int(order.get("trace_id", -1))
        )
        if record is not None:
            record["exchange_book_queue_path_valid"] = 0
            record["exchange_book_queue_invalidated"] = 1
            if was_valid:
                record["exchange_book_queue_invalidated_reason"] = str(
                    reason
                )
            if new_ambiguity:
                record["exchange_book_queue_ambiguous"] = 1
                record["exchange_book_ambiguous_event_count"] = float(
                    record.get(
                        "exchange_book_ambiguous_event_count",
                        0.0,
                    )
                    + 1.0
                )
        intervention_id = int(
            order.get(
                "queue_value_keep_cancel_intervention_id",
                -1,
            )
            or -1
        )
        action_record = self.state.queue_value_open_records.get(intervention_id)
        if action_record is not None:
            action_record["exchange_book_queue_path_valid"] = 0
            action_record["exchange_book_queue_invalidated"] = 1
            if was_valid:
                action_record[
                    "exchange_book_queue_invalidated_reason"
                ] = str(reason)
            if new_ambiguity:
                action_record["exchange_book_queue_ambiguous"] = 1

    def _mark_exchange_book_path_invalid(self,
        orders: list[dict],
        *,
        reason: str,
        event_ts_ms: int | None = None,
    ) -> None:
        for order in orders:
            self._invalidate_exchange_book_order_path(
                order,
                reason=reason,
                event_ts_ms=event_ts_ms,
            )

    def _same_ms_execution_trade_tick_extremes(self,
        now_ts_ms: int,
    ) -> tuple[int | None, int | None]:
        """Return all-trade SELL-min/BUY-max ticks for one exchange millisecond."""


        timestamp = int(now_ts_ms)
        if self.state.same_ms_trade_extremes_cache_ts == timestamp:
            return self.state.same_ms_trade_extremes_cache
        sell_min_tick = None
        buy_max_tick = None
        batch_left = int(self.services.np.searchsorted(self.state.trade_ts, timestamp, side="left"))
        batch_right = int(self.services.np.searchsorted(self.state.trade_ts, timestamp, side="right"))
        for batch_idx in range(batch_left, batch_right):
            if not self.state.is_execution_trade[batch_idx] or self.state.trade_qty[batch_idx] <= 0.0:
                continue
            batch_tick = self.services._price_to_tick(float(self.state.trade_price[batch_idx]), self.state.TICK)
            if self.state.is_seller[batch_idx]:
                sell_min_tick = (
                    batch_tick
                    if sell_min_tick is None
                    else min(sell_min_tick, batch_tick)
                )
            else:
                buy_max_tick = (
                    batch_tick
                    if buy_max_tick is None
                    else max(buy_max_tick, batch_tick)
                )
        self.state.same_ms_trade_extremes_cache_ts = timestamp
        self.state.same_ms_trade_extremes_cache = (sell_min_tick, buy_max_tick)
        return self.state.same_ms_trade_extremes_cache

    def _mark_cancel_ack_trade_ambiguity(self,
        orders: list[dict],
        side: str,
        now_ts_ms: int,
    ) -> None:
        """Censor cancel/fill races without a common exchange sequence."""


        if self.state.exchange_book_scheduler is None:
            return
        sell_min_tick, buy_max_tick = self._same_ms_execution_trade_tick_extremes(
            int(now_ts_ms)
        )
        for order in orders:
            if order.get("state") not in (
                self.state.ORDER_OPEN,
                self.state.ORDER_PENDING_CANCEL,
            ):
                continue
            if self._order_cancel_effective_ts(order) != int(now_ts_ms):
                continue
            order_tick = self.services._order_price_tick(order, self.state.TICK)
            crosses = bool(
                (
                    side == "BUY"
                    and sell_min_tick is not None
                    and sell_min_tick <= order_tick
                )
                or (
                    side == "SELL"
                    and buy_max_tick is not None
                    and buy_max_tick >= order_tick
                )
            )
            if not crosses:
                continue
            was_ambiguous = bool(
                order.get("exchange_book_queue_ambiguous", False)
            )
            self._invalidate_exchange_book_order_path(
                order,
                reason="same_ms_cancel_ack_trade_ambiguity",
                ambiguous=True,
                event_ts_ms=int(now_ts_ms),
            )
            if (
                not was_ambiguous
                and bool(
                    order.get("exchange_book_queue_ambiguous", False)
                )
            ):
                self.state.exchange_book_cancel_trade_ambiguous_order_count += 1

    def _mark_cancel_ack_exchange_book_ambiguity(self,
        orders: list[dict],
        side: str,
        now_ts_ms: int,
    ) -> None:
        """Censor cancel/native-book races without a shared sequence."""


        if self.state.exchange_book_scheduler is None:
            return
        preview = self.state.exchange_book_scheduler.preview_at(
            int(now_ts_ms) * 1_000_000
        )
        if preview.event_count <= 0:
            return
        native_side = "bid" if side == "BUY" else "ask"
        for order in orders:
            if order.get("state") not in (
                self.state.ORDER_OPEN,
                self.state.ORDER_PENDING_CANCEL,
            ):
                continue
            if self._order_cancel_effective_ts(order) != int(now_ts_ms):
                continue
            price_tick = int(
                order.get(
                    "exchange_book_queue_price_tick",
                    self.services._order_price_tick(order, self.state.TICK),
                )
            )
            if (
                not preview.snapshot_or_gap
                and (native_side, price_tick) not in preview.touched_levels
            ):
                continue
            was_ambiguous = bool(
                order.get("exchange_book_queue_ambiguous", False)
            )
            self._invalidate_exchange_book_order_path(
                order,
                reason="same_ms_cancel_ack_exchange_book_ambiguity",
                ambiguous=True,
                event_ts_ms=int(now_ts_ms),
            )
            if (
                not was_ambiguous
                and bool(order.get("exchange_book_queue_ambiguous", False))
            ):
                self.state.exchange_book_cancel_book_ambiguous_order_count += 1

    def _deplete_policy_visible_queue(self,
        orders: list[dict],
        side: str,
        trade_price_now: float,
        trade_qty_now: float,
        deplete_mult: float,
    ) -> None:
        """Advance the delayed, policy-visible queue estimate on public trades."""

        available = max(0.0, float(trade_qty_now) * float(deplete_mult))
        if available <= 0.0:
            return
        for order in orders:
            if order.get("state") not in (
                self.state.ORDER_OPEN,
                self.state.ORDER_PENDING_CANCEL,
            ):
                continue
            crosses = self.services._trade_crosses_order_tick(
                side,
                float(trade_price_now),
                float(order["price"]),
                self.state.TICK,
            )
            if not crosses:
                continue
            queue_left = max(
                0.0,
                float(
                    order.get(
                        "policy_queue_left",
                        order.get("queue_left", 0.0),
                    )
                    or 0.0
                ),
            )
            order["policy_queue_left"] = max(0.0, queue_left - available)
        for record in self.state.queue_value_open_records.values():
            if str(record.get("side", "")).upper() != side:
                continue
            order_price = float(record.get("order_price", 0.0) or 0.0)
            if order_price <= 0.0:
                continue
            crosses = self.services._trade_crosses_order_tick(
                side,
                float(trade_price_now),
                order_price,
                self.state.TICK,
            )
            if not crosses:
                continue
            shadow_left = max(
                0.0,
                float(
                    record.get(
                        "shadow_policy_queue_left",
                        record.get("queue_left", 0.0),
                    )
                    or 0.0
                ),
            )
            record["shadow_policy_queue_left"] = max(
                0.0,
                shadow_left - available,
            )

    def _enqueue_dynamic_fill_hazard_visible_trade(self,
        *,
        trade_ts_ms: int,
        trade_price_now: float,
        trade_qty_now: float,
    ) -> None:

        if self.state.dynamic_fill_hazard_visibility_scheduler is None:
            return
        self.state.dynamic_fill_hazard_visible_trade_ordinal += 1
        exchange_ns = int(trade_ts_ms) * 1_000_000
        ready_ns = self._dynamic_fill_hazard_feature_ready_ts_ns(
            exchange_ts_ns=exchange_ns,
            provider_receive_ts_ns=0,
            event_type="trade",
            source_ordinal=self.state.dynamic_fill_hazard_visible_trade_ordinal,
        )
        if ready_ns < exchange_ns:
            raise RuntimeError(
                "q90 trade feature-ready clock preceded exchange truth"
            )
        self.services.heapq.heappush(
            self.state.dynamic_fill_hazard_visible_trade_heap,
            (
                int(ready_ns),
                int(self.state.dynamic_fill_hazard_visible_trade_ordinal),
                exchange_ns,
                float(trade_price_now),
                float(trade_qty_now),
            ),
        )
        self.state.dynamic_fill_hazard_visible_trade_enqueued_count += 1

    def _record_exchange_book_same_price_trade(self,
        orders: list[dict],
        trade_price_now: float,
        trade_qty_now: float,
        trade_ts_ms: int,
    ) -> None:
        if (
            self.state.exchange_book_scheduler is None
            or trade_qty_now <= 0.0
        ):
            return
        trade_tick = self.services._price_to_tick(float(trade_price_now), self.state.TICK)
        for order in orders:
            if order.get("state") not in (
                self.state.ORDER_OPEN,
                self.state.ORDER_PENDING_CANCEL,
            ):
                continue
            if self.services._order_price_tick(order, self.state.TICK) != trade_tick:
                continue
            order["exchange_book_queue_trade_since_update"] = float(
                order.get(
                    "exchange_book_queue_trade_since_update",
                    0.0,
                )
                + float(trade_qty_now)
            )
        side = "BUY" if orders is self.state.bid_orders else "SELL"
        if self.state.dynamic_fill_hazard_visibility_scheduler is None:
            for path in self.state.dynamic_fill_hazard_paths.values():
                if (
                    path.side == side
                    and self.services._price_to_tick(float(path.price), self.state.TICK) == trade_tick
                ):
                    path.observe_trade(
                        float(trade_qty_now),
                        receive_ts_ns=int(trade_ts_ms) * 1_000_000,
                    )
            if self.state.dynamic_fill_hazard_cpp_runtime is not None and side == "BUY":
                self.state.dynamic_fill_hazard_cpp_runtime.observe_trade(
                    is_sell_trade=True,
                    trade_price=float(trade_price_now),
                    quantity=float(trade_qty_now),
                    receive_ts_ns=int(trade_ts_ms) * 1_000_000,
                )
        elif side == "BUY":
            self._enqueue_dynamic_fill_hazard_visible_trade(
                trade_ts_ms=int(trade_ts_ms),
                trade_price_now=float(trade_price_now),
                trade_qty_now=float(trade_qty_now),
            )
        for record in self.state.queue_value_open_records.values():
            if str(record.get("side", "")).upper() != side:
                continue
            record_price = float(record.get("order_price", 0.0) or 0.0)
            if record_price <= 0.0:
                continue
            if (
                self.services._price_to_tick(record_price, self.state.TICK)
                != trade_tick
            ):
                continue
            record["_exchange_book_trade_since_update_qty"] = float(
                record.get(
                    "_exchange_book_trade_since_update_qty",
                    0.0,
                )
                + float(trade_qty_now)
            )

    def _queue_value_native_runtime_target(self,
        side: str,
    ) -> Optional[dict]:
        episode_id = self.state.queue_value_active_episode_by_side.get(side)
        if episode_id is not None:
            record = self.state.queue_value_open_records.get(int(episode_id))
            if record is not None:
                return record
        orders = self.state.bid_orders if side == "BUY" else self.state.ask_orders
        return self._best_live_order(orders, side)

    def _apply_exchange_book_advance(self,
        advance,
        *,
        now_ts_ms: int,
        execution_trade_same_ms: bool,
    ) -> None:
        """Apply native exact-level changes to active queue mechanics."""


        if self.state.exchange_book_scheduler is None:
            return
        if self.state.dynamic_fill_hazard_visibility_scheduler is not None:
            for source_event in advance.source_events:
                if (
                    self.state.dynamic_fill_hazard_visibility_clock_mode
                    == "provider_receive_sensitivity"
                    and int(source_event.local_receive_ts_ns or 0) <= 0
                ):
                    self.state.dynamic_fill_hazard_provider_receive_missing_count += 1
                ready_ns = self._dynamic_fill_hazard_feature_ready_ts_ns(
                    exchange_ts_ns=int(source_event.exchange_ts_ns),
                    provider_receive_ts_ns=int(
                        source_event.local_receive_ts_ns or 0
                    ),
                    event_type="book",
                    source_ordinal=int(source_event.source_ordinal),
                )
                self.state.dynamic_fill_hazard_visibility_scheduler.enqueue(
                    source_event,
                    feature_ready_ts_ns=ready_ns,
                )
        if (
            self.state.dynamic_fill_hazard_cpp_runtime is not None
            and self.state.dynamic_fill_hazard_visibility_scheduler is None
        ):
            cpp_accepted = 0
            cpp_rejected = 0
            cpp_snapshot_reset = False
            cpp_invalidated = False
            for source_event in advance.source_events:
                same_ms_trade = bool(
                    execution_trade_same_ms
                    and int(source_event.exchange_ts_ns) // 1_000_000
                    == int(now_ts_ms)
                )
                cpp_result = (
                    self.state.dynamic_fill_hazard_cpp_runtime.apply_exchange_book_event(
                        source_event,
                        execution_trade_same_ms=same_ms_trade,
                    )
                )
                self.state.dynamic_fill_hazard_cpp_book_event_count += 1
                cpp_accepted += int(bool(cpp_result.get("accepted", False)))
                cpp_rejected += int(not bool(cpp_result.get("accepted", False)))
                cpp_snapshot_reset = bool(
                    cpp_snapshot_reset
                    or cpp_result.get("snapshot_reset", False)
                )
                cpp_invalidated = bool(
                    cpp_invalidated or cpp_result.get("invalidated", False)
                )
            self._dynamic_fill_hazard_cpp_check(
                cpp_accepted == int(advance.accepted_events)
                and cpp_rejected == int(advance.rejected_events)
                and cpp_snapshot_reset == bool(advance.snapshot_reset)
                and cpp_invalidated == bool(advance.invalidated),
                "native_book_advance",
                now_ts_ms=int(now_ts_ms),
                source_event_count=len(advance.source_events),
                python={
                    "accepted": int(advance.accepted_events),
                    "rejected": int(advance.rejected_events),
                    "snapshot_reset": bool(advance.snapshot_reset),
                    "invalidated": bool(advance.invalidated),
                },
                cpp={
                    "accepted": cpp_accepted,
                    "rejected": cpp_rejected,
                    "snapshot_reset": cpp_snapshot_reset,
                    "invalidated": cpp_invalidated,
                },
            )
        l2_orders_before_reset = None
        if self.state.l2_journal is not None and advance.snapshot_reset:
            l2_orders_before_reset = [
                {k: order.get(k) for k in
                 ('trace_id', 'side', 'state', 'remaining', 'exchange_remaining',
                  'queue_left', 'exchange_book_queue_path_valid',
                  'exchange_book_queue_invalidated_reason',
                  'cancel_request_ts', 'cancel_effective_ts', 'cancel_ts')}
                for order in (*self.state.bid_orders, *self.state.ask_orders)]
        if advance.snapshot_reset or advance.invalidated:
            reason = (
                "native_sequence_invalidated"
                if advance.invalidated
                else "native_snapshot_reset"
            )
            self._mark_exchange_book_path_invalid(
                self.state.bid_orders,
                reason=reason,
                event_ts_ms=int(now_ts_ms),
            )
            self._mark_exchange_book_path_invalid(
                self.state.ask_orders,
                reason=reason,
                event_ts_ms=int(now_ts_ms),
            )

        for source_event in advance.source_events:
            if source_event.event_type != 'snapshot':
                continue
            event = self.state.l2_production.observe('book_snapshot_boundary', now_ts_ms)
            if self.state.l2_journal is not None:
                self.state.l2_journal.emit('book_snapshot_boundary', now_ts_ms,
                    payload={"source": source_event.source, "source_ordinal": source_event.source_ordinal,
                             "source_event_ns": int(source_event.exchange_ts_ns),
                             "message_levels": len(source_event.levels),
                             "snapshot_reset_in_boundary": bool(advance.snapshot_reset),
                             "accepted_events_in_boundary": advance.accepted_events,
                             "rejected_events_in_boundary": advance.rejected_events,
                             "segment_after_boundary": self.state.exchange_book_scheduler.segment_id,
                             "own_orders_before_queue_invalidation": l2_orders_before_reset,
                             "own_orders_after_queue_invalidation": [
                                 {k: order.get(k) for k in
                                  ('trace_id', 'side', 'state', 'remaining', 'exchange_remaining',
                                   'queue_left', 'exchange_book_queue_path_valid',
                                   'exchange_book_queue_invalidated_reason',
                                   'cancel_request_ts', 'cancel_effective_ts', 'cancel_ts')}
                                 for order in (*self.state.bid_orders, *self.state.ask_orders)],
                             "scope": "whole_message_consumed_boundary_not_native_visibility"},
                    production_event=event)

        # A millisecond can contain several child trades (and a timer before
        # them). Inspect the whole execution batch, not merely the current
        # row. Only opposite aggressors reaching this price can race with its
        # book update; a trade at an unrelated level must not freeze its queue.
        same_ms_sell_min_tick = None
        same_ms_buy_max_tick = None
        if advance.level_changes:
            (
                same_ms_sell_min_tick,
                same_ms_buy_max_tick,
            ) = self._same_ms_execution_trade_tick_extremes(int(now_ts_ms))

        for change in advance.level_changes:
            change_ts_ms = int(change.exchange_ts_ns) // 1_000_000
            side = "BUY" if change.side == "bid" else "SELL"
            orders = self.state.bid_orders if side == "BUY" else self.state.ask_orders
            matching_orders = [
                order
                for order in orders
                if int(
                    order.get(
                        "exchange_book_queue_price_tick",
                        self.services._order_price_tick(order, self.state.TICK),
                    )
                )
                == int(change.price_tick)
                and order.get("state")
                in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
            ]
            same_ms_activation = any(
                int(order.get("activate_ts", -1)) == change_ts_ms
                for order in matching_orders
            )
            ambiguous = bool(
                (
                    change_ts_ms == int(now_ts_ms)
                    and (
                        (side == "BUY" and same_ms_sell_min_tick is not None
                         and same_ms_sell_min_tick <= int(change.price_tick))
                        or (side == "SELL" and same_ms_buy_max_tick is not None
                            and same_ms_buy_max_tick >= int(change.price_tick))
                    )
                )
                or same_ms_activation
            )
            decrease = max(
                0.0,
                float(change.quantity_before)
                - float(change.quantity_after),
            )
            increase = max(
                0.0,
                float(change.quantity_after)
                - float(change.quantity_before),
            )
            hazard_side = "BUY" if change.side == "bid" else "SELL"
            hazard_receive_ns = int(change.receive_ts_ns)
            if self.state.dynamic_fill_hazard_visibility_scheduler is None:
                # The optional historical hazard evaluator has a separate
                # coarse same-ms ABI shared with its C++ implementation.
                # Do not change that research model in a matching-queue fix.
                hazard_ambiguous = bool(
                    same_ms_activation
                    or (execution_trade_same_ms and change_ts_ms == int(now_ts_ms))
                )
                for hazard_path in self.state.dynamic_fill_hazard_paths.values():
                    if (
                        hazard_path.side == hazard_side
                        and self.services._price_to_tick(float(hazard_path.price), self.state.TICK)
                        == int(change.price_tick)
                    ):
                        hazard_path.observe_level_change(
                            quantity_before=float(change.quantity_before),
                            quantity_after=float(change.quantity_after),
                            generation=int(change.segment_id),
                            receive_ts_ns=hazard_receive_ns,
                            ambiguous=hazard_ambiguous,
                        )
            if (
                self.state.queue_value_keep_cancel_enabled
                and self.state.queue_value_runtime_event_source.get(side)
                == "native_exchange_exact_level"
                and not ambiguous
            ):
                runtime_target = self._queue_value_native_runtime_target(side)
                if runtime_target is not None:
                    target_tick = int(
                        round(
                            float(
                                runtime_target.get(
                                    "order_price",
                                    runtime_target.get("price", 0.0),
                                )
                                or 0.0
                            )
                            / float(self.state.TICK)
                        )
                    )
                    if target_tick == int(change.price_tick):
                        tracker_key = (
                            "_exchange_book_trade_since_update_qty"
                            if "order_price" in runtime_target
                            else "exchange_book_queue_trade_since_update"
                        )
                        explained_trade = min(
                            float(change.quantity_before),
                            max(
                                0.0,
                                float(
                                    runtime_target.get(
                                        tracker_key,
                                        0.0,
                                    )
                                    or 0.0
                                ),
                            ),
                        )
                        cancellation = max(
                            0.0,
                            decrease - explained_trade,
                        )
                        self.state.queue_value_runtime[side].observe(
                            int(change.exchange_ts_ns),
                            cancel=float(cancellation > 1e-12),
                            refill=float(increase > 1e-12),
                        )
                        self.state.queue_value_runtime_event_counts[side][
                            "cancel"
                        ] += int(cancellation > 1e-12)
                        self.state.queue_value_runtime_event_counts[side][
                            "refill"
                        ] += int(increase > 1e-12)
                        runtime_target[tracker_key] = 0.0

            for order in matching_orders:
                if ambiguous:
                    if not bool(order.get("exchange_book_queue_ambiguous", False)):
                        self._trace_exchange_book_queue_ambiguity(
                            order,
                            reason="same_ms_exchange_book_ambiguity",
                            event_ts_ms=int(change_ts_ms),
                            ambiguous=True,
                        )
                    order["exchange_book_queue_ambiguous"] = True
                    if bool(
                        order.get(
                            "exchange_book_queue_path_valid",
                            False,
                        )
                    ):
                        order["exchange_book_queue_path_valid"] = False
                        order["exact_queue_path_valid"] = False
                        order[
                            "exchange_book_queue_invalidated_reason"
                        ] = "same_ms_exchange_book_ambiguity"
                        self.state.exchange_book_queue_invalidated_order_count += 1
                    self.state.exchange_book_queue_ambiguous_event_count += 1
                    intervention_id = int(
                        order.get(
                            "queue_value_keep_cancel_intervention_id",
                            -1,
                        )
                        or -1
                    )
                    action_record = self.state.queue_value_open_records.get(
                        intervention_id
                    )
                    if action_record is not None:
                        action_record[
                            "exchange_book_queue_path_valid"
                        ] = 0
                        action_record[
                            "exchange_book_queue_ambiguous"
                        ] = 1
                        action_record[
                            "exchange_book_queue_invalidated"
                        ] = 1
                        action_record[
                            "exchange_book_queue_invalidated_reason"
                        ] = "same_ms_exchange_book_ambiguity"
                    continue
                # Exact cross-stream evidence remains invalid after a race,
                # but that does not stop the modeled physical queue. Resume
                # native updates in the same valid book segment; never revive
                # a missing seed, inherited unknown queue, or sequence gap.
                if not (
                    order.get("exchange_book_queue_path_valid", False)
                    or (
                        order.get("exchange_book_queue_invalidated_reason")
                        == "same_ms_exchange_book_ambiguity"
                        and order.get("simulator_queue_source") == "native_exchange_book"
                        and order.get("exchange_book_queue_segment_id") == change.segment_id
                    )
                ):
                    continue
                explained_trade = min(
                    float(change.quantity_before),
                    max(
                        0.0,
                        float(
                            order.get(
                                "exchange_book_queue_trade_since_update",
                                0.0,
                            )
                            or 0.0
                        ),
                    ),
                )
                cancellation = max(0.0, decrease - explained_trade)
                ahead = max(
                    0.0,
                    float(order.get("queue_left", 0.0) or 0.0),
                )
                public_after_trade = max(
                    0.0,
                    float(change.quantity_before) - explained_trade,
                )
                behind = max(0.0, public_after_trade - ahead)
                denominator = ahead + behind
                ahead_probability = (
                    ahead / denominator if denominator > 0.0 else 0.0
                )
                removed = min(
                    ahead,
                    cancellation * ahead_probability,
                )
                # A skipped ambiguous update cannot leave phantom public
                # quantity ahead after a later unambiguous level observation.
                # Clamp the value itself: reconstructing this bound through
                # subtraction can leave 0.0010000000000000009 ahead of a
                # 0.001 level and incorrectly suppress a later whole-lot fill.
                queue_after = min(
                    max(0.0, float(change.quantity_after)),
                    self.services.subtract_lot_quantity(ahead, removed, self.state.LOT_SIZE),
                )
                removed = ahead - queue_after
                if removed > 0.0:
                    order["queue_left"] = queue_after
                    self.state.exchange_book_queue_cancel_ahead_event_count += 1
                    self.state.exchange_book_queue_cancel_ahead_qty += removed
                order["exchange_book_queue_trade_since_update"] = 0.0

            for record in self.state.local_order_value_open_records.values():
                record_side = str(record["side"])
                record_tick = int(
                    self.services._price_to_tick(float(record["order_price"]), self.state.TICK)
                )
                if record_side != side or record_tick != int(
                    change.price_tick
                ):
                    continue
                record["exchange_book_event_count"] = float(
                    record["exchange_book_event_count"] + 1.0
                )
                if ambiguous:
                    record["exchange_book_ambiguous_event_count"] = float(
                        record["exchange_book_ambiguous_event_count"]
                        + 1.0
                    )
                    record["exchange_book_queue_path_valid"] = 0
                    record["exchange_book_queue_ambiguous"] = 1
                    record["exchange_book_queue_invalidated"] = 1
                    record[
                        "exchange_book_queue_invalidated_reason"
                    ] = "same_ms_exchange_book_ambiguity"
                    continue
                explained_trade = min(
                    float(change.quantity_before),
                    max(
                        0.0,
                        float(
                            record.get(
                                "_exchange_book_trade_since_update_qty",
                                0.0,
                            )
                            or 0.0
                        ),
                    ),
                )
                cancellation = max(0.0, decrease - explained_trade)
                if cancellation > 0.0:
                    record["exchange_book_cancel_count"] = float(
                        record["exchange_book_cancel_count"] + 1.0
                    )
                    record["exchange_book_cancel_qty"] = float(
                        record["exchange_book_cancel_qty"]
                        + cancellation
                    )
                if increase > 0.0:
                    record["exchange_book_refill_count"] = float(
                        record["exchange_book_refill_count"] + 1.0
                    )
                    record["exchange_book_refill_qty"] = float(
                        record["exchange_book_refill_qty"] + increase
                    )
                record["_exchange_book_trade_since_update_qty"] = 0.0

        if self.state.local_order_lifecycle.enabled:
            mid_changes, next_mid_change_cursor = (
                self.state.exchange_book_scheduler.mid_changes_since(
                    self.state.local_order_lifecycle_mid_change_cursor
                )
            )
            for change_ts_ns, mid_tick in mid_changes:
                same_ms_trade = bool(
                    execution_trade_same_ms
                    and int(change_ts_ns) // 1_000_000 == int(now_ts_ms)
                )
                self.state.local_order_lifecycle.native_mid(
                    int(change_ts_ns),
                    float(mid_tick) * float(self.state.TICK),
                    segment_id=int(self.state.exchange_book_scheduler.segment_id),
                    same_ms_ordering_resolved=not same_ms_trade,
                )
            self.state.local_order_lifecycle_mid_change_cursor = next_mid_change_cursor

    def _dynamic_fill_hazard_visible_levels(self) -> set[tuple[str, int]]:
        return {
            (
                "bid" if path.side == "BUY" else "ask",
                self.services._price_to_tick(float(path.price), self.state.TICK),
            )
            for path in self.state.dynamic_fill_hazard_paths.values()
        }

    def _apply_dynamic_fill_hazard_visible_book_advance(self,
        advance,
        *,
        visible_trade_same_ms: bool,
    ) -> None:

        if self.state.dynamic_fill_hazard_visibility_scheduler is None:
            return
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            cpp_accepted = 0
            cpp_rejected = 0
            cpp_snapshot_reset = False
            cpp_invalidated = False
            for source_event in advance.source_events:
                cpp_result = (
                    self.state.dynamic_fill_hazard_cpp_runtime.apply_exchange_book_event(
                        source_event,
                        feature_ready_ts_ns=int(
                            advance.feature_ready_ts_ns
                        ),
                        feature_source_ts_ns=(
                            self._dynamic_fill_hazard_policy_source_ts_ns(
                                exchange_ts_ns=int(
                                    source_event.exchange_ts_ns
                                ),
                                provider_receive_ts_ns=int(
                                    source_event.local_receive_ts_ns or 0
                                ),
                            )
                        ),
                        execution_trade_same_ms=bool(visible_trade_same_ms),
                    )
                )
                self.state.dynamic_fill_hazard_cpp_book_event_count += 1
                cpp_accepted += int(bool(cpp_result.get("accepted", False)))
                cpp_rejected += int(not bool(cpp_result.get("accepted", False)))
                cpp_snapshot_reset = bool(
                    cpp_snapshot_reset
                    or cpp_result.get("snapshot_reset", False)
                )
                cpp_invalidated = bool(
                    cpp_invalidated or cpp_result.get("invalidated", False)
                )
            self._dynamic_fill_hazard_cpp_check(
                cpp_accepted == int(advance.accepted_events)
                and cpp_rejected == int(advance.rejected_events)
                and cpp_snapshot_reset == bool(advance.snapshot_reset)
                and cpp_invalidated == bool(advance.invalidated),
                "visibility_book_advance",
                feature_ready_ts_ns=int(advance.exchange_ts_ns),
                source_event_count=len(advance.source_events),
                python={
                    "accepted": int(advance.accepted_events),
                    "rejected": int(advance.rejected_events),
                    "snapshot_reset": bool(advance.snapshot_reset),
                    "invalidated": bool(advance.invalidated),
                },
                cpp={
                    "accepted": cpp_accepted,
                    "rejected": cpp_rejected,
                    "snapshot_reset": cpp_snapshot_reset,
                    "invalidated": cpp_invalidated,
                },
            )
        if advance.snapshot_reset or advance.invalidated:
            reason = (
                "native_sequence_invalidated"
                if advance.invalidated
                else "native_snapshot_reset"
            )
            for path in self.state.dynamic_fill_hazard_paths.values():
                path.invalidate(reason)
        for change in advance.level_changes:
            if change.side != "bid":
                continue
            change_ready_ns = int(change.feature_ready_ts_ns)
            for path in self.state.dynamic_fill_hazard_paths.values():
                if (
                    path.side != "BUY"
                    or self.services._price_to_tick(float(path.price), self.state.TICK)
                    != int(change.price_tick)
                ):
                    continue
                same_ms_activation = bool(
                    int(path.activation_ts_ns) > 0
                    and int(path.activation_ts_ns) == change_ready_ns
                )
                path.observe_level_change(
                    quantity_before=float(change.quantity_before),
                    quantity_after=float(change.quantity_after),
                    generation=int(change.segment_id),
                    receive_ts_ns=(
                        self._dynamic_fill_hazard_policy_source_ts_ns(
                            exchange_ts_ns=int(change.exchange_ts_ns),
                            provider_receive_ts_ns=int(change.receive_ts_ns),
                        )
                    ),
                    feature_ready_ts_ns=change_ready_ns,
                    ambiguous=bool(
                        visible_trade_same_ms or same_ms_activation
                    ),
                )
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count += (
                self.services.synchronize_visibility_batch_ambiguity_to_cpp(
                    self.state.dynamic_fill_hazard_paths,
                    self.state.dynamic_fill_hazard_cpp_runtime,
                    self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_synced_ids,
                )
            )

    def _deliver_dynamic_fill_hazard_visible_trade(self,
        trade_event: tuple[int, int, int, float, float],
    ) -> None:

        ready_ns, _, exchange_ns, trade_price_now, trade_qty_now = trade_event
        trade_tick = self.services._price_to_tick(float(trade_price_now), self.state.TICK)
        for path in self.state.dynamic_fill_hazard_paths.values():
            if (
                path.side == "BUY"
                and self.services._price_to_tick(float(path.price), self.state.TICK) == trade_tick
            ):
                path.observe_trade(
                    float(trade_qty_now),
                    receive_ts_ns=int(exchange_ns),
                    feature_ready_ts_ns=int(ready_ns),
                )
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            self.state.dynamic_fill_hazard_cpp_runtime.observe_trade(
                is_sell_trade=True,
                trade_price=float(trade_price_now),
                quantity=float(trade_qty_now),
                receive_ts_ns=int(exchange_ns),
                feature_ready_ts_ns=int(ready_ns),
            )
        self.state.dynamic_fill_hazard_visible_trade_delivered_count += 1

    def _advance_dynamic_fill_hazard_visibility(self,
        target_ts_ns: int,
        *,
        inclusive: bool,
    ) -> None:

        scheduler = self.state.dynamic_fill_hazard_visibility_scheduler
        if scheduler is None:
            return
        target = int(target_ts_ns)
        while True:
            book_ready = scheduler.next_feature_ready_ts_ns
            trade_ready = (
                int(self.state.dynamic_fill_hazard_visible_trade_heap[0][0])
                if self.state.dynamic_fill_hazard_visible_trade_heap
                else None
            )
            candidates = [
                value for value in (book_ready, trade_ready) if value is not None
            ]
            if not candidates:
                break
            next_ready = min(candidates)
            if next_ready > target or (next_ready == target and not inclusive):
                break
            same_ready = bool(
                book_ready is not None
                and trade_ready is not None
                and int(book_ready) == int(trade_ready)
            )
            if book_ready is not None and (
                trade_ready is None or int(book_ready) <= int(trade_ready)
            ):
                if same_ready:
                    self.state.dynamic_fill_hazard_visible_book_trade_tie_count += 1
                advance = scheduler.advance_to(
                    int(book_ready),
                    inclusive=True,
                    emitted_levels=self._dynamic_fill_hazard_visible_levels(),
                )
                self._apply_dynamic_fill_hazard_visible_book_advance(
                    advance,
                    visible_trade_same_ms=same_ready,
                )
                continue
            trade_event = self.services.heapq.heappop(
                self.state.dynamic_fill_hazard_visible_trade_heap
            )
            self._deliver_dynamic_fill_hazard_visible_trade(trade_event)

        boundary = int(scheduler.boundary_feature_ready_ts_ns)
        if target > boundary or (
            target == boundary
            and inclusive
            and not scheduler.boundary_inclusive
        ):
            advance = scheduler.advance_to(
                target,
                inclusive=bool(inclusive),
                emitted_levels=self._dynamic_fill_hazard_visible_levels(),
            )
            self._apply_dynamic_fill_hazard_visible_book_advance(
                advance,
                visible_trade_same_ms=False,
            )

    def _exchange_book_emitted_levels(self) -> set[tuple[str, int]]:
        """Select notification levels without changing native book state."""

        levels: set[tuple[str, int]] = set()
        for native_side, orders in (
            ("bid", self.state.bid_orders),
            ("ask", self.state.ask_orders),
        ):
            for order in orders:
                if order.get("state") not in (
                    self.state.ORDER_PENDING_NEW,
                    self.state.ORDER_OPEN,
                    self.state.ORDER_PENDING_CANCEL,
                ):
                    continue
                levels.add(
                    (
                        native_side,
                        int(
                            order.get(
                                "exchange_book_queue_price_tick",
                                round(
                                    float(order["price"])
                                    / float(self.state.TICK)
                                ),
                            )
                        ),
                    )
                )
        for records in (
            self.state.local_order_value_open_records,
            self.state.queue_value_open_records,
        ):
            for record in records.values():
                price = float(record.get("order_price", 0.0) or 0.0)
                if price <= 0.0:
                    continue
                native_side = (
                    "bid"
                    if str(record.get("side", "")).upper() == "BUY"
                    else "ask"
                )
                levels.add(
                    (
                        native_side,
                        self.services._price_to_tick(float(price), self.state.TICK),
                    )
                )
        for path in self.state.dynamic_fill_hazard_paths.values():
            levels.add(
                (
                    "bid" if path.side == "BUY" else "ask",
                    self.services._price_to_tick(float(path.price), self.state.TICK),
                )
            )
        return levels

    def _pending_exchange_book_lifecycle_boundaries(self,
        target_ts_ms: int,
    ) -> list[int]:
        """Return scheduled order transitions before the next outer event."""

        if self.state.exchange_book_scheduler is None:
            return []
        lower_ns = int(self.state.exchange_book_scheduler.boundary_exchange_ts_ns)
        target = int(target_ts_ms)
        boundaries: set[int] = set()
        for order in (*self.state.bid_orders, *self.state.ask_orders):
            state = order.get("state")
            candidates: list[int] = []
            if state == self.state.ORDER_PENDING_NEW:
                activate_ts = int(order.get("activate_ts", 0) or 0)
                if activate_ts > 0:
                    candidates.append(activate_ts)
                cancel_ts = int(order.get("cancel_ts", 0) or 0)
                if cancel_ts > 0:
                    candidates.append(cancel_ts)
                cancel_effective_ts = self._order_cancel_effective_ts(order)
                if cancel_effective_ts > 0:
                    candidates.append(cancel_effective_ts)
            elif state in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL):
                cancel_ts = int(order.get("cancel_ts", 0) or 0)
                if cancel_ts > 0:
                    candidates.append(cancel_ts)
                cancel_effective_ts = self._order_cancel_effective_ts(order)
                if cancel_effective_ts > 0:
                    candidates.append(cancel_effective_ts)
            for boundary_ms in candidates:
                boundary_ns = int(boundary_ms) * 1_000_000
                if lower_ns < boundary_ns < target * 1_000_000:
                    boundaries.add(int(boundary_ms))
        return sorted(boundaries)

    def _advance_exchange_book_through_order_lifecycle(self,
        target_ts_ms: int,
        fallback_mid: float,
    ) -> None:
        """Merge native book state with activation/ACK boundaries.

        These boundaries update only simulator order state. They do not run
        quote policy, guards, TTL initiation, or any other strategy decision.
        """

        def apply_boundary(
            boundary_ms: int,
            events: tuple[dict[str, Any], ...] = (),
        ) -> None:
            self._accrue_local_path_to(int(boundary_ms), float(fallback_mid))
            if self.state.exchange_book_scheduler is not None:
                before = self.state.exchange_book_scheduler.advance_to(
                    boundary_ms * 1_000_000,
                    inclusive=False,
                    emitted_levels=self._exchange_book_emitted_levels(),
                )
                self._apply_exchange_book_advance(
                    before,
                    now_ts_ms=boundary_ms,
                    execution_trade_same_ms=False,
                )
                self._advance_dynamic_fill_hazard_visibility(
                    boundary_ms * 1_000_000,
                    inclusive=False,
                )
                self._mark_cancel_ack_exchange_book_ambiguity(
                    self.state.bid_orders,
                    "BUY",
                    boundary_ms,
                )
                self._mark_cancel_ack_exchange_book_ambiguity(
                    self.state.ask_orders,
                    "SELL",
                    boundary_ms,
                )
            private_fill_events = tuple(
                event
                for event in events
                if str(event.get("event_id", "")).startswith("private-fill:")
            )
            self._process_order_transitions(
                self.state.bid_orders,
                "BUY",
                boundary_ms,
                fallback_mid,
                allow_ttl_initiation=False,
                defer_all_cancel_ack_at_ts=bool(private_fill_events),
            )
            self._process_order_transitions(
                self.state.ask_orders,
                "SELL",
                boundary_ms,
                fallback_mid,
                allow_ttl_initiation=False,
                defer_all_cancel_ack_at_ts=bool(private_fill_events),
            )
            for event in private_fill_events:
                payload = dict(event.get("payload") or {})
                q_before_visible = float(self.state.q)
                _, filled_buy_now, filled_sell_now = self._publish_passive_fill(
                    visibility_ts_ms=int(boundary_ms),
                    exchange_reserved=True,
                    **payload,
                )
                self._apply_visible_fill_inventory_state(
                    q_before_visible=float(q_before_visible),
                    visibility_ts_ms=int(boundary_ms),
                    filled_buy=bool(filled_buy_now),
                    filled_sell=bool(filled_sell_now),
                )
                payload_order = payload.get("order")
                if (
                    isinstance(payload_order, dict)
                    and float(payload_order.get("remaining", 0.0)) < self.state.LOT_SIZE
                ):
                    if payload_order in self.state.bid_orders:
                        self.state.bid_orders.remove(payload_order)
                    if payload_order in self.state.ask_orders:
                        self.state.ask_orders.remove(payload_order)
            # IOC activation reserves the physical fill here, even when the
            # next market/timer row comes much later. Its scheduled terminal
            # boundary revisits the same order and publishes local economics.
            boundary_trade_idx = max(
                0, int(self.services.np.searchsorted(self.state.trade_ts, boundary_ms, side="right")) - 1,
            )
            self._process_circuit_breaker_ioc_orders(
                self.state.bid_orders, "BUY", boundary_ms, boundary_trade_idx, fallback_mid,
            )
            self._process_circuit_breaker_ioc_orders(
                self.state.ask_orders, "SELL", boundary_ms, boundary_trade_idx, fallback_mid,
            )
            if private_fill_events:
                self._process_order_transitions(
                    self.state.bid_orders,
                    "BUY",
                    boundary_ms,
                    fallback_mid,
                    allow_ttl_initiation=False,
                )
                self._process_order_transitions(
                    self.state.ask_orders,
                    "SELL",
                    boundary_ms,
                    fallback_mid,
                    allow_ttl_initiation=False,
                )
            for event in events:
                payload = event.get("payload") or {}
                if "serial_rest_plan" in payload:
                    self._continue_serial_rest_decision(
                        payload["serial_rest_plan"], int(boundary_ms),
                        payload.get("completed"),
                    )
                if payload.get("async_rest_worker", False):
                    self._advance_async_rest_worker(int(boundary_ms), payload.get("completed"))
            if self.state.exchange_book_scheduler is not None:
                at_boundary = self.state.exchange_book_scheduler.advance_to(
                    boundary_ms * 1_000_000,
                    inclusive=True,
                    emitted_levels=self._exchange_book_emitted_levels(),
                )
                self._apply_exchange_book_advance(
                    at_boundary,
                    now_ts_ms=boundary_ms,
                    execution_trade_same_ms=False,
                )
                self._advance_dynamic_fill_hazard_visibility(
                    boundary_ms * 1_000_000,
                    inclusive=True,
                )

        def register_local_boundaries() -> None:
            for order in (*self.state.bid_orders, *self.state.ask_orders):
                order_id = str(order.get("trace_id", ""))
                state = order.get("state")
                if state == self.state.ORDER_PENDING_NEW:
                    activate_ts = int(order.get("activate_ts", 0) or 0)
                    if 0 < activate_ts < self.state.async_rest_unset_ms:
                        self.state.local_lifecycle_boundary_scheduler.schedule(
                            ts_ms=activate_ts,
                            phase="exchange_effective",
                            event_id=f"order:{order_id}:activate:{activate_ts}",
                        )
                    new_ack_ts = int(order.get("new_ack_ts", 0) or 0)
                    if 0 < new_ack_ts < self.state.async_rest_unset_ms:
                        self.state.local_lifecycle_boundary_scheduler.schedule(
                            ts_ms=new_ack_ts,
                            phase="new_ack",
                            event_id=f"order:{order_id}:new-ack:{new_ack_ts}",
                        )
                cancel_effective_ts = self._order_cancel_effective_ts(order)
                if 0 < cancel_effective_ts < self.state.async_rest_unset_ms:
                    self.state.local_lifecycle_boundary_scheduler.schedule(
                        ts_ms=cancel_effective_ts,
                        phase="exchange_effective",
                        event_id=(
                            f"order:{order_id}:cancel-effective:"
                            f"{cancel_effective_ts}"
                        ),
                    )
                cancel_ack_ts = int(order.get("cancel_ts", 0) or 0)
                if 0 < cancel_ack_ts < self.state.async_rest_unset_ms:
                    self.state.local_lifecycle_boundary_scheduler.schedule(
                        ts_ms=cancel_ack_ts,
                        phase="cancel_ack",
                        event_id=f"order:{order_id}:cancel-ack:{cancel_ack_ts}",
                    )
        if self.state.local_lifecycle_boundary_scheduler is not None:
            while True:
                # A fill callback or HTTP return can add a new cancel, request,
                # or activation before the next market event. Drain one exact
                # boundary, then discover those newly scheduled transitions.
                register_local_boundaries()
                boundary_ms = self.state.local_lifecycle_boundary_scheduler.next_timestamp()
                if boundary_ms is None or boundary_ms > int(target_ts_ms):
                    break
                events = self.state.local_lifecycle_boundary_scheduler.drain(
                    through_ts_ms=boundary_ms, inclusive=True,
                )
                apply_boundary(boundary_ms, tuple(events))
            return

        if self.state.exchange_book_scheduler is None:
            return
        while True:
            boundaries = self._pending_exchange_book_lifecycle_boundaries(
                int(target_ts_ms)
            )
            if not boundaries:
                return
            boundary_ms = int(boundaries[0])
            apply_boundary(boundary_ms)
