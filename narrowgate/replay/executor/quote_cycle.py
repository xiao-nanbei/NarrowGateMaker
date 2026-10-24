"""Replay quote cycle owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Optional


class QuoteCycle:
    def _recent_local_rank(self, now_ts: int, mid_px: float) -> tuple[float, float, float]:
        if not self.state.local_extreme_guard_enabled or mid_px <= 0.0:
            return 0.5, mid_px, mid_px
        left = self.services.np.searchsorted(self.state.trade_ts, now_ts - self.state.local_extreme_window_ms, side="left")
        right = self.services.np.searchsorted(self.state.trade_ts, now_ts, side="right")
        if right <= left:
            return 0.5, mid_px, mid_px
        window = self.state.trade_price[left:right]
        local_low = float(window.min())
        local_high = float(window.max())
        span = local_high - local_low
        if span <= self.state.TICK:
            return 0.5, local_low, local_high
        rank = (mid_px - local_low) / span
        return float(max(0.0, min(1.0, rank))), local_low, local_high

    def _apply_local_extreme_guard(self, quote_context: dict, quote_flags: dict,
                                   now_ts: int, mid_px: float) -> tuple[bool, bool]:
        rank, local_low, local_high = self._recent_local_rank(now_ts, mid_px)
        result = self.services.apply_local_extreme_guard_context(
            quote_context,
            mid_px=mid_px,
            rank=rank,
            local_low=local_low,
            local_high=local_high,
            cfg=self.state.local_extreme_cfg,
        )
        if result.fragile_ttl_active:
            quote_flags["fragile_order_ttl"] = True
        if result.bid_active or result.ask_active:
            quote_flags["local_extreme_guard"] = True
            self.state.local_extreme_guard_count += int(result.bid_active) + int(result.ask_active)
            self.state.bid_local_extreme_guard_count += int(result.bid_active)
            self.state.ask_local_extreme_guard_count += int(result.ask_active)
            if self.state.local_extreme_pause:
                self.state.local_extreme_pause_count += int(result.bid_active) + int(result.ask_active)
        return result.bid_active, result.ask_active

    def _apply_replay_side_policy_price(self, side: str, price: float, quote_mid: float,
                                        spread_mult: float) -> float:
        if quote_mid <= 0.0 or abs(spread_mult - 1.0) <= 1e-12:
            return price
        spread_mult = max(0.05, float(spread_mult))
        if side == "BUY":
            dist = max(quote_mid - price, self.state.TICK)
            adj = self.services.math.floor((quote_mid - dist * spread_mult) / self.state.TICK) * self.state.TICK
            return min(adj, quote_mid - self.state.TICK)
        dist = max(price - quote_mid, self.state.TICK)
        adj = self.services.math.ceil((quote_mid + dist * spread_mult) / self.state.TICK) * self.state.TICK
        return max(adj, quote_mid + self.state.TICK)

    def _replay_common_side_policy(self,
        side: str,
        quote_ctx: dict,
        *,
        mutate_state: bool = True,
    ):
        markout_ema = self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask
        toxicity = self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask
        exposure_increasing = self.state.q >= 0.0 if side == "BUY" else self.state.q <= 0.0
        cooldown_active = self._fill_cooldown_active(
            side,
            int(self.state.t),
            mutate_state=mutate_state,
        )
        if getattr(self.state, 'cooldown_rearm_runtime', None) is not None:
            cooldown_active = self._cooldown_rearm_admission(
                side, cooldown_active, mutate_state=mutate_state)
        role_generic_enabled = self.context.generic_markout_enabled
        if self.context.inventory_markout_scope == "exclude_pure_reducing" or self.context.inventory_cooldown_mode != "original":
            # Only policy-visible remaining quantity; never exchange_remaining
            # or exchange_inventory ahead of a private fill notification.
            role = "unknown"
            quantity = None
            try:
                sizes = self.services.pre_generic_sizes(order_size=self.state.order_size,
                    lot=self.state.LOT_SIZE, inventory=self.state.q,
                    max_inventory=self.state.max_inv, eta=self.state.eta,
                    symmetric=self.state.symmetric_size)
                quantity = sizes[0 if side == "BUY" else 1]
                pending = tuple(self.services.PendingExposure(self._ranked_guard_order_id(order),
                    str(order["side"]), float(order["remaining"]))
                    for order in (*self.state.bid_orders, *self.state.ask_orders)
                    if float(order["remaining"]) > 0)
                role = self.services.pure_reducing_role(side=side, inventory=self.state.q,
                    quantity=quantity, pending_orders=pending, snapshot_known=True)
            except (KeyError, TypeError, ValueError, OverflowError):
                pass  # Unknown denies only the new exemption, not the order.
            # Cooldown-only cells record the same role diagnostic, but retain
            # their original generic-markout decision without an exemption.
            if self.context.inventory_markout_scope == "exclude_pure_reducing":
                role_generic_enabled = self.services.generic_markout_enabled_for_role(self.context.inventory_markout_scope, role)
            quote_ctx.update(pre_generic_role=role, pre_generic_quantity=quantity,
                role_scoped_generic_markout_enabled=role_generic_enabled)
        quote_ctx["policy_markout"] = bool(
            role_generic_enabled and markout_ema < 0.0 and self.state.markout_spread_scale > 0.0)
        near_depth = float(
            quote_ctx.get(
                "l2_near_depth_total",
                quote_ctx.get("near_depth_total", 0.0),
            )
            or 0.0
        )
        policy_input = self.services.CommonSidePolicyInput(
                exposure_increasing=exposure_increasing,
                fill_cooldown_active=self.services.inventory_cooldown_blocks_action(
                    cooldown_active, self.context.inventory_cooldown_mode),
                inventory_ratio=min(abs(self.state.q) / max(self.state.max_inv, 1e-9), 1.0),
                depth_age_s=float(quote_ctx.get("depth_age_s", self.services.math.inf)),
                max_book_age_s=float(self.state.max_exec_book_age_ms) / 1000.0,
                toxicity=toxicity,
                markout_ema=markout_ema,
                markout_spread_scale=self.state.markout_spread_scale,
                markout_reference=self.state.mo_ref,
                weighted_mid_proxy_shift_bps=float(quote_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0),
                l2_quote_flip_rate=float(quote_ctx.get("l2_quote_flip_rate", 0.0) or 0.0),
                l2_book_cancel_ratio=float(quote_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0),
                l2_near_depth_total=near_depth,
                thin_depth_threshold=self.state.thin_depth_threshold,
                depth_liquidity_baseline=self.state.depth_liquidity_baseline,
                side_adverse=bool(quote_ctx.get("side_adverse", False) or quote_ctx.get("bid_adverse", False)),
                side_adverse_pause=bool(quote_ctx.get("side_adverse_pause", False)),
                local_extreme_guard=bool(quote_ctx.get("local_extreme_guard", False)),
                local_extreme_spread_mult=float(quote_ctx.get("local_extreme_spread_mult", 1.0) or 1.0),
                local_extreme_pause=bool(quote_ctx.get("local_extreme_pause", False)),
                defense_guard=bool(quote_ctx.get("defense_guard", False)),
                defense_spread_mult=float(quote_ctx.get("defense_spread_mult", 1.0) or 1.0),
                defense_pause=bool(quote_ctx.get("defense_pause", False)),
            )
        result = self.services.evaluate_common_side_policy(
            policy_input,
            generic_markout_enabled=role_generic_enabled,
        )
        if not role_generic_enabled and self.context.inventory_markout_scope == "exclude_pure_reducing":
            rounded = self.services.rounded_release_quantity(quantity=quantity,
                size_mult=result.size_mult, lot=self.state.LOT_SIZE,
                side=side, inventory=self.state.q, pending_orders=pending)
            quote_ctx["release_rounded_quantity"] = rounded
            if rounded is None:
                # Restore the original composition before any price/size is
                # consumed, rather than introducing a new order veto.
                result = self.services.evaluate_common_side_policy(policy_input, generic_markout_enabled=True)
                quote_ctx["role_scoped_generic_markout_enabled"] = True
                quote_ctx["policy_markout"] = bool(
                    markout_ema < 0.0 and self.state.markout_spread_scale > 0.0)
        return result, result.spread_mult

    def _refresh_final_quote_context(self, quote_context: dict, quote_flags: dict,
                                     quote_mid: float, fair_px: float,
                                     bid_px: float, ask_px: float,
                                     *, post_policy_cap: bool = False):
        pair_spread = max(ask_px - bid_px, self.state.TICK)
        final_quote_skew = (
            ((ask_px - quote_mid) - (quote_mid - bid_px)) / pair_spread
            if pair_spread > 1e-12 else 0.0
        )
        final_mid_shift = 0.5 * (bid_px + ask_px) - fair_px
        final_bias_side = "balanced"
        if final_mid_shift > self.state.TICK:
            final_bias_side = "BUY"
        elif final_mid_shift < -self.state.TICK:
            final_bias_side = "SELL"
        quote_context["BUY"].update({
            "final_pair_spread": pair_spread,
            "final_price": bid_px,
            "final_quote_delta_to_bbo": self.state.cur_best_bid - bid_px if self.state.cur_best_bid > 0.0 else 0.0,
            "final_distance_to_mid": quote_mid - bid_px,
            "final_quote_skew": final_quote_skew,
            "final_bias_side": final_bias_side,
        })
        quote_context["SELL"].update({
            "final_pair_spread": pair_spread,
            "final_price": ask_px,
            "final_quote_delta_to_bbo": ask_px - self.state.cur_best_ask if self.state.cur_best_ask > 0.0 else 0.0,
            "final_distance_to_mid": ask_px - quote_mid,
            "final_quote_skew": final_quote_skew,
            "final_bias_side": final_bias_side,
        })
        if post_policy_cap:
            quote_flags["final_compressed"] = True
            quote_flags["post_policy_cap"] = True
            quote_flags["side_policy_cap"] = True
            for side_name in ("BUY", "SELL"):
                quote_context[side_name]["post_policy_cap"] = True
                quote_context[side_name]["final_compressed"] = True
                quote_context[side_name]["any_constraint_changed"] = True

    def _inventory_lifecycle_repair_at_quote(self,
        now_ms: int,
        quote_context: dict,
    ) -> tuple[float, float, float, Optional[dict]]:
        """Score repair from the candidate path using decision-time state only."""

        policy_now_ns = int(now_ms) * 1_000_000
        repair_probability = self.services.math.nan
        repair_probability_change = self.services.math.nan
        repair_probability_age_ms = self.services.math.inf
        repair_features = None
        if self.context.inventory_lifecycle_repair_model is not None and self.state.inventory_lifecycle_active and abs(self.state.q) > 1e-12:
            current_inventory_lifecycle_side = self.services.inventory_lifecycle_side(self.state.q)
            add_side = "BUY" if current_inventory_lifecycle_side == "LONG" else "SELL"
            add_ctx = quote_context[add_side]
            add_markout_ema = self.state.mo_ema_bid if add_side == "BUY" else self.state.mo_ema_ask
            add_toxicity = self.state.cur_tox_bid if add_side == "BUY" else self.state.cur_tox_ask
            side_quote_fill_probability = float(
                add_ctx.get(
                    "bid_quote_fill_prob" if add_side == "BUY" else "ask_quote_fill_prob",
                    0.0,
                )
                or 0.0
            )
            side_quote_markout_30s = float(
                add_ctx.get(
                    "bid_quote_fill_markout_30s"
                    if add_side == "BUY"
                    else "ask_quote_fill_markout_30s",
                    0.0,
                )
                or 0.0
            )
            repair_features = self.services.build_inventory_lifecycle_repair_features(
                inventory=float(self.state.q),
                order_size=float(self.state.order_size),
                max_inventory=float(self.state.max_inv),
                inventory_lifecycle_age_s=max(
                    0.0, float(now_ms - self.state.inventory_lifecycle_start_ts) / 1000.0
                ),
                inventory_lifecycle_max_abs_qty_so_far=float(self.state.inventory_lifecycle_cur_max_abs),
                inventory_lifecycle_pnl_so_far=float(self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl),
                inventory_lifecycle_adverse_excursion_so_far=float(self.state.inventory_lifecycle_cur_min_pnl),
                inventory_lifecycle_exposure_increasing_fills_so_far=int(
                    self.state.inventory_lifecycle_exposure_increasing_fills
                ),
                inventory_lifecycle_reducing_fills_so_far=int(self.state.inventory_lifecycle_reducing_fills),
                l2_book_refresh_ratio=float(
                    add_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
                ),
                l2_book_cancel_ratio=float(
                    add_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0
                ),
                l2_quote_flip_rate=float(
                    add_ctx.get("l2_quote_flip_rate", 0.0) or 0.0
                ),
                near_depth_total=float(
                    add_ctx.get(
                        "l2_near_depth_total",
                        add_ctx.get("near_depth_total", 0.0),
                    )
                    or 0.0
                ),
                weighted_mid_proxy_shift_bps=float(
                    add_ctx.get("weighted_mid_proxy_shift_bps", 0.0) or 0.0
                ),
                toxicity=float(add_toxicity),
                markout_ema=float(add_markout_ema),
                side_quote_fill_probability=side_quote_fill_probability,
                side_quote_markout_30s=side_quote_markout_30s,
            )
            raw_probability = float(
                self.context.inventory_lifecycle_repair_model.score(float(self.state.q), repair_features)
            )
            repair_probability, repair_probability_change = (
                self.state.inventory_lifecycle_repair_history.update(
                    inventory_lifecycle_id=int(self.state.inventory_lifecycle_count),
                    ts_ns=policy_now_ns,
                    probability=raw_probability,
                    lookback_ms=self.state.multi_market_policy_cfg.repair_lookback_ms,
                )
            )
            repair_probability_age_ms = 0.0
            if (
                self.state.trace_inventory_lifecycle_repair is not None
                and len(self.state.trace_inventory_lifecycle_repair) < self.state.trace_inventory_lifecycle_repair_max
            ):
                self.state.trace_inventory_lifecycle_repair.append(
                    {
                        "timestamp": float(now_ms) / 1000.0,
                        "ts_ns": policy_now_ns,
                        "inventory_lifecycle_id": int(self.state.inventory_lifecycle_count),
                        "inventory_lifecycle_side": self.services.inventory_lifecycle_side,
                        "inventory": float(self.state.q),
                        "repair_probability": repair_probability,
                        "repair_probability_change": repair_probability_change,
                        "model_id": str(
                            getattr(self.context.inventory_lifecycle_repair_model, "model_id", "")
                        ),
                        "training_end_day": str(
                            getattr(
                                self.context.inventory_lifecycle_repair_model,
                                "training_end_day",
                                "",
                            )
                        ),
                        **repair_features,
                    }
                )
        elif self.state.inventory_lifecycle_repair_cursor is not None:
            (
                repair_probability,
                repair_probability_change,
                repair_probability_age_ms,
            ) = self.state.inventory_lifecycle_repair_cursor.asof(
                policy_now_ns,
                lookback_ms=self.state.multi_market_policy_cfg.repair_lookback_ms,
                max_age_ms=self.state.multi_market_policy_cfg.repair_max_age_ms,
            )
        elif not self.state.inventory_lifecycle_active:
            self.state.inventory_lifecycle_repair_history.reset()
        return (
            repair_probability,
            repair_probability_change,
            repair_probability_age_ms,
            repair_features,
        )

    def _publish_local_new_ack(self, order: dict, side: str, visibility_ts: int) -> None:
        if bool(order.get("local_new_ack_published", False)):
            return
        if not bool(order.get("exchange_accepted", False)):
            raise RuntimeError("local NEW ACK preceded exchange acceptance")
        order["local_new_ack_published"] = True
        order["state"] = (
            self.state.ORDER_PENDING_CANCEL
            if int(order.get("cancel_ts", -1) or -1) > int(visibility_ts)
            else self.state.ORDER_OPEN
        )
        local_consumer_ts = (
            int(visibility_ts)
            if self.state.new_order_latency_split_enabled
            else int(order["activate_ts"])
        )
        if self.state.dynamic_fill_hazard_action_enabled and side == "BUY":
            hazard_lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(
                str(order["trace_id"])
            )
            if hazard_lifecycle is None:
                raise RuntimeError("q90 replay lifecycle missing at activation")
            activation_ns = int(order["activate_ts"]) * 1_000_000
            hazard_lifecycle.activate(
                int(local_consumer_ts) * 1_000_000,
                exchange_ts_ns=activation_ns,
            )
            self._append_dynamic_fill_hazard_lifecycle_journal(order, "activate")
        ack_book = order.get("_new_ack_policy_book") or {}
        self._observe_local_order_value_order(
            order,
            side,
            int(local_consumer_ts),
            best_bid=float(ack_book.get("best_bid", 0.0) or 0.0),
            best_ask=float(ack_book.get("best_ask", 0.0) or 0.0),
            bid_qty=float(ack_book.get("bid_qty", 0.0) or 0.0),
            ask_qty=float(ack_book.get("ask_qty", 0.0) or 0.0),
        )
        self._record_order_lifecycle_activate(
            order,
            visibility_ts_ms=int(visibility_ts),
            exchange_ts_ms=int(order["activate_ts"]),
            mid=float(order.get("_new_ack_policy_mid", 0.0) or 0.0),
        )
        self._ranked_guard_order_activated(
            order,
            visibility_ts_ms=int(visibility_ts),
            exchange_ts_ms=int(order["activate_ts"]),
        )

    def _process_order_transitions(self,
        orders,
        side: str,
        now_ts: int,
        fallback_mid: float,
        *,
        allow_ttl_initiation: bool = True,
        defer_cancel_ack_at_ts: bool = False,
        defer_all_cancel_ack_at_ts: bool = False,
    ):
        idx = 0
        while idx < len(orders):
            order = orders[idx]

            ttl_ms = int(order.get("ttl_ms", 0) or 0)
            if (
                allow_ttl_initiation
                and (
                    not self.state.main_loop_enabled
                    or (self.state.is_main_loop_wake and (
                        self.state.quote_schedule is not None
                        or int(now_ts) - int(self.state.last_rq_ts) >= self.state.cur_rq_ms
                    ))
                )
                and ttl_ms > 0
                and order["state"] in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
                and order["cancel_ts"] <= 0
                and now_ts - order["quote_ts"] >= ttl_ms
            ):
                request_start_ts, effective_ts, ack_ts = (
                    self._sample_cancel_deadlines_ms(
                    request_ts=int(now_ts),
                    side=str(order.get("side", side)),
                    operation=self.services._LATENCY_TTL_CANCEL,
                    order_ts=int(order.get("quote_ts", order.get("submit_ts", 0)) or 0),
                    )
                )
                order["cancel_effective_ts"] = int(effective_ts)
                order["cancel_ts"] = int(ack_ts)
                order["cancel_request_ts"] = int(request_start_ts)
                order["cancel_reason"] = "fragile_ttl"
                if order["state"] == self.state.ORDER_OPEN:
                    order["state"] = self.state.ORDER_PENDING_CANCEL
                if self.state.async_rest_gateway:
                    self.state.rest_gateway_last_timing["order"] = order
                    order["cancel_rest_return_ts"] = self.state.async_rest_unset_ms
                else:
                    self._record_order_lifecycle_cancel_request(
                        order, int(request_start_ts), reason="fragile_ttl",
                    )
                self._ranked_guard_cancel_requested(
                    order,
                    visibility_ts_ms=int(now_ts if self.state.async_rest_gateway else request_start_ts),
                    reason="fragile_ttl",
                )
                self.state.fragile_ttl_cancel_count += 1

            cancel_effective_ts = self._order_cancel_effective_ts(order)
            cancel_effective_now = bool(
                cancel_effective_ts > 0
                and cancel_effective_ts <= int(now_ts)
                and not (
                    defer_cancel_ack_at_ts
                    and cancel_effective_ts == int(now_ts)
                    and int(order.get("cancel_ts", -1) or -1)
                    == cancel_effective_ts
                )
            )
            if cancel_effective_now:
                if self.state.serial_rest_return_enabled and order.get("exchange_fill_terminal", False):
                    # Matching won the race. Retain the request/HTTP return,
                    # but do not invent CANCELED while its fill is in flight.
                    order["cancel_terminal_suppressed_full_fill"] = True
                else:
                    order["exchange_cancel_effective"] = True
                order["fill_eligible"] = False

            # Local NEW visibility precedes a same-millisecond cancel ACK.
            # Exchange-effective cancellation above still wins fillability at
            # the boundary, while the local consumer observes NEW before the
            # terminal cancel notification.
            if (
                order["state"] == self.state.ORDER_PENDING_NEW
                and bool(order.get("exchange_accepted", False))
                and order.get("time_in_force") != "IOC"
                and not bool(order.get("local_new_ack_published", False))
                and int(order.get("new_ack_ts", 0) or 0) <= int(now_ts)
            ):
                self._publish_local_new_ack(
                    order,
                    side,
                    int(order.get("new_ack_ts", now_ts) or now_ts),
                )

            if (order["state"] == self.state.ORDER_PENDING_NEW and order["cancel_ts"] > 0
                    and order["cancel_ts"] <= now_ts
                    and not order.get("cancel_terminal_suppressed_full_fill", False)
                    and (
                        cancel_effective_ts <= order["activate_ts"]
                        or bool(order.get("exchange_accepted", False))
                    )
                    and not (
                        int(order["cancel_ts"]) == int(now_ts)
                        and (
                            defer_all_cancel_ack_at_ts
                            or (
                                defer_cancel_ack_at_ts
                                and int(order["cancel_ts"])
                                == cancel_effective_ts
                            )
                        )
                    )):
                self._record_order_lifecycle_cancel_ack(
                    order,
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=int(cancel_effective_ts),
                    reason=str(
                        order.get("cancel_reason") or "cancel_before_active"
                    ),
                )
                self._record_queue_value_cancel(order, int(now_ts))
                self._append_order_outcome(
                    order, now_ts, "cancel",
                    order.get("cancel_reason") or "cancel_before_active",
                )
                self._post_cooldown_budget_release_order(
                    order,
                    now_ts=int(now_ts),
                    reason=str(
                        order.get("cancel_reason") or "cancel_before_active"
                    ),
                )
                self._ranked_guard_exchange_terminal(
                    order,
                    reason="cancel_ack",
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=int(cancel_effective_ts),
                )
                orders.pop(idx)
                continue

            if (
                order["state"] == self.state.ORDER_PENDING_NEW
                and order["activate_ts"] <= now_ts
                and not bool(order.get("exchange_cancel_effective", False))
                and not bool(order.get("exchange_accepted", False))
            ):
                best_bid_at, best_ask_at, bid_qty_at, ask_qty_at, mid_at, activation_source = self._book_snapshot_at(
                    order["activate_ts"], fallback_mid,
                )
                admission_bid, admission_ask = best_bid_at, best_ask_at
                admission_source = activation_source
                if order.get("time_in_force") != "IOC" and self.state.exchange_book_scheduler is not None:
                    execution_book, evidence = self._execution_admission_book_at(
                        order["activate_ts"], fallback_mid, order)
                    admission_bid, admission_ask = execution_book[:2]
                    admission_source = execution_book[5]
                    # Execution BBO governs GTX admission only. Keep the
                    # existing delivered midpoint/depth for distance-based
                    # queue consumers and fill diagnostics; changing those
                    # is not part of this admission correction.
                    if self.state.trace_orders is not None or self.state.l2_journal is not None:
                        order["activation_book_evidence"] = {
                            "processed_ts_ms": int(now_ts), "execution": evidence,
                            "delivered_bid": float(best_bid_at), "delivered_ask": float(best_ask_at),
                            "actual_source": str(admission_source),
                            "actual_bid": float(admission_bid), "actual_ask": float(admission_ask),
                        }
                if admission_source == "unavailable_book":
                    # Keep unresolved exchange risk in the order lifecycle;
                    # missing book evidence is neither a GTX reject nor ACK.
                    order["activation_book_status"] = "UNKNOWN"
                    idx += 1
                    continue
                if order.get("time_in_force") == "IOC":
                    # Matching and terminal visibility are separate. No NEW
                    # callback, queue seed, or resting order is fabricated at
                    # exchange activation while the result is still in flight.
                    order["exchange_accepted"] = True
                    order["fill_eligible"] = False
                    order["simulator_queue_source"] = "ioc_not_applicable"
                    order["exact_queue_path_valid"] = False
                    idx += 1
                    continue
                if self.services._order_would_cross_book_tick(
                    side,
                    float(order["price"]),
                    float(admission_bid),
                    float(admission_ask),
                    self.state.TICK,
                ):
                    reject_reason = (
                        "gtx_would_cross_ask"
                        if side == "BUY"
                        else "gtx_would_cross_bid"
                    )
                    self.state.gtx_rejects += 1
                    if order.get("circuit_breaker_close", False):
                        self.state.circuit_breaker_close_gtx_reject_count += 1
                        self.state.circuit_breaker_close_gtx_reject_streak += 1
                    self._record_order_lifecycle_reject(
                        order,
                        visibility_ts_ms=int(now_ts),
                        exchange_ts_ms=int(order["activate_ts"]),
                        reason=reject_reason,
                    )
                    self._post_cooldown_budget_release_order(
                        order,
                        now_ts=int(order["activate_ts"]),
                        reason=reject_reason,
                    )
                    self._ranked_guard_exchange_terminal(
                        order,
                        reason="rejected",
                        visibility_ts_ms=int(now_ts),
                        exchange_ts_ms=int(order["activate_ts"]),
                    )
                    orders.pop(idx)
                    continue
                if order.get("circuit_breaker_close", False):
                    self.state.circuit_breaker_close_gtx_reject_streak = 0

                quote_mid = mid_at if mid_at > 0.0 else order["mid_at_quote"]
                dist = max(0.0, quote_mid - order["price"]) if side == "BUY" else max(0.0, order["price"] - quote_mid)
                diag = order.get("quote_diag") or {}
                try:
                    near_depth_total = float(diag.get("near_depth_total", 0.0) or 0.0)
                except Exception:
                    near_depth_total = 0.0
                reason_bucket = self._queue_reason_bucket(order, side, near_depth_total)
                local_rank = self._local_rank_at_ts(order["activate_ts"])
                regime_queue_mult = self._queue_regime_multiplier(
                    side,
                    dist,
                    near_depth_total,
                    local_rank,
                    reason_bucket,
                    order["activate_ts"],
                )
                mo_queue_mult = self._queue_mo_multiplier(order, side, order["activate_ts"])
                inv_queue_mult, inv_exposure_increasing = self._queue_inventory_multiplier(
                    side,
                    float(order.get("inventory_at_submit", 0.0) or 0.0),
                )
                queue_mult = float(regime_queue_mult) * float(mo_queue_mult) * float(inv_queue_mult)
                policy_book_at = self._decision_visible_book(
                    int(order["activate_ts"]),
                    float(quote_mid),
                    advance_visibility_state=False,
                )
                policy_mid = float(policy_book_at["mid"])
                policy_distance = (
                    max(0.0, policy_mid - float(order["price"]))
                    if side == "BUY"
                    else max(0.0, float(order["price"]) - policy_mid)
                )
                native_queue = self._exchange_book_queue_seed(
                    order,
                    side,
                    int(order["activate_ts"]),
                )
                sparse_queue = (
                    self._active_order_queue_seed(
                        side,
                        float(order["price"]),
                        int(order["activate_ts"]),
                    )
                    if native_queue is None
                    else None
                )
                policy_queue = self._estimate_queue_ahead(
                    side,
                    order["price"],
                    policy_distance,
                    target_ts=int(policy_book_at["visible_ts"]),
                    regime_queue_mult=queue_mult,
                )
                if native_queue is not None:
                    # A newly accepted passive order joins behind the complete
                    # public quantity at its exact price. Do not apply the old
                    # top-N/fitted queue multiplier to native exchange truth.
                    order["queue_left"] = max(0.0, float(native_queue))
                    order["simulator_queue_source"] = (
                        "native_exchange_book"
                    )
                    order["exact_queue_path_valid"] = bool(
                        order.get("exchange_book_queue_path_valid", False)
                    )
                elif sparse_queue is None:
                    order["queue_left"] = self._estimate_queue_ahead(
                        side,
                        order["price"],
                        dist,
                        target_ts=order["activate_ts"],
                        regime_queue_mult=queue_mult,
                    )
                    order["simulator_queue_source"] = (
                        "topn_or_fitted_fallback"
                    )
                    order["exact_queue_path_valid"] = False
                else:
                    order["queue_left"] = max(
                        0.0,
                        float(sparse_queue)
                        * float(self.state.queue_ahead_base_mult)
                        * float(queue_mult),
                    )
                    order["simulator_queue_source"] = "sparse_watch_seed"
                    order["exact_queue_path_valid"] = False
                order["queue_source"] = "delayed_policy_topn_or_fitted"
                order["policy_queue_source"] = str(order["queue_source"])
                order["queue_regime_mult"] = float(regime_queue_mult)
                order["queue_mo_mult"] = float(mo_queue_mult)
                order["queue_inventory_mult"] = float(inv_queue_mult)
                order["queue_inventory_exposure_increasing"] = bool(inv_exposure_increasing)
                order["queue_reason_bucket"] = reason_bucket
                order["queue_local_rank"] = float(local_rank)
                order["queue_init"] = order["queue_left"]
                order["policy_queue_left"] = float(policy_queue)
                order["policy_queue_init"] = float(policy_queue)
                if self.state.dynamic_fill_hazard_action_enabled and side == "BUY":
                    hazard_id = str(order["trace_id"])
                    hazard_lifecycle = self.services.QuantityWeightedOrderLifecycle(
                        initial_quantity=float(order.get("quantity", 0.0) or 0.0),
                        submitted_ts_ns=int(order["submit_ts"]) * 1_000_000,
                    )
                    self.state.dynamic_fill_hazard_lifecycles[hazard_id] = hazard_lifecycle
                    self._append_dynamic_fill_hazard_lifecycle_journal(
                        order,
                        "submit",
                    )
                    if self.state.dynamic_fill_hazard_visibility_scheduler is not None:
                        visible_seed = (
                            self.state.dynamic_fill_hazard_visibility_scheduler.lookup(
                                "BUY",
                                self.services._order_price_tick(order, self.state.TICK),
                            )
                        )
                        seed_raw = visible_seed.quantity
                        seed_status = str(visible_seed.status)
                        seed_reason = str(visible_seed.reason)
                        seed_generation = int(visible_seed.segment_id)
                        seed_receive_ts_ns = (
                            self._dynamic_fill_hazard_policy_source_ts_ns(
                                exchange_ts_ns=int(
                                    self.state.dynamic_fill_hazard_visibility_scheduler.last_truth_exchange_ts_ns
                                ),
                                provider_receive_ts_ns=int(
                                    self.state.dynamic_fill_hazard_visibility_scheduler.last_provider_receive_ts_ns
                                ),
                            )
                        )
                        seed_feature_ready_ts_ns = int(
                            self.state.dynamic_fill_hazard_visibility_scheduler.last_feature_ready_ts_ns
                        )
                        path_valid = bool(visible_seed.strict_usable)
                    else:
                        seed_raw = order.get("_exchange_book_queue_seed_qty")
                        seed_status = str(
                            order.get(
                                "_exchange_book_queue_seed_status",
                                "unknown",
                            )
                        )
                        seed_reason = str(
                            order.get(
                                "_exchange_book_queue_seed_reason",
                                "unknown",
                            )
                        )
                        seed_generation = int(
                            order.get(
                                "exchange_book_queue_segment_id",
                                self.state.exchange_book_scheduler.segment_id,
                            )
                            or 0
                        )
                        seed_receive_ts_ns = max(
                            int(
                                order.get(
                                    "exchange_book_queue_asof_ts_ns",
                                    0,
                                )
                                or 0
                            ),
                            int(self.state.exchange_book_scheduler.last_local_receive_ts_ns),
                        )
                        seed_feature_ready_ts_ns = seed_receive_ts_ns
                        path_valid = bool(
                            order.get("exchange_book_queue_path_valid", False)
                        )
                    seed_qty = (
                        max(0.0, float(seed_raw))
                        if seed_raw is not None
                        and self.services.np.isfinite(float(seed_raw))
                        else 0.0
                    )
                    self.state.dynamic_fill_hazard_paths[hazard_id] = (
                        self.services.ReplayOrderDepthPath(
                            client_order_id=hazard_id,
                            side="BUY",
                            price=float(order["price"]),
                            generation=seed_generation,
                            initial_visible_qty=seed_qty,
                            current_visible_qty=seed_qty,
                            receive_ts_ns=seed_receive_ts_ns,
                            activation_ts_ns=(
                                int(order["activate_ts"]) * 1_000_000
                            ),
                            feature_ready_ts_ns=seed_feature_ready_ts_ns,
                            valid=path_valid,
                            invalid_reason=(
                                ""
                                if path_valid
                                else seed_reason
                            ),
                        )
                    )
                    if self.state.dynamic_fill_hazard_cpp_runtime is not None:
                        cpp_seed = self.state.dynamic_fill_hazard_cpp_runtime.activate_order(
                            hazard_id,
                            float(order["price"]),
                            int(order["activate_ts"]) * 1_000_000,
                        )
                        self.state.dynamic_fill_hazard_cpp_activation_count += 1
                        cpp_quantity = cpp_seed.get("quantity")
                        quantity_equal = bool(
                            cpp_quantity is not None
                            and self._dynamic_fill_hazard_float_equal(
                                cpp_quantity,
                                seed_qty,
                            )
                        ) if path_valid else cpp_quantity is None
                        self._dynamic_fill_hazard_cpp_check(
                            str(cpp_seed.get("status", "unknown"))
                            == seed_status
                            and str(cpp_seed.get("reason", "unknown"))
                            == seed_reason
                            and int(cpp_seed.get("segment_id", 0) or 0)
                            == seed_generation
                            and quantity_equal,
                            "order_activation",
                            now_ts_ms=int(now_ts),
                            client_order_id=hazard_id,
                            python={
                                "status": seed_status,
                                "reason": seed_reason,
                                "quantity": seed_qty if path_valid else None,
                                "segment_id": seed_generation,
                            },
                            cpp=cpp_seed,
                        )
                queue_value_intervention_id = int(
                    order.get(
                        "queue_value_keep_cancel_intervention_id",
                        -1,
                    )
                    or -1
                )
                queue_value_record = self.state.queue_value_open_records.get(
                    queue_value_intervention_id
                )
                if (
                    queue_value_record is not None
                    and bool(order.get("queue_value_reentry_order", False))
                ):
                    reentry_status = str(
                        order.get(
                            "_exchange_book_queue_seed_status",
                            "disabled",
                        )
                    )
                    reentry_supported = bool(
                        order.get("simulator_queue_source")
                        == "native_exchange_book"
                        and reentry_status in {"exact", "known_zero"}
                        and order.get(
                            "exchange_book_queue_path_valid",
                            False,
                        )
                    )
                    queue_value_record[
                        "native_reentry_seed_supported"
                    ] = int(reentry_supported)
                    queue_value_record[
                        "native_reentry_queue_status"
                    ] = reentry_status
                    if not reentry_supported:
                        queue_value_record[
                            "exchange_book_queue_path_valid"
                        ] = 0
                        queue_value_record[
                            "exchange_book_queue_invalidated"
                        ] = 1
                        queue_value_record[
                            "exchange_book_queue_invalidated_reason"
                        ] = "reentry_native_seed_unavailable"
                self._init_l2_cancel_ahead_state(
                    order,
                    side,
                    int(order["activate_ts"]),
                )
                order["mid_at_quote"] = quote_mid
                order["quote_ts"] = int(order.get("restore_quote_ts", order["activate_ts"]) or order["activate_ts"])
                order["fill_eligible"] = self._new_fill_eligible(side, order["activate_ts"])
                order["exchange_accepted"] = True
                order["_new_ack_policy_book"] = dict(policy_book_at)
                order["_new_ack_policy_mid"] = float(policy_mid)
                new_ack_ts = int(
                    order.get("new_ack_ts", order["activate_ts"])
                    or order["activate_ts"]
                )
                if new_ack_ts <= int(now_ts):
                    self._publish_local_new_ack(order, side, int(now_ts))

            if (
                order["state"] == self.state.ORDER_PENDING_NEW
                and bool(order.get("exchange_accepted", False))
                and not bool(order.get("local_new_ack_published", False))
                and int(order.get("new_ack_ts", 0) or 0) <= int(now_ts)
            ):
                self._publish_local_new_ack(
                    order,
                    side,
                    int(order.get("new_ack_ts", now_ts) or now_ts),
                )

            if (
                order["cancel_ts"] > 0
                and order["cancel_ts"] <= now_ts
                and not order.get("cancel_terminal_suppressed_full_fill", False)
                and not (
                    self.state.replace_terminal_continuation
                    and bool(
                        order.get(
                            "replace_terminal_continuation_armed",
                            False,
                        )
                    )
                    and int(order.get("cancel_request_ts", -1)) >= int(now_ts)
                )
                and order["state"] in (
                    self.state.ORDER_PENDING_NEW,
                    self.state.ORDER_OPEN,
                    self.state.ORDER_PENDING_CANCEL,
                )
                and not (
                    (defer_cancel_ack_at_ts or defer_all_cancel_ack_at_ts)
                    and int(order["cancel_ts"]) == int(now_ts)
                )
            ):
                self._record_order_lifecycle_cancel_ack(
                    order,
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=int(cancel_effective_ts),
                    reason=str(order.get("cancel_reason") or "cancel_ack"),
                )
                self._record_queue_value_cancel(order, int(now_ts))
                self._append_order_outcome(
                    order, now_ts, "cancel",
                    order.get("cancel_reason") or "cancel_ack",
                )
                self._dynamic_fill_hazard_order_terminal(
                    order,
                    event="cancel_ack",
                    now_ts=int(now_ts),
                )
                self._post_cooldown_budget_release_order(
                    order,
                    now_ts=int(now_ts),
                    reason=str(order.get("cancel_reason") or "cancel_ack"),
                )
                self._ranked_guard_exchange_terminal(
                    order,
                    reason="cancel_ack",
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=int(cancel_effective_ts),
                )
                if (
                    self.state.replace_terminal_continuation
                    and bool(
                        order.get(
                            "replace_terminal_continuation_armed",
                            False,
                        )
                    )
                    and str(order.get("cancel_reason") or "")
                    in {"requote_replace", "cancel_first_replace"}
                ):
                    self.state.replacement_terminal_due_ts[side] = int(
                        order.get("cancel_ts", now_ts) or now_ts
                    )
                    self.state.replacement_terminal_terminal_count += 1
                    if (self.state.async_rest_gateway and self.state.main_loop_next_wake_ms is not None
                            and int(now_ts) < self.state.main_loop_next_start_ms):
                        # A private terminal wakes a sleeping executor, not
                        # one already computing or doing measured local work.
                        self.state.main_loop_work_pre_sum_ms -= (
                            self.state.main_loop_next_wake_ms - self.state.main_loop_next_start_ms
                        )
                        self._schedule_main_loop_tick(max(int(now_ts), self.state.main_loop_idle_start_ms))
                orders.pop(idx)
                continue

            idx += 1
        self._sync_local_order_lifecycle_repair(int(now_ts))

    def _quote_opportunity_ready(self) -> bool:
        """Resolve wake, risk, warmup and side routing before a quote call."""
        if self.state.quote_schedule is not None:
            self._quote_schedule_publish(int(self.state.t))
            if (getattr(self.state, "main_loop_pre_pending", False)
                    and self.state.pending_quote_compute is None
                    and self.state.serial_rest_decision is None
                    and self.state.quote_schedule.unhandled()):
                self.context.eligible = max(int(self.state.t), self.state.main_loop_idle_start_ms,
                               (self.state.quote_schedule.busy_until_ns + 999_999) // 1_000_000)
                if (self.state.main_loop_next_wake_ms is not None
                        and self.context.eligible < self.state.main_loop_next_start_ms):
                    self._schedule_main_loop_tick(self.context.eligible)
        if self.state.main_loop_enabled and not (self.state.is_main_loop_wake or self.state.is_quote_compute_resume):
            # Exchange matching, native state, and private callbacks above are
            # independent of the sleeping/REST-blocked live main thread.
            return False

        if self.state.main_loop_enabled and self.state.dynamic_rq and self.context.var_retsq is not None:
            # Live's one-second bar callback updates these EMAs before tick()
            # asks whether it is due. Catch up delivered bars once per wake,
            # including bars delivered while synchronous REST blocked main.
            # The first close only initializes the previous-close anchor.
            self.state.rq_visible_index = (
                self._message_clock_state("variance", int(self.state.t))[0]
                if self.state.exec_message_schedules
                else int(self.services.np.searchsorted(self.context.var_ts_ms, int(self.state.t) - 1_000, side="right")) - 1
            )
            while self.state.main_loop_rq_var_idx < self.state.rq_visible_index:
                self.state.main_loop_rq_var_idx += 1
                self.state.rsq = float(self.context.var_retsq[self.state.main_loop_rq_var_idx])
                if not self.state.dyn_rq_inited:
                    self.state.ema_var_fast = self.state.ema_var_slow = self.state.rsq
                    self.state.dyn_rq_inited = True
                else:
                    self.state.ema_var_fast = 0.067 * self.state.rsq + (1.0 - 0.067) * self.state.ema_var_fast
                    self.state.ema_var_slow = 0.011 * self.state.rsq + (1.0 - 0.011) * self.state.ema_var_slow
            self.state.cur_rq_ms = self.state.rq_ms
            if self.state.dyn_rq_inited and self.state.ema_var_slow >= 1e-12:
                self.state.vol_ratio = max(0.0, min(self.state.ema_var_fast / self.state.ema_var_slow, 2.0))
                self.state.rq_f = self.state.rq_max_ms * self.services.math.exp(self.services.math.log(self.state.rq_min_ms / self.state.rq_max_ms) * self.state.vol_ratio)
                self.state.cur_rq_ms = int(max(self.state.rq_min_ms, min(self.state.rq_max_ms, self.state.rq_f)))

        self.state.wake_decision_book = (
            None if self.state.is_quote_compute_resume
            else self._decision_visible_book(int(self.state.t), float(self.state.p))
        )
        if self.state.quote_schedule is not None and self.state.wake_decision_book is not None:
            self.context.usable = bool(self.state.wake_decision_book.get("book_usable", True))
            if self.context.usable != getattr(self.state, "quote_schedule_book_usable", None):
                self.state.quote_schedule.change("BOOK_VALIDITY")
                self.state.quote_schedule_book_usable = self.context.usable
                if self.state.is_main_loop_wake:
                    self.state.quote_schedule.claim(int(self.state.t) * 1_000_000)
        if self.state.hard_risk_enabled:
            if self.state.wake_decision_book is None:
                self.state.wake_decision_book = self._decision_visible_book(int(self.state.t), float(self.state.p))
            self.state.mark = float(self.state.wake_decision_book.get("inventory_mark_price", self.state.wake_decision_book["mid"]))
            if self.services.math.isfinite(self.state.mark) and self.state.mark > 0.0:
                self.state.hard_risk_mark_price = self.state.mark
                self.state.risk_state.observe(int(self.state.t), self.state.cash, self.state.q, self.state.mark)
        if self.state.risk_emergency_latched and self.state.risk_emergency_attempt_complete:
            # The emergency call stopped the live main loop. Its exchange
            # request still settles above, but a later stale wake cannot
            # cancel that in-flight market close or resume ordinary quoting.
            return False
        if (
            not self.state.is_quote_compute_resume and (self.state.bid_orders or self.state.ask_orders)
            and (not self.state.wake_decision_book.get("book_usable", True) or (
              self.state.replay_event_clock != "empirical"
              and (self.state.exec_message_schedules or (
                not self.state.use_bar_pricing and self.state.max_exec_book_age_ms > 0
                and (self.state.historical_bbo or self.state.historical_l2)
              ))))
        ):
            # Quote-stop safety runs on each available main-thread wake, not
            # only every requote. Reuse this snapshot if the same wake is due;
            # a resumed compute phase takes its own later visible snapshot.
            if self.state.exec_message_schedules:
                self.state.wake_age = float(self.state.wake_decision_book["depth_visible_age_ms"])
                self.state.wake_lag = float(self.state.wake_decision_book["depth_source_lag_ms"])
                self.state.wake_stale = (
                    (self.state.max_exec_book_visible_age_ms > 0 and (
                        not self.services.math.isfinite(self.state.wake_age) or self.state.wake_age < 0.0
                        or self.state.wake_age > self.state.max_exec_book_visible_age_ms
                    ))
                    or (self.state.max_exec_book_source_lag_ms > 0 and (
                        not self.services.math.isfinite(self.state.wake_lag) or self.state.wake_lag < 0.0
                        or self.state.wake_lag > self.state.max_exec_book_source_lag_ms
                    ))
                )
            else:
                self.state.wake_age = self._current_book_age_ms(
                    int(self.state.t), bbo_cursor=int(self.state.wake_decision_book["bbo_idx"]),
                    l2_cursor=int(self.state.wake_decision_book["l2_idx"]),
                )
                self.state.wake_stale = self.state.wake_age is None or self.state.wake_age > self.state.max_exec_book_age_ms
            self.state.wake_stale |= not self.state.wake_decision_book.get("book_usable", True)
            if self.state.wake_stale:
                self._cooldown_lineage_observe_early_blocker(int(self.state.t), "stale_book")
                self.state.stale_book_skip_count += 1
                self.state.stale_book_cancel_count += 1
                self.state.age_for_stats = float(self.state.wake_age if self.state.wake_age is not None else self.state.max_exec_book_age_ms)
                self.state.stale_book_age_sum += self.state.age_for_stats
                self.state.stale_book_age_max = max(self.state.stale_book_age_max, self.state.age_for_stats)
                self._request_cancel_all(self.state.bid_orders, self.state.t, "stale_book")
                self._request_cancel_all(self.state.ask_orders, self.state.t, "stale_book")
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
                return False

        if self.state.risk_emergency_latched:
            # The stopped live main thread finishes its cancel/market request;
            # exchange transitions and private callbacks above remain active.
            if not self.state.risk_emergency_attempt_complete and not self.state.bid_orders and not self.state.ask_orders:
                if abs(self.state.q) >= self.state.LOT_SIZE:
                    self._requote_circuit_breaker_close(
                        int(self.state.t), self.state.hard_risk_mark_price, emergency_market=True,
                    )
                    self.state.risk_emergency_attempt_complete = bool(self.state.bid_orders or self.state.ask_orders)
            return False

        if not self.state.is_quote_compute_resume and self.state.consecutive_loss_cooldown.active(int(self.state.t)):
            self._cooldown_lineage_observe_early_blocker(
                int(self.state.t),
                "consecutive_loss_cooldown",
            )
            self.state.consecutive_loss_cooldown_block_count += 1
            if (
                int(self.state.t) - int(self.state.consecutive_loss_cooldown.last_cancel_ts_ms)
                >= 5_000
            ):
                self._request_cancel_all(
                    self.state.bid_orders,
                    int(self.state.t),
                    reason="consecutive_loss_cooldown",
                )
                self._request_cancel_all(
                    self.state.ask_orders,
                    int(self.state.t),
                    reason="consecutive_loss_cooldown",
                )
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
                self.state.consecutive_loss_cooldown_cancel_count += 1
                self.state.consecutive_loss_cooldown.last_cancel_ts_ms = int(self.state.t)
            return False

        # ── Requote ──
        if self.state.replay_event_clock == "empirical" and self.state.empirical_clock_action == 3:
            self._cooldown_lineage_observe_early_blocker(int(self.state.t), "stale_book")
            self.state.stale_book_skip_count += 1
            if self.state.bid_orders or self.state.ask_orders:
                self.state.stale_book_cancel_count += 1
            self._request_cancel_all(self.state.bid_orders, self.state.t, "stale_book")
            self._request_cancel_all(self.state.ask_orders, self.state.t, "stale_book")
            self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
            self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
            return False
        if self.state.replay_event_clock == "empirical" and self.state.empirical_clock_action != 2:
            return False
        self.state.scheduled_requote_due_raw = (
            self.state.empirical_clock_action == 2
            if self.state.replay_event_clock == "empirical"
            else self.state.t - self.state.last_rq_ts >= self.state.cur_rq_ms
        )
        if self.state.quote_schedule is not None:
            self.state.scheduled_requote_due_raw = self.state.quote_schedule.inflight is not None
        if self.state.signal_cold_start and self.state.signal_warmup_observed_ts_ms is None:
            # Live finalizes the prior exchange-second bar on the next trade
            # callback and emits intervening no-trade bars. Source/preroll rows
            # received before this process origin are not a REST prefill.
            self.context.schedule = self.state.exec_message_schedules["trade"]
            self.context.visible_index, self.context.visible_exchange_ns, self.context._ = self._message_clock_state("trade", int(self.state.t))
            if self.state.signal_first_bucket_ms is None:
                self.context.first_index = int(self.services.np.searchsorted(self.context.schedule.ready_ns_for_channel(),
                    self.state.replay_origin_ts_ms * 1_000_000, side="left"))
                if self.context.visible_index >= self.context.first_index:
                    self.state.signal_first_bucket_ms = int(self.context.schedule.exchange_ns_for_channel()[self.context.first_index] // 1_000_000_000) * 1_000
            if self.state.signal_first_bucket_ms is not None:
                self.state.signal_completed_bars = max(self.state.signal_completed_bars,
                    (self.context.visible_exchange_ns // 1_000_000_000 * 1_000 - self.state.signal_first_bucket_ms) // 1_000)
            if self.state.signal_completed_bars < self.services.SIGNAL_WARMUP_BARS:
                self.state.signal_warmup_skip_count += 1
                if self.state.scheduled_requote_due_raw:
                    self.state.last_rq_ts = int(self.state.t)
                return False
            self.state.signal_warmup_observed_ts_ms = int(self.state.t)
        # Terminal continuation wins a same-wake collision with the ordinary
        # cadence. It performs a fresh side-only decision without advancing
        # last_rq_ts; the overdue ordinary cadence remains due at the following
        # 100ms wake. While waiting for that wake, intermediate market events
        # cannot consume the continuation or start a normal quote decision.
        self.state.cadence_defer_ready = bool(
            self.state.replacement_terminal_cadence_defer_until_ms >= 0
            and self.state.continuation_timer_wake
            and int(self.state.t) >= self.state.replacement_terminal_cadence_defer_until_ms
        )
        if self.state.replacement_terminal_cadence_defer_until_ms >= 0:
            self.state.scheduled_requote_due = bool(
                self.state.scheduled_requote_due_raw and self.state.cadence_defer_ready
            )
            if self.state.cadence_defer_ready:
                self.state.replacement_terminal_cadence_defer_until_ms = -1
        else:
            self.state.scheduled_requote_due = bool(
                self.state.scheduled_requote_due_raw
                and not (self.state.continuation_pending_bid or self.state.continuation_pending_ask)
            )
        if (
            self.state.continuation_force_bid_due or self.state.continuation_force_ask_due
        ) and self.state.scheduled_requote_due_raw:
            self.state.replacement_terminal_cadence_defer_until_ms = max(
                self.state.replacement_terminal_cadence_defer_until_ms,
                int(self.state.t) + self.state.replay_clock_interval_ms,
            )
            self.state.scheduled_requote_due = False
            if self.state.quote_schedule is not None:
                # U1 leaves the opposite-side watermark outstanding; it does
                # not turn that work into another fixed 100ms quote timer.
                self.state.replacement_terminal_cadence_defer_until_ms = -1
        self.state.response_force_bid_due = bool(
            self.state.post_fill_quote_response_cfg.enabled
            and self.state.post_fill_quote_response_force_bid_requote
            and not self.state.bid_orders
        )
        self.state.response_force_ask_due = bool(
            self.state.post_fill_quote_response_cfg.enabled
            and self.state.post_fill_quote_response_force_ask_requote
            and not self.state.ask_orders
        )
        if self.state.is_quote_compute_resume:
            # Continue the original call, not a newly scheduled requote. A
            # callback during compute can separately request the next call.
            self.state.scheduled_requote_due = self.state.active_quote_compute["scheduled_requote_due"]
            self.state.response_force_bid_due = self.state.active_quote_compute["response_force_bid_due"]
            self.state.response_force_ask_due = self.state.active_quote_compute["response_force_ask_due"]
            self.state.continuation_force_bid_due = self.state.active_quote_compute[
                "continuation_force_bid_due"
            ]
            self.state.continuation_force_ask_due = self.state.active_quote_compute[
                "continuation_force_ask_due"
            ]
            self.state.continuation_bid_terminal_ts = self.state.active_quote_compute[
                "continuation_bid_terminal_ts"
            ]
            self.state.continuation_ask_terminal_ts = self.state.active_quote_compute[
                "continuation_ask_terminal_ts"
            ]
            if self.state.continuation_force_bid_due:
                self.state.replacement_terminal_due_ts["BUY"] = -1
            if self.state.continuation_force_ask_due:
                self.state.replacement_terminal_due_ts["SELL"] = -1
        self.state.continuation_side_routing = bool(
            self.state.continuation_force_bid_due or self.state.continuation_force_ask_due
        )
        self.state.bid_route_due = bool(
            not self.state.continuation_side_routing or self.state.continuation_force_bid_due
        )
        self.state.ask_route_due = bool(
            not self.state.continuation_side_routing or self.state.continuation_force_ask_due
        )
        self.state.pair_route_due = bool(self.state.bid_route_due and self.state.ask_route_due)
        if (
            self.state.sampled_serial_gateway
            and not self.state.async_rest_gateway
            and not self.state.is_quote_compute_resume
            and (int(self.state.t) < self.state.rest_gateway_busy_until_ms or self.state.serial_rest_decision is not None)
            and (
                self.state.scheduled_requote_due
                or self.state.response_force_bid_due
                or self.state.response_force_ask_due
                or self.state.continuation_force_bid_due
                or self.state.continuation_force_ask_due
            )
        ):
            # Exchange matching and private callbacks above still advance.
            # A synchronous REST lane cannot begin another policy decision.
            if self.state.continuation_force_bid_due:
                self.state.replacement_terminal_due_ts["BUY"] = int(
                    self.state.continuation_bid_terminal_ts
                )
            if self.state.continuation_force_ask_due:
                self.state.replacement_terminal_due_ts["SELL"] = int(
                    self.state.continuation_ask_terminal_ts
                )
            self.state.rest_gateway_decision_deferral_count += 1
            return False
        return True

    def _capture_quote_decision(self) -> bool:
        """Charge compute and capture only the market view visible at the read."""
        if self.state.quote_schedule is not None and not self.state.is_quote_compute_resume:
            self.context.ledger = self.state.quote_schedule
            self.context.sides = tuple(side for side, due in (("BUY", self.state.bid_route_due),
                ("SELL", self.state.ask_route_due)) if due)
            if self.context.ledger.inflight is None:
                self.context.ledger.claim_route(int(self.state.t) * 1_000_000, self.context.sides, "terminal")
            else:
                self.context.ledger.inflight.sides = self.context.sides
            self._quote_schedule_read("prediction", "PREDICTION_VIEW", int(self.state.t))
        if self.state.target_observation is not None and not self.state.is_quote_compute_resume:
            self.state.target_observation.begin_call()
        if self.state.runtime_compute_by_path and not self.state.is_quote_compute_resume:
            self.state.runtime_cutoff_ms = (
                self._message_clock_state("prediction", int(self.state.t))[1] // 1_000_000
                if self.context.params["runtime_compute_clock"] == "prediction_delivery"
                else (int(self.state.t) // 1_000) * 1_000
            )
            self.state.bucket_end_ms = (
                self.state.runtime_cutoff_ms // self.state.runtime_compute_bucket_ms
            ) * self.state.runtime_compute_bucket_ms
            if (
                not any(self.state.runtime_compute_path_counts.values())
                and self.state.runtime_compute_last_bucket_end_ms is not None
                and self.state.runtime_compute_last_bucket_end_ms > self.state.bucket_end_ms
            ):
                raise ValueError("runtime compute initial watermark exceeds causal cutoff")
            # Cold-flat inputs have no completed pre-roll prediction. Their
            # original account origin measures elapsed computation buckets;
            # it is not a fabricated prediction or a new warmup policy.
            if self.state.runtime_compute_last_bucket_end_ms is not None:
                self.context.compute_origin = self.state.runtime_compute_last_bucket_end_ms
            else:
                self.context.origin_ms = (self.state.signal_first_bucket_ms
                    if self.state.signal_cold_start else self.state.replay_origin_ts_ms)
                self.context.compute_origin = (self.context.origin_ms // self.state.runtime_compute_bucket_ms
                    * self.state.runtime_compute_bucket_ms)
            self.state.bucket_count = max(
                0, (self.state.bucket_end_ms - self.context.compute_origin)
                // self.state.runtime_compute_bucket_ms,
            )
            self.state.compute_path = (
                "cached_no_new_bucket" if self.state.bucket_count == 0 else
                "new_bucket" if self.state.bucket_count == 1 else "catch_up"
            )
            self.state.runtime_compute_last_bucket_end_ms = max(
                self.context.compute_origin, self.state.bucket_end_ms,
            )
            self.state.runtime_compute_path_counts[self.state.compute_path] += 1
            self.state.phases = self.state.runtime_compute_by_path[self.state.compute_path]
            self.state.pre_snapshot_compute_samples_ms = self.state.phases[:, 0]
            self.state.decision_to_gateway_latency_samples_ms = self.state.phases[:, 1]
            self.state.requote_tail_work_samples_ms = self.state.phases[:, 2]
        if self.state.main_loop_enabled and not self.state.is_quote_compute_resume:
            # Live sets its timer at actual _requote() entry, never at a
            # missed fixed-grid deadline. Compute also occupies keep/skip
            # calls, even when they produce no HTTP request.
            self.state.main_loop_tick_complete_ms = self._decision_gateway_request_ts(int(self.state.t))
            if self.state.requote_tail_work_enabled:
                self.state.main_loop_quote_tail_ms = self.services._deterministic_decision_to_gateway_latency_ms(
                    self.state.requote_tail_work_samples_ms, seed=self.state.decision_to_gateway_latency_seed,
                    decision_ts_ms=int(self.state.t),
                )
                self.state.requote_tail_work_count += 1
                self.state.requote_tail_work_sum_ms += self.state.main_loop_quote_tail_ms
        # Live main loop wakes on a wall-clock cadence. If replay resets the
        # next quote time to the current trade timestamp, sparse trade
        # periods drift slower than live and understate time-based guards
        # such as fill cooldown. Fixed mode keeps the scheduler anchored to
        # the intended cadence while still using the latest observed market
        # state at this trade.
        if (
            self.state.main_loop_enabled
            and not self.state.is_quote_compute_resume
            and not self.state.continuation_side_routing
        ):
            self.state.last_rq_ts = self.state.t
        elif self.state.scheduled_requote_due and self.state.replay_event_clock == "empirical":
            self.state.last_rq_ts = self.state.t
        elif self.state.scheduled_requote_due and not self.state.is_quote_compute_resume:
            if self.state.random_passive_enabled and self.state.random_passive_timing_jitter_fraction > 0.0:
                self.state.random_unit = self.services.deterministic_random_passive_unit(
                    seed=self.state.random_passive_seed,
                    event_ts_ms=int(self.state.t),
                    action_identity=int(self.state.i),
                    operation=self.services._RANDOM_PASSIVE_TIMING_JITTER,
                )
                self.state.interval_mult = 1.0 + (
                    2.0 * self.state.random_unit - 1.0
                ) * self.state.random_passive_timing_jitter_fraction
                self.state.target_interval_ms = max(
                    1,
                    int(self.services.math.floor(self.state.cur_rq_ms * self.state.interval_mult + 0.5)),
                )
                # The normal due check is t - last >= cur. Offset last so its
                # next due interval is target_interval_ms while preserving the
                # same mean cadence over many requotes.
                self.state.last_rq_ts = int(self.state.t + self.state.target_interval_ms - self.state.cur_rq_ms)
                self.state.random_passive_timing_jitter_count += 1
            elif self.state.requote_clock == "fixed":
                self.state.missed_intervals = max(1, int((self.state.t - self.state.last_rq_ts) // max(self.state.cur_rq_ms, 1)))
                self.state.last_rq_ts += self.state.missed_intervals * self.state.cur_rq_ms
            else:
                self.state.last_rq_ts = self.state.t
        if self.state.response_force_bid_due and not self.state.is_quote_compute_resume:
            self.state.post_fill_quote_response_force_bid_requote = False
        if self.state.response_force_ask_due and not self.state.is_quote_compute_resume:
            self.state.post_fill_quote_response_force_ask_requote = False

        if self.state.pre_snapshot_compute_enabled and not self.state.is_quote_compute_resume:
            self.state.pre_ms = self.services._deterministic_decision_to_gateway_latency_ms(
                self.state.pre_snapshot_compute_samples_ms,
                seed=self.state.decision_to_gateway_latency_seed, decision_ts_ms=int(self.state.t),
            )
            self.state.prediction_cutoff_ms = (
                self._message_clock_state("prediction", int(self.state.t))[1] // 1_000_000
                if self.state.exec_message_schedules and "prediction" in self.state.exec_message_schedules
                else (int(self.state.t) // 1_000) * 1_000
            )
            self.state.active_quote_compute = {
                "entry_ts": int(self.state.t), "capture_ts": int(self.state.t) + self.state.pre_ms,
                "request_ready_ts": int(self.state.main_loop_tick_complete_ms),
                "prediction_cutoff_ms": int(self.state.prediction_cutoff_ms),
                "scheduled_requote_due": bool(self.state.scheduled_requote_due),
                "response_force_bid_due": bool(self.state.response_force_bid_due),
                "response_force_ask_due": bool(self.state.response_force_ask_due),
                "continuation_force_bid_due": bool(
                    self.state.continuation_force_bid_due
                ),
                "continuation_force_ask_due": bool(
                    self.state.continuation_force_ask_due
                ),
                "continuation_bid_terminal_ts": int(
                    self.state.continuation_bid_terminal_ts
                ),
                "continuation_ask_terminal_ts": int(
                    self.state.continuation_ask_terminal_ts
                ),
            }
            self.state.pre_snapshot_compute_count += 1
            self.state.pre_snapshot_compute_sum_ms += self.state.pre_ms
            if self.state.pre_ms > 0:
                if self.state.continuation_force_bid_due:
                    self.state.replacement_terminal_due_ts["BUY"] = int(
                        self.state.continuation_bid_terminal_ts
                    )
                if self.state.continuation_force_ask_due:
                    self.state.replacement_terminal_due_ts["SELL"] = int(
                        self.state.continuation_ask_terminal_ts
                    )
                self.state.pending_quote_compute = self.state.active_quote_compute
                self.state.active_quote_compute = None
                return False

        if self.state.active_quote_compute is not None:
            self.state.pre_snapshot_compute_completed_count += 1
        if self.state.continuation_force_bid_due:
            self.state.replacement_terminal_decision_count += 1
            self.state.replacement_terminal_bid_decision_count += 1
            self.state.latency_ms = max(
                0,
                int(self.state.t) - int(self.state.continuation_bid_terminal_ts),
            )
            self.state.replacement_terminal_decision_latency_sum_ms += self.state.latency_ms
            self.state.replacement_terminal_decision_latency_max_ms = max(
                self.state.replacement_terminal_decision_latency_max_ms,
                self.state.latency_ms,
            )
        if self.state.continuation_force_ask_due:
            self.state.replacement_terminal_decision_count += 1
            self.state.replacement_terminal_ask_decision_count += 1
            self.state.latency_ms = max(
                0,
                int(self.state.t) - int(self.state.continuation_ask_terminal_ts),
            )
            self.state.replacement_terminal_decision_latency_sum_ms += self.state.latency_ms
            self.state.replacement_terminal_decision_latency_max_ms = max(
                self.state.replacement_terminal_decision_latency_max_ms,
                self.state.latency_ms,
            )
        self.state.truth_book_state = (
            self.state.cur_best_bid,
            self.state.cur_best_ask,
            self.state.cur_l2_bid_px_row,
            self.state.cur_l2_bid_qty_row,
            self.state.cur_l2_ask_px_row,
            self.state.cur_l2_ask_qty_row,
        )
        self.state.decision_book = (
            self.state.wake_decision_book if self.state.wake_decision_book is not None
            else self._decision_visible_book(int(self.state.t), float(self.state.p))
        )
        self.state.decision_bbo_idx = int(self.state.decision_book["bbo_idx"])
        self.state.decision_l2_idx = int(self.state.decision_book["l2_idx"])
        if self.state.quote_schedule is not None:
            for loop_item in self._quote_schedule_sources():
                self.context.feed, self.context.domain = loop_item
                if self.context.feed != "prediction":
                    self._quote_schedule_read(self.context.feed, self.context.domain, int(self.state.t))
        self.state.active_decision_bbo_idx = self.state.decision_bbo_idx
        self.state.cur_best_bid = float(self.state.decision_book["best_bid"])
        self.state.cur_best_ask = float(self.state.decision_book["best_ask"])
        self.state.cur_l2_bid_px_row = self.state.decision_book["l2_bid_px"]
        self.state.cur_l2_bid_qty_row = self.state.decision_book["l2_bid_qty"]
        self.state.cur_l2_ask_px_row = self.state.decision_book["l2_ask_px"]
        self.state.cur_l2_ask_qty_row = self.state.decision_book["l2_ask_qty"]
        self.state.mid = float(self.state.decision_book["mid"])
        if self.state.exec_message_schedules and any(
            self._message_clock_state(feed, int(self.state.t))[0] < 0
            for feed in self.state.exec_message_schedules
            if feed != "bbo"
        ):
            self.state.exec_message_missing_source_skip_count += 1
            self._request_cancel_all(self.state.bid_orders, self.state.t, "message_source_not_ready")
            self._request_cancel_all(self.state.ask_orders, self.state.t, "message_source_not_ready")
            return False
        if self.state.exec_book_visibility_delay_enabled and not self.state.use_bar_pricing:
            self.state.sampled_book_delay_ms = int(self.state.decision_book["delay_ms"])
            self.state.sampled_depth_delay_ms = int(self.state.decision_book["depth_delay_ms"])
            self.state.sampled_trade_delay_ms = int(self.state.decision_book["trade_delay_ms"])
            self.state.exec_book_visibility_delay_applied_count += 1
            self.state.exec_book_visibility_delay_sum_ms += self.state.sampled_book_delay_ms
            self.state.exec_book_visibility_delay_max_ms = max(
                self.state.exec_book_visibility_delay_max_ms,
                self.state.sampled_book_delay_ms,
            )
            self.state.exec_depth_visibility_delay_sum_ms += self.state.sampled_depth_delay_ms
            self.state.exec_depth_visibility_delay_max_ms = max(
                self.state.exec_depth_visibility_delay_max_ms,
                self.state.sampled_depth_delay_ms,
            )
            self.state.exec_trade_visibility_delay_sum_ms += self.state.sampled_trade_delay_ms
            self.state.exec_trade_visibility_delay_max_ms = max(
                self.state.exec_trade_visibility_delay_max_ms,
                self.state.sampled_trade_delay_ms,
            )
            if bool(self.state.decision_book["paired_hit"]):
                self.state.exec_book_visibility_paired_hit_count += 1
            elif self.state.exec_book_visibility_mode == "paired":
                self.state.exec_book_visibility_paired_miss_count += 1
            if bool(self.state.decision_book["observed_mid_override"]):
                self.state.exec_book_visibility_mid_override_count += 1

        if (not self.state.decision_book.get("book_usable", True) or bool(self.state.exec_message_schedules) or (
                self.state.replay_event_clock != "empirical"
                and not self.state.use_bar_pricing and self.state.max_exec_book_age_ms > 0
                and (self.state.historical_bbo or self.state.historical_l2))):
            if self.state.exec_message_schedules:
                self.state.book_age_ms = float(self.state.decision_book["depth_visible_age_ms"])
                self.state.source_lag_ms = float(self.state.decision_book["depth_source_lag_ms"])
                self.state.stale_book = (
                    not self.services.math.isfinite(self.state.book_age_ms) or self.state.book_age_ms < 0.0
                    or self.state.book_age_ms > self.state.max_exec_book_visible_age_ms
                    or not self.services.math.isfinite(self.state.source_lag_ms) or self.state.source_lag_ms < 0.0
                    or self.state.source_lag_ms > self.state.max_exec_book_source_lag_ms
                )
            else:
                self.state.book_age_ms = self._current_book_age_ms(
                    int(self.state.t), bbo_cursor=self.state.decision_bbo_idx, l2_cursor=self.state.decision_l2_idx,
                )
                self.state.stale_book = self.state.book_age_ms is None or self.state.book_age_ms > self.state.max_exec_book_age_ms
            self.state.stale_book |= not self.state.decision_book.get("book_usable", True)
            if self.state.stale_book:
                self._cooldown_lineage_observe_early_blocker(int(self.state.t), "stale_book")
                self.state.stale_book_skip_count += 1
                self.state.age_for_stats = float(self.state.book_age_ms if self.state.book_age_ms is not None else self.state.max_exec_book_age_ms)
                self.state.stale_book_age_sum += self.state.age_for_stats
                if self.state.age_for_stats > self.state.stale_book_age_max:
                    self.state.stale_book_age_max = self.state.age_for_stats
                if self.state.bid_orders or self.state.ask_orders:
                    self.state.stale_book_cancel_count += 1
                self._request_cancel_all(self.state.bid_orders, self.state.t, "stale_book")
                self._request_cancel_all(self.state.ask_orders, self.state.t, "stale_book")
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
                return False
        return True

    def _refresh_quote_inputs(self) -> bool:
        """Advance causal signals and apply pre-quote closing and risk controls."""
        # Live signal state can lag the exchange clock independently of
        # BBO/L2. Keep this causal cursor monotonic just like a real
        # receive stream: an age spike cannot make SignalEngine unsee a
        # trade that it already processed.
        self.state.signal_visible_ts = max(
            int(self.state.exec_signal_visible_ts_ms),
            int(self.state.decision_book["visible_trade_ts"]),
        )
        self.state.exec_signal_visible_ts_ms = int(self.state.signal_visible_ts)
        self.state.variance_visible_ts = self.state.signal_visible_ts - 1_000
        if self.state.exec_message_schedules:
            self.state.variance_clock = self._message_clock_state("variance", int(self.state.t))
            self.state.variance_visible_ts = (
                self.state.variance_clock[1] // 1_000_000 if self.state.variance_clock[0] >= 0
                else int(self.context.var_ts_ms[0]) - 1 if len(self.context.var_ts_ms) else -1
            )

        # ── σ² lookup + per-bar EMA catch-up for dynamic RQ & BER ──
        while (self.state.var_idx < len(self.context.var_ts_ms) - 1
               and self.context.var_ts_ms[self.state.var_idx + 1] <= self.state.variance_visible_ts):
            self.state.var_idx += 1
            if self.state.var_idx == 0:
                # The first close anchors subsequent close differences;
                # it is not itself an observed squared return/EMA update.
                continue
            # Dynamic RQ: update fast/slow EMA of squared returns per bar
            if self.state.dynamic_rq and self.context.var_retsq is not None and not self.state.main_loop_enabled:
                self.state.rsq = self.context.var_retsq[self.state.var_idx]
                if not self.state.dyn_rq_inited:
                    self.state.ema_var_fast = self.state.rsq
                    self.state.ema_var_slow = self.state.rsq
                    self.state.dyn_rq_inited = True
                else:
                    self.state.ema_var_fast = 0.067 * self.state.rsq + 0.933 * self.state.ema_var_fast
                    self.state.ema_var_slow = 0.011 * self.state.rsq + 0.989 * self.state.ema_var_slow
            # BER: update trade intensity EMAs per bar
            if self.state.ber_guard_thresh > 0 and self.context.var_ti is not None:
                self.state.ti_val = self.state.ber_held_ti
                if not self.state.ber_inited:
                    self.state.ema_ti_fast = self.state.ti_val
                    self.state.ema_ti_slow = self.state.ti_val
                    self.state.ber_inited = True
                else:
                    self.state.ema_ti_fast = 0.13 * self.state.ti_val + 0.87 * self.state.ema_ti_fast
                    self.state.ema_ti_slow = 0.011 * self.state.ti_val + 0.989 * self.state.ema_ti_slow
                if (int(self.context.var_ts_ms[self.state.var_idx]) + 1000) % 10_000 == 0:
                    # SignalEngine publishes the mean aggregate count per
                    # 10s bar. ``var_ti`` is the trailing per-second mean,
                    # so the exact 60s feature scale is ten times larger.
                    self.state.ber_pending_ti = float(self.context.var_ti[self.state.var_idx]) * 10.0
                    self.state.ber_pending_feature_ts_ms = int(self.context.var_ts_ms[self.state.var_idx])

        if 0 <= self.state.var_idx < len(self.context.var_ssq):
            self.state.sigma_sq = self.context.var_ssq[self.state.var_idx]
        self.state.sigma_sq = max(self.state.sigma_sq, 1e-6)

        # Only complete, delivered 10s features may change liquidity.
        # In message mode this is the current feature publication clock,
        # not the prediction input frozen before signal computation.
        if self.context.var_ti is not None:
            self.state.quote_ti_cutoff_ms = min(self.state.signal_visible_ts, self.state.variance_visible_ts + 1_000)
            if "prediction" in self.state.exec_message_schedules:
                self.state.quote_ti_cutoff_ms = min(
                    self.state.quote_ti_cutoff_ms,
                    self._message_clock_state("prediction", int(self.state.t))[1] // 1_000_000,
                )
            while self.state.quote_ti_cursor < len(self.state.quote_ti_source_indices):
                self.state.quote_ti_source_idx = int(self.state.quote_ti_source_indices[self.state.quote_ti_cursor])
                self.state.completed_ts_ms = int(self.context.var_ts_ms[self.state.quote_ti_source_idx]) + 1_000
                if self.state.completed_ts_ms > self.state.quote_ti_cutoff_ms:
                    break
                self.state.cur_ti = float(self.context.var_ti[self.state.quote_ti_source_idx]) * 10.0
                self.state.quote_ti_completed_ts_ms = self.state.completed_ts_ms
                self.state.quote_ti_publish_count += 1
                self.state.quote_ti_cursor += 1

        # ── Dynamic RQ ──
        if (
            not self.state.main_loop_enabled and self.state.dynamic_rq and self.state.dyn_rq_inited
            and self.state.ema_var_slow > 1e-12 and self.state.nrq > 6
        ):
            self.state.vol_ratio = self.state.ema_var_fast / self.state.ema_var_slow
            self.state.vol_ratio = max(0.0, min(self.state.vol_ratio, 2.0))
            self.state.log_ratio = self.services.math.log(self.state.rq_min_ms / self.state.rq_max_ms)
            self.state.rq_f = self.state.rq_max_ms * self.services.math.exp(self.state.log_ratio * self.state.vol_ratio)
            self.state.cur_rq_ms = int(max(self.state.rq_min_ms, min(self.state.rq_max_ms, self.state.rq_f)))

        # ── BER ──
        if self.state.ber_guard_thresh > 0 and self.state.ber_inited:
            if self.state.ema_ti_slow > 1e-6:
                self.state.trade_intensity_acceleration_guard_active = (self.state.ema_ti_fast / self.state.ema_ti_slow) > self.state.ber_guard_thresh
            else:
                self.state.trade_intensity_acceleration_guard_active = False
        if self.state.ber_pending_feature_ts_ms > self.state.ber_published_feature_ts_ms:
            # Live callbacks update BER before the same requote calls
            # compute_signal(). Publish only for subsequent 1s callbacks.
            self.state.ber_held_ti = self.state.ber_pending_ti
            self.state.ber_published_feature_ts_ms = self.state.ber_pending_feature_ts_ms
            self.state.ber_feature_publish_count += 1

        # ── ML: lookup latest prediction ──
        if (
            self.state.use_ml
            or self.state.has_prediction_features
            or self.state.ranked_toxicity_guard_binding_active
        ):
            self.state.t_pred = (self.state.signal_visible_ts // 1000) * 1000
            if self.state.exec_message_schedules:
                self.state.t_pred = self._message_clock_state("prediction", int(self.state.t))[1] // 1_000_000
            if self.state.active_quote_compute is not None:
                self.state.t_pred = int(self.state.active_quote_compute["prediction_cutoff_ms"])
            while (self.state.ml_idx < len(self.state.ml_ts) - 1
                   and self.state.ml_ts[self.state.ml_idx + 1] <= self.state.t_pred):
                self.state.ml_idx += 1
            if self.state.ml_ts[self.state.ml_idx] <= self.state.t_pred:
                if self.state.use_ml:
                    self.state.cur_dir = self.state.ml_dir[self.state.ml_idx]
                    self.state.cur_vol = self.state.ml_vol[self.state.ml_idx]
                    self.state.cur_ret = self.state.ml_ret[self.state.ml_idx]
                if self.state.use_ml or self.state.ranked_toxicity_guard_binding_active:
                    if self.state.has_tox:
                        self.state.cur_tox_bid = self.state.ml_tox_bid[self.state.ml_idx]
                        self.state.cur_tox_ask = self.state.ml_tox_ask[self.state.ml_idx]
                    else:
                        self.state.cur_tox_bid = 1.0 - self.state.cur_dir
                        self.state.cur_tox_ask = self.state.cur_dir
                if self.state.use_ml:
                    if self.state.has_xmarket:
                        for loop_item in self.state.ml_xmarket.items():
                            self.state._xm_col, self.state._xm_vals = loop_item
                            self.state.cur_xmarket[self.state._xm_col] = float(self.state._xm_vals[self.state.ml_idx])
                if self.state.has_prediction_features:
                    self.state.cur_prediction_features = {}
                    for loop_item in self.state.ml_prediction_features.items():
                        self.state._feature_name, self.state._feature_values = loop_item
                        self.state._feature_value = self.state._feature_values[self.state.ml_idx]
                        if isinstance(self.state._feature_value, self.services.np.generic):
                            self.state._feature_value = self.state._feature_value.item()
                        self.state.cur_prediction_features[self.state._feature_name] = self.state._feature_value
                if self.state.ranked_toxicity_guard_binding_active:
                    for loop_item in range(
                        self.state.ranked_toxicity_guard_last_prediction_idx + 1,
                        self.state.ml_idx + 1,
                    ):
                        self.state.prediction_idx = loop_item
                        self.state.prediction_ts_ms = int(self.state.ml_ts[self.state.prediction_idx])
                        if self.state.prediction_ts_ms > int(self.state.t_pred):
                            break
                        self.context.ranked_toxicity_guard_binding.on_prediction_bucket(
                            prediction_bucket_ts_ms=self.state.prediction_ts_ms,
                            feature_ready_ts_ms=self.state.prediction_ts_ms,
                            observation_ts_ms=int(self.state.signal_visible_ts),
                            tox_bid=float(self.state.ml_tox_bid[self.state.prediction_idx]),
                            tox_ask=float(self.state.ml_tox_ask[self.state.prediction_idx]),
                        )
                        self.state.ranked_toxicity_guard_last_prediction_idx = (
                            self.state.prediction_idx
                        )

        self.state.quote_depth = self.services.DepthSnapshot()
        if self.state.historical_l2 and not self.state.use_bar_pricing:
            self.state.quote_depth = self.services.quote_depth_from_l2_rows(
                self.state.cur_l2_bid_px_row, self.state.cur_l2_bid_qty_row,
                self.state.cur_l2_ask_px_row, self.state.cur_l2_ask_qty_row,
            )
        self.state.quote_ret = self.state.cur_ret
        if self.state.use_ml and self.state.ret_skew > 0 and self.state.ret_demean_halflife > 0:
            self.state.ema_pr = self.state.alpha_dm * self.state.cur_ret + (1.0 - self.state.alpha_dm) * self.state.ema_pr
            self.state.quote_ret = self.state.cur_ret - self.state.ema_pr

        if (
            not self.state.circuit_breaker_closing and self.state.position_timeout_ms > 0
            and abs(self.state.q) >= self.state.LOT_SIZE and self.state.t - self.state.pos_open_ts >= self.state.position_timeout_ms
        ):
            # Live TIMEOUT_CLOSING cancels first and lets subsequent
            # control calls place reduce-only limit orders. A timeout is
            # not itself a fill and must not change cash or inventory.
            self.state.nto += 1
            self._start_maker_close(int(self.state.t), reason="position_timeout")
            return False

        if self.state.circuit_breaker_closing:
            self._cooldown_lineage_observe_early_blocker(
                int(self.state.t),
                "circuit_breaker_closing",
            )
            self._requote_circuit_breaker_close(int(self.state.t), float(self.state.mid))
            return False

        self.state.inventory_mark_price = float(self.state.decision_book.get("inventory_mark_price", self.state.mid))
        self.state.unrealized_pnl = (
            (self.state.inventory_mark_price - self.state.entry_price) * self.state.q
            if self.state.entry_price > 0.0 and self.state.inventory_mark_price > 0.0 else 0.0
        )
        if (
            abs(self.state.q) > 1e-10
            and self.services.circuit_breaker_triggered(
                self.state.unrealized_pnl,
                self.state.sigma_sq,
                self.state.pnl_volatility_horizon_s,
                self.state.q,
                self.state.circuit_breaker_sigma,
            )
        ):
            self._cooldown_lineage_observe_early_blocker(
                int(self.state.t),
                "circuit_breaker",
            )
            self.state.circuit_breaker_count += 1
            if self.state.circuit_breaker_exit_mode == "immediate_taker":
                self.state.aq_t = abs(self.state.q)
                if self.state.q > 0.0:
                    self.state.cash += self.state.mid * self.state.aq_t * (1.0 - self.state.taker_fee)
                    self.state.close_side = "SELL"
                else:
                    self.state.cash -= self.state.mid * self.state.aq_t * (1.0 + self.state.taker_fee)
                    self.state.close_side = "BUY"
                if self.state.private_fill_visibility_enabled:
                    self.state.exchange_inventory -= self.state.q
                self.state.q = 0.0
                self._loss_cooldown_on_fill(
                    self.state.close_side,
                    float(self.state.aq_t),
                    float(self.state.mid),
                    float(self.state.taker_fee),
                )
                self.state.post_fill_quote_response.reset()
                self.state.post_fill_quote_response_force_bid_requote = False
                self.state.post_fill_quote_response_force_ask_requote = False
                self._drop_orders(self.state.bid_orders, int(self.state.t), "circuit_breaker")
                self._drop_orders(self.state.ask_orders, int(self.state.t), "circuit_breaker")
                self.state.pos_open_ts = int(self.state.t)
                self.state.entry_price = 0.0
            else:
                self._start_maker_close(int(self.state.t), reason="circuit_breaker")
            return False

        if self.state.hard_risk_enabled:
            self.state.hard_risk_mark_price = self.state.inventory_mark_price
            self.state.risk_state.observe(int(self.state.t), self.state.cash, self.state.q, self.state.inventory_mark_price)
            self.state.risk_reason = self.services.hard_risk_reason(
                daily_pnl=self.state.risk_state.daily_pnl,
                position_value=abs(self.state.q) * self.state.mid, drawdown=self.state.risk_state.drawdown,
                max_daily_loss=self.state.max_daily_loss,
                max_position_value=self.state.max_position_value,
                emergency_close_dd=self.state.emergency_close_dd,
            )
            if self.state.risk_reason:
                self.state.risk_daily_loss_block_count += int(self.state.risk_reason == "daily_loss")
                self.state.risk_position_value_block_count += int(self.state.risk_reason == "position_value")
                if self.state.risk_reason == "emergency_drawdown":
                    self.state.risk_emergency_latched = True
                    self.state.risk_emergency_close_count += 1
                if self.state.trace_decisions is not None and len(self.state.trace_risk_actions) < self.state.trace_decisions_max:
                    self.state.trace_risk_actions.append({
                        "ts_ms": int(self.state.t), "reason": self.state.risk_reason,
                        "risk_action": "emergency_close" if self.state.risk_emergency_latched else "cancel_all_pause",
                        "daily_pnl": self.state.risk_state.daily_pnl,
                        "drawdown": self.state.risk_state.drawdown,
                        "position_value": abs(self.state.q) * self.state.mid,
                    })
                self._cooldown_lineage_observe_early_blocker(int(self.state.t), self.state.risk_reason)
                self._request_cancel_all(self.state.bid_orders, self.state.t, self.state.risk_reason)
                self._request_cancel_all(self.state.ask_orders, self.state.t, self.state.risk_reason)
                self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
                self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
                if self.state.risk_emergency_latched and self.state.async_rest_gateway:
                    if self.state.main_loop_waiting_request is not None:
                        self.state.main_loop_waiting_request["resume_close"] = {"action": "emergency"}
                    else:
                        self._attempt_async_emergency_close(int(self.state.t))
                return False

        self.state.loss_cooldown_transition = self.state.consecutive_loss_cooldown.on_policy_clock(int(self.state.t))
        if self.state.loss_cooldown_transition in {"triggered", "active"}:
            self._cooldown_lineage_observe_early_blocker(int(self.state.t), "consecutive_loss_cooldown")
            self._request_cancel_all(self.state.bid_orders, self.state.t, "consecutive_loss_cooldown")
            self._request_cancel_all(self.state.ask_orders, self.state.t, "consecutive_loss_cooldown")
            self._process_order_transitions(self.state.bid_orders, "BUY", self.state.t, self.state.p)
            self._process_order_transitions(self.state.ask_orders, "SELL", self.state.t, self.state.p)
            self.state.consecutive_loss_cooldown_cancel_count += 1
            self.state.consecutive_loss_cooldown.last_cancel_ts_ms = int(self.state.t)
            return False
        return True

    def _compute_quote_targets(self):
        """Call the shared quote core and retain its exact side context."""
        self.state.quote_state = self.services.QuoteState(
            mid=self.state.mid,
            inventory=self.state.q,
            sigma_sq=self.state.sigma_sq,
            trade_intensity=self.state.cur_ti,
            best_bid=self.state.cur_best_bid,
            best_ask=self.state.cur_best_ask,
            trade_intensity_acceleration_guard_active=self.state.trade_intensity_acceleration_guard_active,
            mo_ema_all=self.state.mo_ema_all,
            mo_ema_bid=self.state.mo_ema_bid,
            mo_ema_ask=self.state.mo_ema_ask,
            bid_adverse_markout_pause_latch=self._markout_pause_latch_active(
                True, int(self.state.t)
            ),
            ask_adverse_markout_pause_latch=self._markout_pause_latch_active(
                False, int(self.state.t)
            ),
            mo_ref=self.state.mo_ref,
            position_open=abs(self.state.q) > 1e-8,
            hold_time_s=max(0.0, (self.state.t - self.state.pos_open_ts) / 1000.0),
            unrealized_pnl=self.state.unrealized_pnl,
            defense_urgency_visible_state_consistent=(
                self.services.math.isfinite(self.state.q)
                and (abs(self.state.q) <= 1e-10 or self.state.entry_price > 0.0)
                and self.state.inventory_mark_price > 0.0
            ),
        )
        self.state.quote_prediction = self.services.QuotePrediction(
            touch_conditioned_up_probability_10000ms=self.state.cur_dir,
            absolute_price_variance_rate_10000ms=self.state.cur_vol,
            touch_conditioned_price_change_fraction_10000ms=self.state.quote_ret,
            tox_bid=self.state.cur_tox_bid,
            tox_ask=self.state.cur_tox_ask,
        )
        self.context.price_kwargs = {}
        if self.state.quote_price_runtime is not None:
            self.state.quote_price_runtime.begin_capture(int(self.state.t)*1_000_000)
            self.context.price_kwargs['price_correction'] = self.state.quote_price_runtime.correction
        self.state.global_ber_quote = self.services.compute_quote_core(
            self.state.quote_state,
            self.state.quote_core_cfg,
            self.state.quote_prediction,
            self.state.quote_depth,
            **self.context.price_kwargs,
        )
        self.state.quote_result = self.state.global_ber_quote
        self.state.ber_bypass_quote = self.state.global_ber_quote
        self.state.ber_buy_role = ""
        self.state.ber_sell_role = ""
        if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
            self.state.ber_bypass_quote = self.services.compute_quote_core(
                self.services.replace(self.state.quote_state, trade_intensity_acceleration_guard_active=False),
                self.state.quote_core_cfg,
                self.state.quote_prediction,
                self.state.quote_depth,
                **self.context.price_kwargs,
            )
            self.state.quote_result = self.services.compose_trade_intensity_acceleration_add_only_quote(
                acceleration_guard_quote=self.state.global_ber_quote,
                bypass_quote=self.state.ber_bypass_quote,
                inventory=self.state.q,
                target_buy_quantity=self.state.order_size,
                target_sell_quantity=self.state.order_size,
            )
            self.state.ber_buy_role = self.services.trade_intensity_acceleration_inventory_role_for_target(
                "BUY", self.state.q, self.state.order_size
            )
            self.state.ber_sell_role = self.services.trade_intensity_acceleration_inventory_role_for_target(
                "SELL", self.state.q, self.state.order_size
            )
            self.state.ber_role_safe_decision_count += 1
            self.state.ber_role_safe_buy_add_count += int(self.state.ber_buy_role == "add")
            self.state.ber_role_safe_sell_add_count += int(self.state.ber_sell_role == "add")
            self.state.ber_role_safe_flat_bypass_count += int(
                self.state.ber_buy_role == "opener" and self.state.ber_sell_role == "opener"
            )
            self.state.ber_role_safe_mixed_fail_closed_count += int(
                "mixed_cross_zero" in {self.state.ber_buy_role, self.state.ber_sell_role}
            )

            self.state.expected_bid = (
                self.state.global_ber_quote.bid_price
                if self.state.ber_buy_role in {"add", "mixed_cross_zero"}
                else self.state.ber_bypass_quote.bid_price
            )
            self.state.expected_ask = (
                self.state.global_ber_quote.ask_price
                if self.state.ber_sell_role in {"add", "mixed_cross_zero"}
                else self.state.ber_bypass_quote.ask_price
            )
            if (
                abs(self.state.quote_result.bid_price - self.state.expected_bid) > self.state.TICK * 1e-6
                or abs(self.state.quote_result.ask_price - self.state.expected_ask) > self.state.TICK * 1e-6
            ):
                self.state.ber_role_safe_source_mismatch_count += 1
        if self.state.quote_price_runtime is not None:
            self.context.price_extra_ms = self.state.quote_price_runtime.finish_capture()
            if type(self.context.price_extra_ms) is not int or self.context.price_extra_ms < 0:
                raise ValueError('quote-price processing cost must be nonnegative integer ms')
            if self.state.active_quote_compute is None:
                raise ValueError('quote-price capture lost original request clock')
            self.state.main_loop_tick_complete_ms += self.context.price_extra_ms
            self.state.active_quote_compute['request_ready_ts'] += self.context.price_extra_ms
            self.state.active_quote_compute['quote_price_extra_ms'] = self.context.price_extra_ms
            if self.state.quote_price_runtime.contract['schema'] == 'quote-price-replay-v2':
                panel = self.state.quote_price_runtime.capture_evidence()
                self.state.active_quote_compute['depth_quoting_capture_id'] = panel['capture_id']
                event = self.state.l2_production.observe('depth_quoting_capture', int(self.state.t))
                if self.state.l2_journal is not None:
                    self.state.l2_journal.emit('depth_quoting_capture', int(self.state.t),
                        payload=dict(panel, compute=dict(self.state.active_quote_compute),
                                     side_correction_index={side: (None if not panel['corrections'] else
                                         0 if len(panel['corrections']) == 1 or role in {'add', 'mixed_cross_zero'}
                                         else len(panel['corrections']) - 1)
                                         for side, role in (('BUY', self.state.ber_buy_role), ('SELL', self.state.ber_sell_role))},
                                     core_bid=float(self.state.quote_result.bid_price),
                                     core_ask=float(self.state.quote_result.ask_price),
                                     core_flags=dict(self.state.quote_result.quote_flags),
                                     old_orders=[dict(order_id=int(o['trace_id']), side=o['side'],
                                         price=float(o['price']), state=o['state'],
                                         exchange_accepted=bool(o.get('exchange_accepted', False)),
                                         exchange_cancel_effective=bool(o.get('exchange_cancel_effective', False)),
                                         fill_eligible=bool(o.get('fill_eligible', False)))
                                         for o in (*self.state.bid_orders, *self.state.ask_orders)]),
                        production_event=event)
        if self.state.trade_intensity_acceleration_guard_active:
            self.state.ber_active_count += 1

        self.state.global_ber_reference_bid = float(self.state.global_ber_quote.bid_price)
        self.state.global_ber_reference_ask = float(self.state.global_ber_quote.ask_price)
        self.state.diag = self.state.quote_result.diagnostics
        self.state.nbid = self.state.quote_result.bid_price
        self.state.nask = self.state.quote_result.ask_price
        self.state.fair = float(self.state.diag.get("fair", self.state.mid))
        self.state.d_pre_cap = float(self.state.diag.get("delta_pre_cap", 0.0))
        self.state.d = float(self.state.diag.get("delta_after_cap", self.state.quote_result.spread))
        self.state.cap_bps = float(self.state.diag.get("cap_bps", 0.0))
        self.state.delta_cap_hit = bool(self.state.diag.get("delta_cap_hit", False))
        self.state.cap_hit = bool(self.state.diag.get("cap_hit", False))
        self.state.final_compressed = bool(self.state.diag.get("final_compressed", False))
        self.state.mid_guard_bid = bool(self.state.diag.get("mid_guard_bid", False))
        self.state.mid_guard_ask = bool(self.state.diag.get("mid_guard_ask", False))
        self.state.post_only_bid = bool(self.state.diag.get("post_only_bid", False))
        self.state.post_only_ask = bool(self.state.diag.get("post_only_ask", False))
        self.state.bid_adverse_active = bool(self.state.diag.get("bid_adverse_active", False))
        self.state.bid_adverse_pause_active = bool(self.state.diag.get("bid_adverse_pause_active", False))
        self.state.ask_adverse_active = bool(self.state.diag.get("ask_adverse_active", False))
        self.state.ask_adverse_pause_active = bool(self.state.diag.get("ask_adverse_pause_active", False))
        self.state.bid_defense_active = bool(self.state.diag.get("bid_defense_active", False))
        self.state.bid_defense_pause_active = bool(self.state.diag.get("bid_defense_pause_active", False))
        self.state.ask_defense_active = bool(self.state.diag.get("ask_defense_active", False))
        self.state.ask_defense_pause_active = bool(self.state.diag.get("ask_defense_pause_active", False))
        self.state.quote_flags = dict(self.state.quote_result.quote_flags)
        self.state.quote_context = self.state.quote_result.quote_context

    def _apply_quote_side_policies(self):
        """Apply common and declared side policies to the computed targets."""

        self.state.bid_local_extreme_active, self.state.ask_local_extreme_active = self._apply_local_extreme_guard(
            self.state.quote_context,
            self.state.quote_flags,
            self.state.t,
            self.state.mid,
        )

        self.state.policy_now_ns = int(self.state.t) * 1_000_000
        (
            self.state.repair_probability,
            self.state.repair_probability_change,
            self.state.repair_probability_age_ms,
            self.state.repair_features,
        ) = self._inventory_lifecycle_repair_at_quote(int(self.state.t), self.state.quote_context)

        # Live parity: side policy widens quotes after quote_core, so replay
        # must apply the same policy before enforcing the final cap.
        self.state.bid_common_policy, self.state.bid_policy_mult = self._replay_common_side_policy(
            "BUY",
            self.state.quote_context["BUY"],
            mutate_state=self.state.bid_route_due,
        )
        self.state.ask_common_policy, self.state.ask_policy_mult = self._replay_common_side_policy(
            "SELL",
            self.state.quote_context["SELL"],
            mutate_state=self.state.ask_route_due,
        )
        self.state.quote_context["BUY"]["inventory_lifecycle_soft_control"] = False
        self.state.quote_context["SELL"]["inventory_lifecycle_soft_control"] = False
        self.state.quote_context["BUY"]["inventory_lifecycle_soft_spread_mult"] = 1.0
        self.state.quote_context["SELL"]["inventory_lifecycle_soft_spread_mult"] = 1.0
        self.state.bid_inventory_lifecycle_soft = bool(
            self.state.bid_route_due
            and self._inventory_lifecycle_soft_control_active(
                "BUY", int(self.state.t), self.state.quote_context["BUY"]
            )
        )
        self.state.ask_inventory_lifecycle_soft = bool(
            self.state.ask_route_due
            and self._inventory_lifecycle_soft_control_active(
                "SELL", int(self.state.t), self.state.quote_context["SELL"]
            )
        )
        if self.state.bid_inventory_lifecycle_soft:
            self.state.quote_context["BUY"]["inventory_lifecycle_soft_control"] = True
            self.state.quote_context["BUY"]["inventory_lifecycle_soft_spread_mult"] = self.state.inventory_lifecycle_soft_spread_mult
            self.state.bid_policy_mult = max(self.state.bid_policy_mult, self.state.inventory_lifecycle_soft_spread_mult)
            self.state.inventory_lifecycle_soft_control_count += 1
            self.state.bid_inventory_lifecycle_soft_control_count += 1
        if self.state.ask_inventory_lifecycle_soft:
            self.state.quote_context["SELL"]["inventory_lifecycle_soft_control"] = True
            self.state.quote_context["SELL"]["inventory_lifecycle_soft_spread_mult"] = self.state.inventory_lifecycle_soft_spread_mult
            self.state.ask_policy_mult = max(self.state.ask_policy_mult, self.state.inventory_lifecycle_soft_spread_mult)
            self.state.inventory_lifecycle_soft_control_count += 1
            self.state.ask_inventory_lifecycle_soft_control_count += 1

        if (
            self.state.bid_route_due
            and
            self.state.buy_soft_widen_release_probe_enabled
            and not self.state.buy_soft_widen_release_target_consumed
            and int(self.state.t) == self.state.buy_soft_widen_release_target_ts_ms
        ):
            self.state.buy_soft_widen_release_target_consumed = True
            self.state.hard_reason_mask = (
                self.services.POLICY_REASON_FILL_COOLDOWN
                | self.services.POLICY_REASON_STALE_HARD
                | self.services.POLICY_REASON_DEFENSE
                | self.services.POLICY_REASON_BURST
                | self.services.POLICY_REASON_INV_LIMIT
            )
            self.state.release_decision = self.services.evaluate_buy_soft_widen_release(
                enabled=True,
                decision_ts_ms=int(self.state.t),
                target_decision_ts_ms=self.state.buy_soft_widen_release_target_ts_ms,
                target_role=self.state.buy_soft_widen_release_target_role,
                inventory_btc=float(self.state.q),
                lot_size_btc=float(self.state.LOT_SIZE),
                allow_post=bool(self.state.bid_common_policy.allow_post),
                allow_exposure_increase=bool(
                    self.state.bid_common_policy.allow_exposure_increase
                ),
                hard_reason_active=bool(
                    self.state.bid_common_policy.reason_mask & self.state.hard_reason_mask
                ),
                baseline_spread_mult=float(self.state.bid_policy_mult),
                spread_mult_cap=self.state.buy_soft_widen_release_spread_mult_cap,
            )
            self.state.buy_soft_widen_release_target_reached_count += int(
                self.state.release_decision.target_reached
            )
            self.state.buy_soft_widen_release_eligible_count += int(
                self.state.release_decision.eligible
            )
            self.state.buy_soft_widen_release_requested_count += int(
                self.state.release_decision.requested
            )
            self.state.buy_soft_widen_release_effective_mult_count += int(
                self.state.release_decision.effective
            )
            self.state.buy_soft_widen_release_role_observed = self.state.release_decision.role
            self.state.buy_soft_widen_release_reason = self.state.release_decision.reason
            self.state.buy_soft_widen_release_baseline_spread_mult = float(
                self.state.release_decision.baseline_spread_mult
            )
            self.state.buy_soft_widen_release_selected_spread_mult = float(
                self.state.release_decision.selected_spread_mult
            )
            self.state.buy_soft_widen_release_baseline_bid_price = float(
                self._apply_replay_side_policy_price(
                    "BUY",
                    self.state.nbid,
                    self.state.mid,
                    self.state.release_decision.baseline_spread_mult,
                )
            )
            self.state.buy_soft_widen_release_candidate_bid_price = float(
                self._apply_replay_side_policy_price(
                    "BUY",
                    self.state.nbid,
                    self.state.mid,
                    self.state.release_decision.selected_spread_mult,
                )
            )
            self.state.buy_soft_widen_release_effective_price_count += int(
                abs(
                    self.state.buy_soft_widen_release_candidate_bid_price
                    - self.state.buy_soft_widen_release_baseline_bid_price
                )
                >= max(float(self.state.TICK) * 0.5, 1e-12)
            )
            if (
                self.state.buy_soft_widen_release_probe_apply_candidate
                and self.state.release_decision.effective
            ):
                self.state.bid_policy_mult = self.state.release_decision.selected_spread_mult

        self.state.quote_context["BUY"]["buy_fill_selection_live_score"] = 0.0
        self.state.quote_context["BUY"]["buy_fill_selection_live_hit"] = False
        self.state.quote_context["BUY"]["buy_fill_selection_live_threshold_hit"] = False
        self.state.quote_context["BUY"]["buy_fill_selection_live_missing_features"] = 0
        if (
            self.state.bid_route_due
            and self.state.buy_fill_selection_live_enabled
            and self.state.buy_fill_selection_model is not None
        ):
            self.state.bid_exposure_increasing_for_score = self.state.q >= 0.0
            if self.state.bid_exposure_increasing_for_score or self.state.buy_fill_selection_live_apply_reducing:
                self.state.base_bid_px = float(
                    self.state.quote_context["BUY"].get(
                        "pre_guard_price",
                        self.state.quote_context["BUY"].get("base_price", self.state.nbid),
                    )
                    or self.state.nbid
                )
                self.state.bid_allow_post_for_score = self.state.bid_common_policy.allow_post
                self.state.bid_allow_exposure_for_score = (
                    self.state.bid_common_policy.allow_exposure_increase
                )
                self.state.near_depth_for_score = max(
                    float(self.state.quote_context["BUY"].get("near_depth_total", 0.0) or 0.0),
                    float(self.state.quote_context["BUY"].get("l2_near_depth_total", 0.0) or 0.0),
                )
                self.state.score_features = self.services.build_fill_selection_feature_row(
                    prediction_features=self.state.cur_prediction_features,
                    quote_context=self.state.quote_context["BUY"],
                    side="BUY",
                    inventory=self.state.q,
                    max_inventory=self.state.max_inv,
                    mid=self.state.mid,
                    base_price=self.state.base_bid_px,
                    allow_post=self.state.bid_allow_post_for_score,
                    allow_exposure_increase=self.state.bid_allow_exposure_for_score,
                    exposure_increasing=self.state.bid_exposure_increasing_for_score,
                    near_depth_total=self.state.near_depth_for_score,
                    toxicity=self.state.cur_tox_bid,
                    markout_ema=self.state.mo_ema_bid,
                    queue_local_rank=float(
                        self.state.quote_context["BUY"].get("queue_local_rank", 0.5) or 0.5
                    ),
                    materialize=self.services.materialize_quote_ev_feature_values,
                )
                self.state.score_result = self.state.buy_fill_selection_model.score(self.state.score_features)
                self.state.buy_fill_selection_eval_count += 1
                self.state.buy_fill_selection_score_sum += float(self.state.score_result.score)
                self.state.buy_fill_selection_scores.append(float(self.state.score_result.score))
                self.state.buy_fill_selection_score_max = max(self.state.buy_fill_selection_score_max, float(self.state.score_result.score))
                self.state.buy_fill_selection_score_ge_042 += int(self.state.score_result.score >= 0.42)
                self.state.buy_fill_selection_score_ge_043 += int(self.state.score_result.score >= 0.43)
                self.state.buy_fill_selection_score_ge_044 += int(self.state.score_result.score >= 0.44)
                self.state.buy_fill_selection_score_ge_045 += int(self.state.score_result.score >= 0.45)
                self.state.threshold_hit = (
                    self.state.score_result.score >= self.state.buy_fill_selection_live_score_threshold
                    and self.state.score_result.missing_features <= self.state.buy_fill_selection_live_max_missing
                )
                self.state.actionable_hit = self.services.fill_selection_actionable(
                    threshold_hit=self.state.threshold_hit,
                    allow_post=self.state.bid_allow_post_for_score,
                    allow_exposure_increase=self.state.bid_allow_exposure_for_score,
                    hard_reason_active=bool(
                        self.state.bid_common_policy.reason_mask
                        & (
                            self.services.POLICY_REASON_FILL_COOLDOWN
                            | self.services.POLICY_REASON_STALE_HARD
                            | self.services.POLICY_REASON_DEFENSE
                            | self.services.POLICY_REASON_BURST
                            | self.services.POLICY_REASON_INV_LIMIT
                        )
                    ),
                )
                self.state.quote_context["BUY"]["buy_fill_selection_live_score"] = float(self.state.score_result.score)
                self.state.quote_context["BUY"]["buy_fill_selection_live_missing_features"] = int(self.state.score_result.missing_features)
                self.state.quote_context["BUY"]["buy_fill_selection_live_threshold_hit"] = bool(self.state.threshold_hit)
                self.state.quote_context["BUY"]["buy_fill_selection_live_hit"] = bool(self.state.actionable_hit)
                if self.state.actionable_hit:
                    self.state.buy_fill_selection_hit_count += 1
                    self.state.bid_policy_mult = min(self.state.bid_policy_mult, self.state.buy_fill_selection_live_spread_mult_cap)
        if self.context.public_strategy is not None:
            self.state.public_actions = self.context.public_strategy.decide(int(self.state.t) * 1_000_000)
            self.state.bid_policy_mult *= self.state.public_actions["BUY"]["spread_mult"]
            self.state.ask_policy_mult *= self.state.public_actions["SELL"]["spread_mult"]
        self.state.nbid = self._apply_replay_side_policy_price("BUY", self.state.nbid, self.state.mid, self.state.bid_policy_mult)
        self.state.nask = self._apply_replay_side_policy_price("SELL", self.state.nask, self.state.mid, self.state.ask_policy_mult)
        if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
            self.state.global_ber_reference_bid = self._apply_replay_side_policy_price(
                "BUY", self.state.global_ber_reference_bid, self.state.mid, self.state.bid_policy_mult
            )
            self.state.global_ber_reference_ask = self._apply_replay_side_policy_price(
                "SELL", self.state.global_ber_reference_ask, self.state.mid, self.state.ask_policy_mult
            )
            self.state.ber_bypass_reference_bid = self._apply_replay_side_policy_price(
                "BUY", self.state.ber_bypass_quote.bid_price, self.state.mid, self.state.bid_policy_mult
            )
            self.state.ber_bypass_reference_ask = self._apply_replay_side_policy_price(
                "SELL", self.state.ber_bypass_quote.ask_price, self.state.mid, self.state.ask_policy_mult
            )
        else:
            self.state.ber_bypass_reference_bid = self.state.global_ber_reference_bid
            self.state.ber_bypass_reference_ask = self.state.global_ber_reference_ask

    def _apply_quote_response(self):
        """Apply the original response policy and final side-preserving caps."""
        self.state.max_spread_post_policy = float(self.state.diag.get("max_spread", 0.0) or 0.0)
        self.state.add_side = "BUY" if self.state.q > 1e-12 else "SELL" if self.state.q < -1e-12 else ""
        self.state.add_ctx = self.state.quote_context.get(self.state.add_side, {}) if self.state.add_side else {}
        self.state.refill_edge = float(
            self.state.add_ctx.get("l2_book_refresh_ratio", 0.0) or 0.0
        ) - float(self.state.add_ctx.get("l2_book_cancel_ratio", 0.0) or 0.0)
        self.state.response_state = None
        if not self.state.pair_route_due:
            self.state.response_state = (
                self.state.post_fill_quote_response._add_side,
                self.state.post_fill_quote_response._excitation,
                self.state.post_fill_quote_response._last_update_ms,
                self.state.post_fill_quote_response._last_half_life_s,
            )
        try:
            self.state.response_decision = self.state.post_fill_quote_response.quote(
                now_ms=int(self.state.t),
                inventory=float(self.state.q),
                order_size=float(self.state.order_size),
                baseline_bid=float(self.state.nbid),
                baseline_ask=float(self.state.nask),
                tick_size=float(self.state.TICK),
                max_pair_spread=self.state.max_spread_post_policy,
                best_bid=float(self.state.cur_best_bid),
                best_ask=float(self.state.cur_best_ask),
                volatility_bps=(
                    self.services.math.sqrt(max(float(self.state.sigma_sq), 0.0))
                    / max(float(self.state.mid), 1e-12)
                    * 10_000.0
                ),
                refill_edge=self.state.refill_edge,
                repair_probability=self.state.repair_probability,
            )
        finally:
            if self.state.response_state is not None:
                (
                    self.state.post_fill_quote_response._add_side,
                    self.state.post_fill_quote_response._excitation,
                    self.state.post_fill_quote_response._last_update_ms,
                    self.state.post_fill_quote_response._last_half_life_s,
                ) = self.state.response_state
        if (
            self.state.ber_exposure_add_only
            and self.state.trade_intensity_acceleration_guard_active
            and self.state.post_fill_quote_response_cfg.enabled
        ):
            raise ValueError(
                "role-safe BER current-stack identity requires the frozen "
                "post-fill quote response to remain disabled"
            )
        if (
            self.state.pair_route_due
            and self.state.post_fill_quote_response_cfg.enabled
            and abs(self.state.q) > 1e-12
        ):
            self.state.post_fill_quote_response_eval_count += 1
            self.state.post_fill_quote_response_half_life_sum += (
                self.state.response_decision.effective_half_life_s
            )
            self.state.post_fill_quote_response_inventory_ticks_sum += (
                self.state.response_decision.inventory_shift_ticks
            )
            self.state.post_fill_quote_response_add_ticks_sum += (
                self.state.response_decision.add_widen_ticks
            )
            self.state.post_fill_quote_response_add_ticks_max = max(
                self.state.post_fill_quote_response_add_ticks_max,
                float(self.state.response_decision.add_widen_ticks),
            )
            self.state.post_fill_quote_response_active_count += int(self.state.response_decision.active)
            self.state.post_fill_quote_response_inventory_count += int(
                self.state.response_decision.inventory_shift_ticks > 0
            )
            self.state.post_fill_quote_response_flow_count += int(
                self.state.response_decision.add_widen_ticks > 0
            )
            self.state.post_fill_quote_response_cap_limited_count += int(
                self.state.response_decision.cap_limited
            )
            self.state.post_fill_quote_response_post_only_clamp_count += int(
                self.state.response_decision.post_only_clamped
            )

        for loop_item in ("BUY", "SELL"):
            self.state.side_name = loop_item
            self.state.side_ctx = self.state.quote_context[self.state.side_name]
            self.state.side_ctx["post_fill_quote_response_enabled"] = bool(
                self.state.post_fill_quote_response_cfg.enabled
            )
            self.state.side_ctx["post_fill_quote_response_mode"] = self.state.response_decision.mode
            self.state.side_ctx["post_fill_quote_response_active"] = bool(
                self.state.response_decision.active
            )
            self.state.side_ctx["post_fill_quote_response_role"] = (
                "add"
                if self.state.side_name == self.state.response_decision.add_side
                else "reducing"
                if self.state.side_name == self.state.response_decision.reducing_side
                else "flat"
            )
            self.state.side_ctx["post_fill_inventory_shift_ticks"] = int(
                self.state.response_decision.inventory_shift_ticks
            )
            self.state.side_ctx["post_fill_raw_add_widen_ticks"] = int(
                self.state.response_decision.raw_add_widen_ticks
            )
            self.state.side_ctx["post_fill_add_widen_ticks"] = int(
                self.state.response_decision.add_widen_ticks
            )
            self.state.side_ctx["post_fill_flow_amplitude_mode"] = str(
                self.state.response_decision.flow_amplitude_mode
            )
            self.state.side_ctx["post_fill_baseline_add_distance_ticks"] = int(
                self.state.response_decision.baseline_add_distance_ticks
            )
            self.state.side_ctx["post_fill_response_excitation"] = float(
                self.state.response_decision.excitation
            )
            self.state.side_ctx["post_fill_response_half_life_s"] = float(
                self.state.response_decision.effective_half_life_s
            )
            self.state.side_ctx["post_fill_response_refill_edge"] = float(
                self.state.response_decision.refill_edge
            )
            self.state.side_ctx["post_fill_response_repair_probability"] = float(
                self.state.response_decision.repair_probability
            )
            self.state.side_ctx["post_fill_response_cap_headroom_ticks"] = int(
                self.state.response_decision.cap_headroom_ticks
            )
            self.state.side_ctx["post_fill_response_cap_limited"] = bool(
                self.state.response_decision.cap_limited
            )
            self.state.side_ctx["post_fill_response_post_only_clamped"] = bool(
                self.state.response_decision.post_only_clamped
            )
            self.state.side_ctx["post_fill_response_base_price"] = float(
                self.state.response_decision.baseline_bid
                if self.state.side_name == "BUY"
                else self.state.response_decision.baseline_ask
            )
        self.state.quote_flags["post_fill_quote_response"] = bool(
            self.state.response_decision.active
        )
        if (
            self.state.trace_post_fill_quote_response is not None
            and len(self.state.trace_post_fill_quote_response)
            < self.state.trace_post_fill_quote_response_max
            and self.state.post_fill_quote_response_cfg.enabled
            and self.state.pair_route_due
        ):
            self.state.trace_post_fill_quote_response.append(
                {
                    "timestamp": float(self.state.t) / 1000.0,
                    "ts_ms": int(self.state.t),
                    "inventory_lifecycle_id": int(self.state.inventory_lifecycle_count) if self.state.inventory_lifecycle_active else 0,
                    "inventory": float(self.state.q),
                    "mode": self.state.response_decision.mode,
                    "active": int(self.state.response_decision.active),
                    "add_side": self.state.response_decision.add_side,
                    "reducing_side": self.state.response_decision.reducing_side,
                    "baseline_bid": self.state.response_decision.baseline_bid,
                    "baseline_ask": self.state.response_decision.baseline_ask,
                    "bid_price": self.state.response_decision.bid_price,
                    "ask_price": self.state.response_decision.ask_price,
                    "inventory_shift_ticks": self.state.response_decision.inventory_shift_ticks,
                    "raw_add_widen_ticks": self.state.response_decision.raw_add_widen_ticks,
                    "add_widen_ticks": self.state.response_decision.add_widen_ticks,
                    "flow_amplitude_mode": self.state.response_decision.flow_amplitude_mode,
                    "baseline_add_distance_ticks": (
                        self.state.response_decision.baseline_add_distance_ticks
                    ),
                    "excitation": self.state.response_decision.excitation,
                    "effective_half_life_s": self.state.response_decision.effective_half_life_s,
                    "volatility": self.services.math.sqrt(max(float(self.state.sigma_sq), 1e-12)),
                    "refill_edge": self.state.response_decision.refill_edge,
                    "repair_probability": self.state.response_decision.repair_probability,
                    "repair_probability_change": self.state.repair_probability_change,
                    "cap_headroom_ticks": self.state.response_decision.cap_headroom_ticks,
                    "cap_limited": int(self.state.response_decision.cap_limited),
                    "post_only_clamped": int(
                        self.state.response_decision.post_only_clamped
                    ),
                }
            )
        self.state.nbid = self.state.response_decision.bid_price
        self.state.nask = self.state.response_decision.ask_price

        self.state.mirror_eligible = (
            self.state.pair_route_due
            and self.state.random_passive_enabled
            and (
                not self.state.random_passive_preserve_inventory_skew
                or abs(self.state.q) <= self.state.LOT_SIZE * 0.5
            )
        )
        if self.state.mirror_eligible:
            self.state.random_passive_mirror_eligible_count += 1
            if self.services.deterministic_random_passive_unit(
                seed=self.state.random_passive_seed,
                event_ts_ms=int(self.state.t),
                action_identity=int(self.state.i),
                operation=self.services._RANDOM_PASSIVE_SIDE_MIRROR,
            ) < self.state.random_passive_side_mirror_prob:
                self.state.bid_distance = max(self.state.mid - self.state.nbid, self.state.TICK)
                self.state.ask_distance = max(self.state.nask - self.state.mid, self.state.TICK)
                self.state.nbid = self.services.math.floor((self.state.mid - self.state.ask_distance) / self.state.TICK) * self.state.TICK
                self.state.nask = self.services.math.ceil((self.state.mid + self.state.bid_distance) / self.state.TICK) * self.state.TICK
                self.state.nbid = min(self.state.nbid, self.state.mid - self.state.TICK)
                self.state.nask = max(self.state.nask, self.state.mid + self.state.TICK)
                self.state.random_passive_mirror_count += 1
                self.state.quote_context["BUY"]["random_passive_mirrored"] = True
                self.state.quote_context["SELL"]["random_passive_mirrored"] = True

                if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
                    self.state.global_bid_distance = max(
                        self.state.mid - self.state.global_ber_reference_bid, self.state.TICK
                    )
                    self.state.global_ask_distance = max(
                        self.state.global_ber_reference_ask - self.state.mid, self.state.TICK
                    )
                    self.state.global_ber_reference_bid = self.services.math.floor(
                        (self.state.mid - self.state.global_ask_distance) / self.state.TICK
                    ) * self.state.TICK
                    self.state.global_ber_reference_ask = self.services.math.ceil(
                        (self.state.mid + self.state.global_bid_distance) / self.state.TICK
                    ) * self.state.TICK
                    self.state.global_ber_reference_bid = min(
                        self.state.global_ber_reference_bid, self.state.mid - self.state.TICK
                    )
                    self.state.global_ber_reference_ask = max(
                        self.state.global_ber_reference_ask, self.state.mid + self.state.TICK
                    )

                    self.state.bypass_bid_distance = max(
                        self.state.mid - self.state.ber_bypass_reference_bid, self.state.TICK
                    )
                    self.state.bypass_ask_distance = max(
                        self.state.ber_bypass_reference_ask - self.state.mid, self.state.TICK
                    )
                    self.state.ber_bypass_reference_bid = self.services.math.floor(
                        (self.state.mid - self.state.bypass_ask_distance) / self.state.TICK
                    ) * self.state.TICK
                    self.state.ber_bypass_reference_ask = self.services.math.ceil(
                        (self.state.mid + self.state.bypass_bid_distance) / self.state.TICK
                    ) * self.state.TICK
                    self.state.ber_bypass_reference_bid = min(
                        self.state.ber_bypass_reference_bid, self.state.mid - self.state.TICK
                    )
                    self.state.ber_bypass_reference_ask = max(
                        self.state.ber_bypass_reference_ask, self.state.mid + self.state.TICK
                    )

        self.state.global_ref_bid_cap = self.state.global_ber_reference_bid
        self.state.global_ref_ask_cap = self.state.global_ber_reference_ask
        self.state.global_ref_cap_hit = False
        if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
            (
                self.state.global_ref_bid_cap,
                self.state.global_ref_ask_cap,
                self.state.global_ref_cap_hit,
                self.state._,
            ) = self.services.apply_final_spread_cap(
                self.state.mid,
                self.state.global_ber_reference_bid,
                self.state.global_ber_reference_ask,
                self.state.max_spread_post_policy,
                self.state.TICK,
            )

        self.state.preserve_side = ""
        if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
            if self.state.ber_buy_role == "add" and self.state.ber_sell_role == "reducing":
                self.state.preserve_side = "SELL"
            elif self.state.ber_sell_role == "add" and self.state.ber_buy_role == "reducing":
                self.state.preserve_side = "BUY"

        self.state.role_safe_cap_feasible = True
        if self.state.preserve_side:
            (
                self.state.nbid_cap,
                self.state.nask_cap,
                self.state.post_policy_cap_hit,
                self.state.post_policy_cap_excess,
                self.state.role_safe_cap_feasible,
            ) = self.services.apply_final_spread_cap_preserve_side(
                self.state.mid,
                self.state.nbid,
                self.state.nask,
                self.state.max_spread_post_policy,
                self.state.TICK,
                preserve_side=self.state.preserve_side,
            )
            self.state.ber_role_safe_cap_collision_count += int(self.state.post_policy_cap_hit)
            self.state.ber_role_safe_cap_infeasible_count += int(
                self.state.post_policy_cap_hit and not self.state.role_safe_cap_feasible
            )
        else:
            (
                self.state.nbid_cap,
                self.state.nask_cap,
                self.state.post_policy_cap_hit,
                self.state.post_policy_cap_excess,
            ) = self.services.apply_final_spread_cap(
                self.state.mid, self.state.nbid, self.state.nask, self.state.max_spread_post_policy, self.state.TICK
            )
        self.state.post_policy_cap_compressed = False
        if self.state.post_policy_cap_hit:
            self.state.post_policy_cap_count += 1
            self.state.post_policy_cap_excess_sum += self.state.post_policy_cap_excess
            if self.state.post_policy_cap_excess > self.state.post_policy_cap_excess_max:
                self.state.post_policy_cap_excess_max = self.state.post_policy_cap_excess
            if not self.state.cap_hit:
                self.state.cap_hit_count += 1
                self.state.cap_hit = True
            if self.state.spread_cap_action_code == self.services.SPREAD_CAP_COMPRESS:
                if not self.state.final_compressed:
                    self.state.final_cap_compress_count += 1
                    self.state.final_cap_excess_sum += self.state.post_policy_cap_excess
                    if self.state.post_policy_cap_excess > self.state.final_cap_excess_max:
                        self.state.final_cap_excess_max = self.state.post_policy_cap_excess
                if self.state.preserve_side and not self.state.role_safe_cap_feasible:
                    self.state.nbid, self.state.nask = self.state.global_ref_bid_cap, self.state.global_ref_ask_cap
                else:
                    self.state.nbid, self.state.nask = self.state.nbid_cap, self.state.nask_cap
                self.state.final_compressed = True
                self.state.post_policy_cap_compressed = True
            elif self.state.spread_cap_action_code == self.services.SPREAD_CAP_PAUSE_EXPOSURE:
                self.state.quote_context["BUY"]["cap_exposure_block"] = bool(self.state.q >= 0.0)
                self.state.quote_context["SELL"]["cap_exposure_block"] = bool(self.state.q <= 0.0)

        if (
            self.state.ber_exposure_add_only
            and self.state.trade_intensity_acceleration_guard_active
            and self.state.spread_cap_action_code == self.services.SPREAD_CAP_COMPRESS
            and self.state.global_ref_cap_hit
        ):
            self.state.global_ber_reference_bid = self.state.global_ref_bid_cap
            self.state.global_ber_reference_ask = self.state.global_ref_ask_cap

        if self.state.ber_exposure_add_only and self.state.trade_intensity_acceleration_guard_active:
            self.state.bid_changed = abs(self.state.nbid - self.state.global_ber_reference_bid) > self.state.TICK * 1e-6
            self.state.ask_changed = abs(self.state.nask - self.state.global_ber_reference_ask) > self.state.TICK * 1e-6
            self.state.ber_role_safe_bid_change_count += int(self.state.bid_changed)
            self.state.ber_role_safe_ask_change_count += int(self.state.ask_changed)
            self.state.ber_role_safe_pair_change_count += int(self.state.bid_changed or self.state.ask_changed)

            if self.state.role_safe_cap_feasible:
                self.state.expected_non_add = (
                    self.state.ber_bypass_reference_ask
                    if self.state.preserve_side == "SELL"
                    else (
                        self.state.ber_bypass_reference_bid
                        if self.state.preserve_side == "BUY"
                        else None
                    )
                )
                self.state.observed_non_add = (
                    self.state.nask
                    if self.state.preserve_side == "SELL"
                    else self.state.nbid if self.state.preserve_side == "BUY" else None
                )
                if (
                    self.state.expected_non_add is not None
                    and self.state.observed_non_add is not None
                    and abs(self.state.observed_non_add - self.state.expected_non_add) > self.state.TICK * 1e-6
                ):
                    self.state.ber_role_safe_source_mismatch_count += 1
                self.state.quote_flags["cap_exposure_block"] = True

        self._refresh_final_quote_context(
            self.state.quote_context,
            self.state.quote_flags,
            self.state.mid,
            self.state.fair,
            self.state.nbid,
            self.state.nask,
            post_policy_cap=self.state.post_policy_cap_compressed,
        )

    def _finish_quote_decision(self):
        """Restore exchange truth after the decision and sample account state."""
        self.state.bid_top_order = self._best_live_order(self.state.bid_orders, "BUY")
        self.state.ask_top_order = self._best_live_order(self.state.ask_orders, "SELL")
        self.state.hb = self.state.bid_top_order is not None
        self.state.ha = self.state.ask_top_order is not None
        self.state.bp = self.state.bid_top_order["price"] if self.state.hb else 0.0
        self.state.ap = self.state.ask_top_order["price"] if self.state.ha else 0.0

        # Update mid reference
        if self.state.hb and self.state.ha and self.state.ap > self.state.bp:
            self.state.final_spread = self.state.ap - self.state.bp
            self.state.bid_dist = self.state.p - self.state.bp
            self.state.ask_dist = self.state.ap - self.state.p
            self.state.tsprd_final += (self.state.ap - self.state.bp)
            self.state.n_final_spread += 1
            self.state.delta_after_cap_sum += (self.state.ap - self.state.bp)
            self.state.bid_dist_sum += self.state.bid_dist
            self.state.ask_dist_sum += self.state.ask_dist
            self.state.quote_skew = (self.state.ask_dist - self.state.bid_dist) / self.state.final_spread
            self.state.quote_abs_skew = abs(self.state.quote_skew)
            self.state.quote_skew_sum += self.state.quote_skew
            self.state.quote_abs_skew_sum += self.state.quote_abs_skew
            if self.state.quote_abs_skew >= 0.10:
                self.state.quote_abs_skew_gt_10_count += 1
            if self.state.quote_abs_skew >= 0.25:
                self.state.quote_abs_skew_gt_25_count += 1
            if self.state.bid_dist + self.state.TICK < self.state.ask_dist:
                self.state.bid_tighter_count += 1
            elif self.state.ask_dist + self.state.TICK < self.state.bid_dist:
                self.state.ask_tighter_count += 1
            else:
                self.state.quote_balanced_count += 1
            if self.state.final_spread < 100.0:
                self.state.final_spread_lt_100_count += 1
            if self.state.final_spread < 150.0:
                self.state.final_spread_lt_150_count += 1

        (
            self.state.cur_best_bid,
            self.state.cur_best_ask,
            self.state.cur_l2_bid_px_row,
            self.state.cur_l2_bid_qty_row,
            self.state.cur_l2_ask_px_row,
            self.state.cur_l2_ask_qty_row,
        ) = self.state.truth_book_state
        self.state.active_decision_bbo_idx = None
        if self.state.cur_best_bid > 0.0 and self.state.cur_best_ask > self.state.cur_best_bid:
            self.state.mid = 0.5 * (self.state.cur_best_bid + self.state.cur_best_ask)
        else:
            self.state.mid = self.state.p

        self.state.tsprd += self.state.d
        if self.state.d < 100.0:
            self.state.quote_spread_lt_100_count += 1
        if self.state.d < 150.0:
            self.state.quote_spread_lt_150_count += 1
        self.state.nrq += 1
        self.state.rq_sum += self.state.cur_rq_ms

        # Sample PnL
        self.context.ensure_runtime_sample_capacity(self.state)
        self.state.pnl_arr[self.state.si] = self.state.cash + self.state.q * self.state.mid
        self.state.inv_arr[self.state.si] = self.state.q
        self.state.ts_arr[self.state.si] = self.state.t
        self.state.si += 1
