"""Replay fill policy owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any


class FillPolicy:
    def _safe_add_rearm_sample_latency_ms(self) -> int:
        """Sample latency without consuming the baseline order RNG stream."""

        if self.state.new_latency_samples_ms.size > 0:
            latency = float(
                self.state.new_latency_samples_ms[
                    int(self.state.safe_add_rearm_latency_rng.integers(0, self.state.new_latency_samples_ms.size))
                ]
            )
        else:
            latency = float(self.state.new_order_latency_ms)
        if self.state.latency_jitter_ms > 0:
            latency += float(
                self.state.safe_add_rearm_latency_rng.uniform(
                    -self.state.latency_jitter_ms,
                    self.state.latency_jitter_ms,
                )
            )
        return max(0, int(round(latency)))

    def _safe_add_rearm_sample_cancel_latency_ms(self) -> int:
        """Sample cancel ACK latency without consuming the baseline RNG stream."""

        if self.state.cancel_latency_samples_ms.size > 0:
            latency = float(
                self.state.cancel_latency_samples_ms[
                    int(
                        self.state.safe_add_rearm_latency_rng.integers(
                            0,
                            self.state.cancel_latency_samples_ms.size,
                        )
                    )
                ]
            )
        else:
            latency = float(self.state.cancel_order_latency_ms)
        if self.state.latency_jitter_ms > 0:
            latency += float(
                self.state.safe_add_rearm_latency_rng.uniform(
                    -self.state.latency_jitter_ms,
                    self.state.latency_jitter_ms,
                )
            )
        return max(0, int(round(latency)))

    def _safe_add_rearm_fill_eligible(self, side: str, target_ts: int) -> bool:
        side_prob = 1.0
        if len(self.state.trade_ts_seed) > 0:
            idx = self.services.np.searchsorted(self.state.trade_ts_seed, target_ts, side="right") - 1
            if idx >= 0:
                side_prob = (
                    self.state.buy_fill_prob_arr[idx]
                    if side == "BUY"
                    else self.state.sell_fill_prob_arr[idx]
                )
        probability = max(0.0, min(1.0, float(side_prob) * self.state.maker_fill_prob))
        if probability >= 1.0:
            return True
        if probability <= 0.0:
            return False
        return bool(self.state.safe_add_rearm_rng.random() <= probability)

    def _safe_add_rearm_finalize(self, order: dict, outcome: str, now_ts: int, **values) -> None:
        if self.state.trace_safe_add_rearm is None:
            return
        row = dict(order["trace_row"])
        row.update(
            {
                "outcome": str(outcome),
                "outcome_ts_ms": int(now_ts),
                "lifetime_ms": int(max(0, now_ts - int(order["submit_ts"]))),
                "queue_init": float(order.get("queue_init", 0.0) or 0.0),
                "queue_left": float(order.get("queue_left", 0.0) or 0.0),
                "fill_eligible": int(bool(order.get("fill_eligible", True))),
                **values,
            }
        )
        if len(self.state.trace_safe_add_rearm) < self.state.trace_safe_add_rearm_max:
            self.state.trace_safe_add_rearm.append(row)

    def _maybe_submit_safe_add_rearm_probe(self,
        side: str,
        now_ts: int,
        *,
        final_price: float,
        final_size: float,
        side_ctx: dict,
        block_reasons: list[str],
        can_post_after_inventory: bool,
        exposure_increasing: bool,
        side_orders: list[dict],
    ) -> None:
        """Create independent one-requote-cycle probes; never mutate live orders."""

        if self.state.trace_safe_add_rearm is None:
            return
        if len(self.state.trace_safe_add_rearm) + len(self.state.safe_add_rearm_orders) >= self.state.trace_safe_add_rearm_max:
            return
        last_fill_ts = self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
        cooldown_total_ms = (
            self.state.last_buy_fill_cooldown_total_ms
            if side == "BUY"
            else self.state.last_sell_fill_cooldown_total_ms
        )
        consecutive_units = self.state.consec_buy if side == "BUY" else self.state.consec_sell
        if cooldown_total_ms <= 0.0 or last_fill_ts <= 0:
            return
        elapsed_ms = max(0, int(now_ts - last_fill_ts))
        due_bins = [
            value
            for value in self.state.safe_add_rearm_probe_bins_ms
            if value <= int(round(cooldown_total_ms)) and elapsed_ms >= value
        ]
        baseline_end_ms = max(0, int(round(cooldown_total_ms)))
        if elapsed_ms >= baseline_end_ms:
            due_bins.append(baseline_end_ms)
        for scheduled_ms in sorted(set(due_bins)):
            emitted_key = (side, int(last_fill_ts), int(scheduled_ms))
            if emitted_key in self.state.safe_add_rearm_emitted:
                continue
            self.state.safe_add_rearm_emitted.add(emitted_key)
            labels = [f"elapsed_{scheduled_ms / 1000.0:g}s"]
            baseline_end = int(abs(scheduled_ms - baseline_end_ms) <= 1)
            if baseline_end:
                labels.append("baseline_cooldown_end")
            reason_set = {str(reason) for reason in block_reasons if reason}
            other_reasons = sorted(reason_set - {"fill_cd"})
            early_probe = scheduled_ms < baseline_end_ms
            actionable = bool(
                exposure_increasing
                and can_post_after_inventory
                and final_price > 0.0
                and final_size >= self.state.LOT_SIZE
                and not other_reasons
                and (not early_probe or "fill_cd" in reason_set)
                and not side_orders
            )
            probe_id = self.state.safe_add_rearm_next_id
            self.state.safe_add_rearm_next_id += 1
            trace_row = {
                "probe_id": int(probe_id),
                "episode_side": side,
                "episode_fill_ts_ms": int(last_fill_ts),
                "scheduled_elapsed_ms": int(scheduled_ms),
                "actual_elapsed_ms": int(elapsed_ms),
                "probe_label": "|".join(labels),
                "baseline_cooldown_end": baseline_end,
                "cooldown_total_ms": float(cooldown_total_ms),
                "cooldown_remaining_ms": float(
                    max(0.0, cooldown_total_ms - elapsed_ms)
                ),
                "consecutive_units": float(consecutive_units),
                "submit_ts_ms": int(now_ts),
                "side": side,
                "candidate_price": float(final_price),
                "candidate_size": float(final_size),
                "inventory": float(self.state.q),
                "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
                "inventory_lifecycle_id": int(self.state.inventory_lifecycle_count) if self.state.inventory_lifecycle_active else 0,
                "inventory_lifecycle_side": self.services.inventory_lifecycle_side(self.state.q),
                "inventory_lifecycle_age_s": (
                    max(0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1000.0)
                    if self.state.inventory_lifecycle_active
                    else 0.0
                ),
                "inventory_lifecycle_max_abs_qty_so_far": float(self.state.inventory_lifecycle_cur_max_abs),
                "inventory_lifecycle_pnl_so_far": (
                    float(self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl)
                    if self.state.inventory_lifecycle_active
                    else 0.0
                ),
                "inventory_lifecycle_adverse_excursion_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
                "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                    self.state.inventory_lifecycle_exposure_increasing_fills
                ),
                "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
                "mid": float(self.state.mid),
                "best_bid": float(self.state.cur_best_bid),
                "best_ask": float(self.state.cur_best_ask),
                "distance_to_mid": float(
                    max(0.0, self.state.mid - final_price)
                    if side == "BUY"
                    else max(0.0, final_price - self.state.mid)
                ),
                "quote_delta_to_bbo": float(
                    max(0.0, self.state.cur_best_bid - final_price)
                    if side == "BUY"
                    else max(0.0, final_price - self.state.cur_best_ask)
                ),
                "toxicity": float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask),
                "markout_ema": float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask),
                "weighted_mid_proxy_shift_bps": float(side_ctx.get("weighted_mid_proxy_shift_bps", 0.0)),
                "l2_quote_flip_rate": float(side_ctx.get("l2_quote_flip_rate", 0.0)),
                "l2_book_refresh_ratio": float(side_ctx.get("l2_book_refresh_ratio", 0.0)),
                "l2_book_cancel_ratio": float(side_ctx.get("l2_book_cancel_ratio", 0.0)),
                "l2_near_depth_total": float(
                    side_ctx.get("l2_near_depth_total", side_ctx.get("near_depth_total", 0.0))
                    or 0.0
                ),
                "submit_local_rank": float(self._local_rank_at_ts(int(now_ts))),
                "repair_probability": float(
                    side_ctx.get("multi_market_repair_probability", self.services.math.nan)
                ),
                "global_flow_pressure": float(
                    side_ctx.get("multi_market_global_flow_pressure", self.services.math.nan)
                ),
                "actionable": int(actionable),
                "block_reasons": "|".join(sorted(reason_set)) or "none",
                "other_block_reasons": "|".join(other_reasons) or "none",
                "side_order_present": int(bool(side_orders)),
                "probe_lifetime_limit_ms": int(self.state.safe_add_rearm_probe_lifetime_ms),
            }
            if not actionable:
                placeholder = {"submit_ts": int(now_ts), "trace_row": trace_row}
                self._safe_add_rearm_finalize(
                    placeholder,
                    "not_actionable",
                    int(now_ts),
                    activation_ts_ms=0,
                    fill_ts_ms=0,
                    fill_qty=0.0,
                    markout_1s_bps=self.services.math.nan,
                    markout_5s_bps=self.services.math.nan,
                    markout_20s_bps=self.services.math.nan,
                    markout_30s_bps=self.services.math.nan,
                    opportunity_value_30s_bps=0.0,
                )
                continue
            latency_ms = self._safe_add_rearm_sample_latency_ms()
            cancel_latency_ms = self._safe_add_rearm_sample_cancel_latency_ms()
            cancel_request_ts = int(now_ts + self.state.safe_add_rearm_probe_lifetime_ms)
            self.state.safe_add_rearm_orders.append(
                {
                    "probe_id": int(probe_id),
                    "side": side,
                    "price": float(final_price),
                    "quantity": float(final_size),
                    "remaining": float(final_size),
                    "submit_ts": int(now_ts),
                    "activate_ts": int(now_ts + latency_ms),
                    "cancel_request_ts": cancel_request_ts,
                    "cancel_ack_ts": int(cancel_request_ts + cancel_latency_ms),
                    "episode_fill_ts": int(last_fill_ts),
                    "inventory_at_submit": float(self.state.q),
                    "state": self.state.ORDER_PENDING_NEW,
                    "queue_init": 0.0,
                    "queue_left": 0.0,
                    "fill_eligible": True,
                    "quote_diag": dict(side_ctx),
                    "trace_row": {
                        **trace_row,
                        "sampled_new_latency_ms": int(latency_ms),
                        "sampled_cancel_latency_ms": int(cancel_latency_ms),
                    },
                }
            )

    def _safe_add_rearm_markout_bps(self, side: str, fill_ts: int, price: float, horizon_ms: int) -> float:
        future_idx = int(self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left"))
        if future_idx >= len(self.state.trade_price):
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = future_price - price if side == "BUY" else price - future_price
        return float(10_000.0 * signed_move / max(price, 1e-12))

    def _record_queue_value_fill(self,
        order: dict,
        side: str,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(
            order.get("queue_value_keep_cancel_intervention_id", -1) or -1
        )
        record = self.state.queue_value_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        future_idx = int(
            self.services.np.searchsorted(
                self.state.trade_ts,
                fill_ts + self.state.queue_value_fill_horizon_ms,
                side="left",
            )
        )
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = (
            future_price - price if side == "BUY" else price - future_price
        )
        markout_bps = 10_000.0 * signed_move / max(price, 1e-12)
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(
            record.get("fill_value", 0.0) + net_value
        )
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        if bool(order.get("queue_value_reentry_order", False)):
            record["reentry_fill_qty"] = float(
                record.get("reentry_fill_qty", 0.0) + float(fill_qty)
            )
            record["reentry_fill_count"] = int(
                record.get("reentry_fill_count", 0) + 1
            )
            record["reentry_fill_value"] = float(
                record.get("reentry_fill_value", 0.0) + net_value
            )
        record["fill_markout_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_qty_weighted_bps_sum", 0.0)
            + markout_bps * float(fill_qty)
        )
        record["fill_value_horizon_censored"] = int(
            bool(record.get("fill_value_horizon_censored", 0))
            or horizon_censored
        )
        if int(record.get("fill_ts_ns", 0) or 0) <= 0:
            record["fill_ts_ns"] = int(fill_ts) * 1_000_000
            self._queue_value_set_first_event(
                record,
                "favorable_fill" if markout_bps >= 0.0 else "adverse_fill",
                int(fill_ts),
            )

    def _record_queue_value_cancel(self, order: dict, cancel_ts: int) -> None:
        self._record_local_order_value_cancel(order, int(cancel_ts))
        intervention_id = int(
            order.get("queue_value_keep_cancel_intervention_id", -1) or -1
        )
        record = self.state.queue_value_open_records.get(intervention_id)
        if record is None:
            return
        if bool(order.get("queue_value_reentry_order", False)):
            record["reentry_cancel_ack_ts_ns"] = int(cancel_ts) * 1_000_000
            record["reentry_cancel_count"] = int(
                record.get("reentry_cancel_count", 0) + 1
            )
            return
        if int(record.get("cancel_request_ts_ns", 0) or 0) <= 0:
            request_ts = int(order.get("cancel_request_ts", cancel_ts) or cancel_ts)
            record["cancel_request_ts_ns"] = request_ts * 1_000_000
        record["cancel_ack_ts_ns"] = int(cancel_ts) * 1_000_000
        self._queue_value_set_first_event(record, "cancel", int(cancel_ts))

    def _update_queue_value_price_jump(self, now_ts: int, mark_px: float) -> None:
        if self.state.queue_value_price_jump_ticks <= 0.0:
            return
        for record in self.state.queue_value_open_records.values():
            if str(record.get("first_event", "pending")) != "pending":
                continue
            decision_mid = 0.5 * (
                float(record["best_bid"]) + float(record["best_ask"])
            )
            threshold = self.state.queue_value_price_jump_ticks * float(self.state.TICK)
            side = str(record["side"])
            adverse = (
                float(mark_px) <= decision_mid - threshold
                if side == "BUY"
                else float(mark_px) >= decision_mid + threshold
            )
            if not adverse:
                continue
            record["adverse_price_jump_ts_ns"] = int(now_ts) * 1_000_000
            self._queue_value_set_first_event(
                record,
                "adverse_price_jump",
                int(now_ts),
            )

    def _record_local_action_fill(self,
        order: dict,
        side: str,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(order.get("local_action_ope_intervention_id", -1) or -1)
        record = self.state.local_action_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        horizon_ms = 30_000
        future_idx = int(self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left"))
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = future_price - price if side == "BUY" else price - future_price
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(record.get("fill_value", 0.0) + net_value)
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        record["fill_markout_30s_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0)
            + 10_000.0 * signed_move / max(price, 1e-12) * float(fill_qty)
        )
        record["fill_markout_horizon_censored"] = int(
            bool(record.get("fill_markout_horizon_censored", 0))
            or horizon_censored
        )

    def _record_sell_add_skip_fill(self,
        order: dict,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(
            order.get("sell_add_skip_ope_intervention_id", -1) or -1
        )
        record = self.state.sell_add_skip_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        horizon_ms = 30_000
        future_idx = int(
            self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left")
        )
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        side = str(record.get("side", "SELL")).upper()
        signed_move = (
            future_price - price if side == "BUY" else price - future_price
        )
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(
            record.get("fill_value", 0.0) + net_value
        )
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        record["fill_markout_30s_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0)
            + 10_000.0
            * signed_move
            / max(price, 1e-12)
            * float(fill_qty)
        )
        record["fill_markout_horizon_censored"] = int(
            bool(record.get("fill_markout_horizon_censored", 0))
            or horizon_censored
        )

    def _record_state_conditioned_fill(self,
        order: dict,
        side: str,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(
            order.get("state_conditioned_policy_intervention_id", -1) or -1
        )
        record = self.state.state_conditioned_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        horizon_ms = 30_000
        future_idx = int(self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left"))
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = future_price - price if side == "BUY" else price - future_price
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(record.get("fill_value", 0.0) + net_value)
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        record["fill_markout_30s_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0)
            + 10_000.0 * signed_move / max(price, 1e-12) * float(fill_qty)
        )
        record["fill_markout_horizon_censored"] = int(
            bool(record.get("fill_markout_horizon_censored", 0))
            or horizon_censored
        )

    def _record_safe_add_rearm_fill(self,
        order: dict,
        side: str,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(
            order.get("safe_add_rearm_intervention_id", -1) or -1
        )
        record = self.state.safe_add_rearm_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        horizon_ms = 30_000
        future_idx = int(self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left"))
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = future_price - price if side == "BUY" else price - future_price
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(record.get("fill_value", 0.0) + net_value)
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        record["fill_markout_30s_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0)
            + 10_000.0 * signed_move / max(price, 1e-12) * float(fill_qty)
        )
        record["fill_markout_horizon_censored"] = int(
            bool(record.get("fill_markout_horizon_censored", 0))
            or horizon_censored
        )

    def _record_state_conditioned_rearm_fill(self,
        order: dict,
        side: str,
        fill_ts: int,
        fill_qty: float,
    ) -> None:
        intervention_id = int(
            order.get("state_conditioned_rearm_intervention_id", -1) or -1
        )
        record = self.state.state_conditioned_rearm_open_records.get(intervention_id)
        if record is None:
            return
        price = float(order["price"])
        horizon_ms = 30_000
        future_idx = int(
            self.services.np.searchsorted(self.state.trade_ts, fill_ts + horizon_ms, side="left")
        )
        horizon_censored = future_idx >= len(self.state.trade_price)
        if horizon_censored:
            future_idx = len(self.state.trade_price) - 1
        future_price = float(self.state.trade_price[future_idx])
        signed_move = (
            future_price - price if side == "BUY" else price - future_price
        )
        net_value = (signed_move - self.state.maker_fee * price) * float(fill_qty)
        record["fill_value"] = float(
            record.get("fill_value", 0.0) + net_value
        )
        record["intervention_fill_qty"] = float(
            record.get("intervention_fill_qty", 0.0) + float(fill_qty)
        )
        record["intervention_fill_count"] = int(
            record.get("intervention_fill_count", 0) + 1
        )
        record["fill_markout_30s_qty_weighted_bps_sum"] = float(
            record.get("fill_markout_30s_qty_weighted_bps_sum", 0.0)
            + 10_000.0 * signed_move / max(price, 1e-12) * float(fill_qty)
        )
        record["fill_markout_horizon_censored"] = int(
            bool(record.get("fill_markout_horizon_censored", 0))
            or horizon_censored
        )

    def _process_safe_add_rearm_orders(self, now_ts: int, trade_price_now: float, trade_qty_now: float, is_sell_trade: bool) -> None:
        if self.state.trace_safe_add_rearm is None or not self.state.safe_add_rearm_orders:
            return
        survivors: list[dict] = []
        for order in self.state.safe_add_rearm_orders:
            side = str(order["side"])
            current_episode_ts = self.state.last_buy_fill_ts if side == "BUY" else self.state.last_sell_fill_ts
            current_episode_total_ms = (
                self.state.last_buy_fill_cooldown_total_ms
                if side == "BUY"
                else self.state.last_sell_fill_cooldown_total_ms
            )
            inventory_same_inventory_lifecycle = (
                (side == "BUY" and self.state.q > 0.0)
                or (side == "SELL" and self.state.q < 0.0)
            )
            if (
                int(current_episode_ts) != int(order["episode_fill_ts"])
                or current_episode_total_ms <= 0.0
            ):
                self._safe_add_rearm_finalize(
                    order,
                    "episode_superseded",
                    now_ts,
                    activation_ts_ms=int(order["activate_ts"]),
                    fill_ts_ms=0,
                    fill_qty=0.0,
                    opportunity_value_30s_bps=0.0,
                )
                continue
            if not inventory_same_inventory_lifecycle:
                self._safe_add_rearm_finalize(
                    order,
                    "inventory_lifecycle_flat_before_fill",
                    now_ts,
                    activation_ts_ms=int(order["activate_ts"]),
                    fill_ts_ms=0,
                    fill_qty=0.0,
                    opportunity_value_30s_bps=0.0,
                )
                continue
            if now_ts >= int(order["cancel_ack_ts"]):
                cancel_before_active = int(order["cancel_ack_ts"]) <= int(
                    order["activate_ts"]
                )
                self._safe_add_rearm_finalize(
                    order,
                    "cancel_before_active" if cancel_before_active else "no_fill_cancel_ack",
                    int(order["cancel_ack_ts"]),
                    activation_ts_ms=int(order["activate_ts"]),
                    cancel_request_ts_ms=int(order["cancel_request_ts"]),
                    cancel_ack_ts_ms=int(order["cancel_ack_ts"]),
                    fill_ts_ms=0,
                    fill_qty=0.0,
                    fill_while_pending_cancel=0,
                    opportunity_value_30s_bps=0.0,
                )
                continue
            if order["state"] == self.state.ORDER_PENDING_NEW:
                if now_ts < int(order["activate_ts"]):
                    survivors.append(order)
                    continue
                best_bid_at, best_ask_at, bid_qty_at, ask_qty_at, mid_at, source = self._book_snapshot_at(
                    int(order["activate_ts"]),
                    float(order["trace_row"].get("mid", trade_price_now)),
                )
                if source == "unavailable_book":
                    order["activation_book_status"] = "UNKNOWN"
                    survivors.append(order)
                    continue
                crosses = self.services._order_would_cross_book_tick(
                    side,
                    float(order["price"]),
                    float(best_bid_at),
                    float(best_ask_at),
                    self.state.TICK,
                )
                if crosses:
                    self._safe_add_rearm_finalize(
                        order,
                        "gtx_reject",
                        now_ts,
                        activation_ts_ms=int(order["activate_ts"]),
                        activation_book_source=source,
                        fill_ts_ms=0,
                        fill_qty=0.0,
                        opportunity_value_30s_bps=0.0,
                    )
                    continue
                quote_mid = mid_at if mid_at > 0.0 else float(order["trace_row"].get("mid", trade_price_now))
                distance = (
                    max(0.0, quote_mid - order["price"])
                    if side == "BUY"
                    else max(0.0, order["price"] - quote_mid)
                )
                diag = order.get("quote_diag") or {}
                near_depth_total = float(
                    diag.get("l2_near_depth_total", diag.get("near_depth_total", 0.0))
                    or 0.0
                )
                reason_bucket = self._queue_reason_bucket(order, side, near_depth_total)
                local_rank = self._local_rank_at_ts(int(order["activate_ts"]))
                regime_mult = self._queue_regime_multiplier(
                    side,
                    distance,
                    near_depth_total,
                    local_rank,
                    reason_bucket,
                    int(order["activate_ts"]),
                )
                mo_mult = self._queue_mo_multiplier(order, side, int(order["activate_ts"]))
                inventory_mult, _ = self._queue_inventory_multiplier(
                    side,
                    float(order["inventory_at_submit"]),
                )
                queue_mult = float(regime_mult) * float(mo_mult) * float(inventory_mult)
                order["queue_left"] = self._estimate_queue_ahead(
                    side,
                    float(order["price"]),
                    float(distance),
                    target_ts=int(order["activate_ts"]),
                    regime_queue_mult=queue_mult,
                )
                order["queue_init"] = float(order["queue_left"])
                self._init_l2_cancel_ahead_state(
                    order,
                    side,
                    int(order["activate_ts"]),
                )
                order["fill_eligible"] = self._safe_add_rearm_fill_eligible(
                    side,
                    int(order["activate_ts"]),
                )
                order["state"] = self.state.ORDER_OPEN
                order["trace_row"].update(
                    {
                        "activation_book_source": source,
                        "queue_regime_mult": float(regime_mult),
                        "queue_mo_mult": float(mo_mult),
                        "queue_inventory_mult": float(inventory_mult),
                        "queue_local_rank": float(local_rank),
                    }
                )
            crossing_trade = (
                side == "BUY"
                and is_sell_trade
                and self.services._trade_crosses_order_tick(
                    "BUY", trade_price_now, float(order["price"]), self.state.TICK
                )
            ) or (
                side == "SELL"
                and not is_sell_trade
                and self.services._trade_crosses_order_tick(
                    "SELL", trade_price_now, float(order["price"]), self.state.TICK
                )
            )
            if not crossing_trade or not order.get("fill_eligible", True):
                survivors.append(order)
                continue
            deplete_mult = self._queue_deplete_multiplier(side, now_ts)
            trade_remaining = float(trade_qty_now) * float(deplete_mult)
            if order["queue_left"] > 0.0:
                consumed = min(float(order["queue_left"]), trade_remaining)
                order["queue_left"] = self.services.subtract_lot_quantity(
                    float(order["queue_left"]), consumed, self.state.LOT_SIZE
                )
                trade_remaining = self.services.subtract_lot_quantity(trade_remaining, consumed, self.state.LOT_SIZE)
            if trade_remaining < self.state.LOT_SIZE or order["remaining"] < self.state.LOT_SIZE:
                survivors.append(order)
                continue
            fill_qty = self.services.math.floor(
                min(float(order["remaining"]), trade_remaining) / self.state.LOT_SIZE
            ) * self.state.LOT_SIZE
            if fill_qty < self.state.LOT_SIZE:
                survivors.append(order)
                continue
            markouts = {
                horizon: self._safe_add_rearm_markout_bps(side, now_ts, float(order["price"]), horizon)
                for horizon in (1_000, 5_000, 20_000, 30_000)
            }
            self._safe_add_rearm_finalize(
                order,
                "would_fill",
                now_ts,
                activation_ts_ms=int(order["activate_ts"]),
                fill_ts_ms=int(now_ts),
                fill_qty=float(fill_qty),
                cancel_request_ts_ms=int(order["cancel_request_ts"]),
                cancel_ack_ts_ms=int(order["cancel_ack_ts"]),
                fill_while_pending_cancel=int(now_ts >= int(order["cancel_request_ts"])),
                queue_deplete_mult=float(deplete_mult),
                markout_1s_bps=float(markouts[1_000]),
                markout_5s_bps=float(markouts[5_000]),
                markout_20s_bps=float(markouts[20_000]),
                markout_30s_bps=float(markouts[30_000]),
                opportunity_value_30s_bps=float(markouts[30_000]),
            )
        self.state.safe_add_rearm_orders[:] = survivors

    def _latest_side_order(self, orders):
        if not orders:
            return None
        return max(
            orders,
            key=lambda o: (
                o["submit_ts"],
                o["activate_ts"],
                self.services._order_price_tick(o, self.state.TICK),
            ),
        )

    def _best_live_order(self, orders, side: str):
        live_orders = [o for o in orders if o["state"] in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL) and o["remaining"] >= self.state.LOT_SIZE]
        if not live_orders:
            return None
        if side == "BUY":
            return max(live_orders, key=lambda o: self.services._order_price_tick(o, self.state.TICK))
        return min(live_orders, key=lambda o: self.services._order_price_tick(o, self.state.TICK))

    def _current_top_quantities(self) -> tuple[float, float]:
        quantity_bbo_idx = (
            self.state.active_decision_bbo_idx
            if self.state.active_decision_bbo_idx is not None
            else self.state.bbo_idx
        )
        if (
            self.state.historical_bbo
            and quantity_bbo_idx >= 0
            and self.state.bbo_usable[quantity_bbo_idx]
            and self.state.bbo_bid_qty is not None
            and self.state.bbo_ask_qty is not None
        ):
            return (
                max(0.0, float(self.state.bbo_bid_qty[quantity_bbo_idx])),
                max(0.0, float(self.state.bbo_ask_qty[quantity_bbo_idx])),
            )
        if self._bbo_query_unavailable(quantity_bbo_idx, self.state.l2_idx):
            return 0.0, 0.0
        if (
            self.state.historical_l2
            and self.state.cur_l2_bid_qty_row is not None
            and self.state.cur_l2_ask_qty_row is not None
        ):
            return (
                max(0.0, float(self.state.cur_l2_bid_qty_row[0])),
                max(0.0, float(self.state.cur_l2_ask_qty_row[0])),
            )
        return 0.0, 0.0

    def _queue_value_features(self,
        side: str,
        now_ts: int,
        order: dict,
        side_ctx: dict,
    ) -> dict[str, Any]:
        bid_qty, ask_qty = self._current_top_quantities()
        denominator = bid_qty + ask_qty
        book_imbalance = (
            (bid_qty - ask_qty) / denominator
            if denominator > 0.0
            else float(side_ctx.get("book_imb", 0.0) or 0.0)
        )
        # Native exchange-time depth is hidden simulator state.  Keep/cancel
        # decisions must use the policy-visible queue estimate reconstructed
        # from delayed BBO/L2 and public trades.
        queue_init = max(
            0.0,
            float(
                order.get(
                    "policy_queue_init",
                    order.get("queue_init", 0.0),
                )
                or 0.0
            ),
        )
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
        queue_fraction_left = (
            min(1.0, queue_left / queue_init)
            if queue_init > 1e-12
            else 0.0
        )
        order_price = float(order.get("price", 0.0) or 0.0)
        order_quote_ts = int(
            order.get(
                "quote_ts",
                order.get("submit_ts", now_ts),
            )
            or now_ts
        )
        quote_distance = (
            max(0.0, float(self.state.mid) - order_price)
            if side == "BUY"
            else max(0.0, order_price - float(self.state.mid))
        )
        return {
            "side": str(side),
            "spread_ticks": max(
                0.0,
                float(self.state.cur_best_ask - self.state.cur_best_bid) / max(float(self.state.TICK), 1e-12),
            ),
            "book_imbalance": float(book_imbalance),
            "queue_fraction_left": float(queue_fraction_left),
            "quote_distance_ticks": float(
                quote_distance / max(float(self.state.TICK), 1e-12)
            ),
            "order_age_ms": max(0, int(now_ts) - order_quote_ts),
            "inventory_lifecycle_age_s": (
                max(0.0, float(now_ts - self.state.inventory_lifecycle_start_ts) / 1_000.0)
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_pnl_so_far": (
                float(self._inventory_lifecycle_pnl(self.state.mid) - self.state.inventory_lifecycle_start_pnl)
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_add_count_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
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
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            "bid_qty": float(bid_qty),
            "ask_qty": float(ask_qty),
        }

    def _finalize_local_order_value_observation(self,
        order_id: int,
        event: str,
        event_ts_ms: int,
        *,
        fill_markout_bps: float = float('nan'),
        fill_horizon_censored: bool = False,
    ) -> None:
        if self.state.trace_local_order_value is None:
            return
        record = self.state.local_order_value_open_records.pop(int(order_id), None)
        if record is None:
            return
        event = str(event)
        event_ts_ns = int(event_ts_ms) * 1_000_000
        record["censor_ts_ns"] = event_ts_ns
        record["interval_end_ts_ns"] = event_ts_ns
        record["interval_ms"] = max(
            1.0,
            float(
                event_ts_ns - int(record["decision_ts_ns"])
            )
            / 1_000_000.0,
        )
        record["first_event"] = event
        record["first_event_ts_ns"] = event_ts_ns
        record["event_time_ms"] = float(record["interval_ms"])
        record["event_observed"] = int(event != "censored")
        if event in {"favorable_fill", "adverse_fill"}:
            record["fill_ts_ns"] = event_ts_ns
            record["fill_event_seq"] = 1
            record["fill_value_markout_bps"] = float(fill_markout_bps)
            record["fill_value_horizon_censored"] = int(
                bool(fill_horizon_censored)
            )
        elif event == "cancel":
            record["cancel_ack_ts_ns"] = event_ts_ns
            record["cancel_event_seq"] = 2
        elif event == "adverse_price_jump":
            record["adverse_price_jump_ts_ns"] = event_ts_ns
            record["adverse_price_jump_event_seq"] = 3
        elif event == "inventory_lifecycle_repair":
            record["repair_ts_ns"] = event_ts_ns
            record["inventory_lifecycle_repair_event_seq"] = 4
        for event_name in (
            "favorable_fill",
            "adverse_fill",
            "cancel",
            "adverse_price_jump",
            "inventory_lifecycle_repair",
            "censored",
        ):
            record[f"event_{event_name}"] = int(event == event_name)
        record.pop("_exchange_book_trade_since_update_qty", None)
        if len(self.state.trace_local_order_value) < self.state.trace_local_order_value_max:
            self.state.trace_local_order_value.append(record)

    def _observe_local_order_value_order(self,
        order: dict,
        side: str,
        decision_ts_ms: int,
        *,
        best_bid: float,
        best_ask: float,
        bid_qty: float,
        ask_qty: float,
    ) -> None:
        if self.state.trace_local_order_value is None:
            return
        order_id = int(order.get("trace_id", -1))
        if (
            order_id < 0
            or order_id in self.state.local_order_value_observed_order_ids
            or len(self.state.trace_local_order_value)
            + len(self.state.local_order_value_open_records)
            >= self.state.trace_local_order_value_max
            or best_bid <= 0.0
            or best_ask <= best_bid
        ):
            return
        self.state.local_order_value_observed_order_ids.add(order_id)
        decision_mid = 0.5 * (float(best_bid) + float(best_ask))
        quantity_sum = max(0.0, float(bid_qty)) + max(
            0.0, float(ask_qty)
        )
        book_imbalance = (
            (float(bid_qty) - float(ask_qty)) / quantity_sum
            if quantity_sum > 1e-12
            else 0.0
        )
        simulator_queue_init = max(
            0.0, float(order.get("queue_init", 0.0) or 0.0)
        )
        simulator_queue_left = max(
            0.0, float(order.get("queue_left", 0.0) or 0.0)
        )
        queue_init = max(
            0.0,
            float(
                order.get("policy_queue_init", simulator_queue_init) or 0.0
            ),
        )
        queue_left = max(
            0.0,
            float(
                order.get("policy_queue_left", simulator_queue_left) or 0.0
            ),
        )
        queue_fraction_left = (
            min(1.0, queue_left / queue_init)
            if queue_init > 1e-12
            else 0.0
        )
        future_ts_ms = int(
            decision_ts_ms + self.state.local_order_value_fill_horizon_ms
        )
        future_censored = bool(future_ts_ms > int(self.state.trade_ts[-1]))
        future_lookup_ts = min(future_ts_ms, int(self.state.trade_ts[-1]))
        future_idx = int(
            self.services.np.searchsorted(self.state.trade_ts, future_lookup_ts, side="left")
        )
        future_idx = min(max(future_idx, 0), len(self.state.trade_price) - 1)
        (
            future_bid,
            future_ask,
            _,
            _,
            _,
            _,
        ) = self._book_snapshot_at(
            future_lookup_ts,
            float(self.state.trade_price[future_idx]),
        )
        future_mid = (
            0.5 * (float(future_bid) + float(future_ask))
            if future_bid > 0.0 and future_ask > future_bid
            else float(self.state.trade_price[future_idx])
        )
        side_ctx = order.get("quote_diag") or {}
        role = self.services.inventory_role_for_quote(
            side,
            float(self.state.q),
            float(self.state.order_size),
        )
        inventory_lifecycle_id = (
            int(self.state.inventory_lifecycle_count)
            if self.state.inventory_lifecycle_active
            else int(order.get("inventory_lifecycle_id_at_submit", 0) or 0)
        )
        record = {
            "schema_version": "local_order_value_panel.v1",
            "policy_family": "rolling_baseline",
            "input_scope": "local_only",
            "external_reference_used": 0,
            "decision_id": (
                f"local_order_value:{self.services.SYMBOL}:{order_id}:{decision_ts_ms}"
            ),
            "day": self.services.pd.to_datetime(
                int(decision_ts_ms), unit="ms", utc=True
            ).strftime("%Y-%m-%d"),
            "decision_ts_ns": int(decision_ts_ms) * 1_000_000,
            "feature_ready_ts_ns": int(decision_ts_ms) * 1_000_000,
            "censor_ts_ns": int(self.state.trade_ts[-1]) * 1_000_000,
            "interval_end_ts_ns": int(self.state.trade_ts[-1]) * 1_000_000,
            "inventory_lifecycle_id": int(inventory_lifecycle_id),
            "order_id": order_id,
            "client_order_id": "",
            "side": str(side),
            "inventory_role": str(role),
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "inventory_lifecycle_age_s": (
                max(
                    0.0,
                    float(decision_ts_ms - self.state.inventory_lifecycle_start_ts) / 1_000.0,
                )
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "inventory_lifecycle_pnl_so_far": (
                float(self._inventory_lifecycle_pnl(decision_mid) - self.state.inventory_lifecycle_start_pnl)
                if self.state.inventory_lifecycle_active
                else 0.0
            ),
            "inventory_lifecycle_mae_so_far": float(self.state.inventory_lifecycle_cur_min_pnl),
            "inventory_lifecycle_add_count_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "order_age_ms": max(
                0, int(decision_ts_ms) - int(order["quote_ts"])
            ),
            "order_price": float(order["price"]),
            "quote_distance_ticks": float(
                (
                    max(0.0, decision_mid - float(order["price"]))
                    if side == "BUY"
                    else max(0.0, float(order["price"]) - decision_mid)
                )
                / max(float(self.state.TICK), 1e-12)
            ),
            "queue_init": queue_init,
            "queue_left": queue_left,
            "queue_fraction_left": float(queue_fraction_left),
            "simulator_queue_init": simulator_queue_init,
            "simulator_queue_left": simulator_queue_left,
            "queue_local_rank": float(
                order.get("queue_local_rank", 0.5) or 0.5
            ),
            "queue_source": str(
                order.get(
                    "policy_queue_source",
                    "delayed_policy_topn_or_fitted",
                )
            ),
            "simulator_queue_source": str(
                order.get(
                    "simulator_queue_source",
                    "topn_or_fitted_fallback",
                )
            ),
            "exchange_book_queue_mode": str(self.state.exchange_book_queue_mode),
            "exchange_book_queue_status": str(
                order.get("_exchange_book_queue_seed_status", "disabled")
            ),
            "exchange_book_queue_reason": str(
                order.get("_exchange_book_queue_seed_reason", "")
            ),
            "exchange_book_queue_segment_id": int(
                order.get("exchange_book_queue_segment_id", 0) or 0
            ),
            "exchange_book_queue_asof_ts_ns": int(
                order.get("exchange_book_queue_asof_ts_ns", 0) or 0
            ),
            "exchange_book_queue_path_valid": int(
                bool(order.get("exchange_book_queue_path_valid", False))
            ),
            "exchange_book_queue_ambiguous": int(
                bool(order.get("exchange_book_queue_ambiguous", False))
            ),
            "exchange_book_event_count": 0.0,
            "exchange_book_cancel_count": 0.0,
            "exchange_book_cancel_qty": 0.0,
            "exchange_book_refill_count": 0.0,
            "exchange_book_refill_qty": 0.0,
            "exchange_book_ambiguous_event_count": 0.0,
            "_exchange_book_trade_since_update_qty": 0.0,
            "spread_ticks": float(
                (float(best_ask) - float(best_bid))
                / max(float(self.state.TICK), 1e-12)
            ),
            "book_imbalance": float(book_imbalance),
            "best_bid": float(best_bid),
            "best_ask": float(best_ask),
            "bid_qty": max(0.0, float(bid_qty)),
            "ask_qty": max(0.0, float(ask_qty)),
            "future_mid": float(future_mid),
            "future_mid_horizon_ms": int(
                self.state.local_order_value_fill_horizon_ms
            ),
            "future_mid_censored": int(future_censored),
            "book_state_resolution_ms": float(
                self.state.book_state_resolution_ms
            ),
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
            "adverse_market_order_count": 0.0,
            "adverse_market_order_qty": 0.0,
            "cancel_count": 0.0,
            "cancel_qty": 0.0,
            "refill_count": 0.0,
            "refill_qty": 0.0,
            "interval_ms": max(
                1.0,
                float(int(self.state.trade_ts[-1]) - int(decision_ts_ms)),
            ),
            "first_event": "pending",
            "first_event_ts_ns": 0,
            "fill_ts_ns": 0,
            "fill_event_seq": 0,
            "cancel_ack_ts_ns": 0,
            "cancel_event_seq": 0,
            "adverse_price_jump_ts_ns": 0,
            "adverse_price_jump_event_seq": 0,
            "repair_ts_ns": 0,
            "inventory_lifecycle_repair_event_seq": 0,
            "fill_value_markout_bps": self.services.math.nan,
            "fill_value_horizon_censored": 0,
            "fill_value_horizon_ms": int(
                self.state.local_order_value_fill_horizon_ms
            ),
            "price_jump_ticks": float(
                self.state.local_order_value_price_jump_ticks
            ),
        }
        self.state.local_order_value_open_records[order_id] = record

    def _record_local_order_value_fill(self,
        order: dict,
        side: str,
        fill_ts_ms: int,
    ) -> None:
        order_id = int(order.get("trace_id", -1))
        if (
            order_id not in self.state.local_order_value_open_records
            and not self.state.local_order_lifecycle.enabled
        ):
            return
        price = float(order["price"])
        future_ts_ms = int(
            fill_ts_ms + self.state.local_order_value_fill_horizon_ms
        )
        (
            future_price,
            observation_ts_ms,
            observation_source,
            book_censored,
        ) = self._fixed_horizon_book_mid(future_ts_ms)
        horizon_censored = bool(
            future_ts_ms > int(self.state.trade_ts[-1])
            or book_censored
            or not self.services.math.isfinite(future_price)
        )
        observation_age_ms = (
            max(0, int(future_ts_ms) - int(observation_ts_ms))
            if observation_ts_ms > 0 else self.services.math.nan
        )
        signed_move = (
            future_price - price
            if side == "BUY"
            else price - future_price
        )
        markout_bps = (
            10_000.0 * signed_move / max(price, 1e-12)
        )
        self.state.local_order_lifecycle.annotate_fill_value(
            order,
            markout_bps=float(markout_bps),
            horizon_ms=int(self.state.local_order_value_fill_horizon_ms),
            horizon_censored=bool(horizon_censored),
            observation_ts_ns=int(observation_ts_ms) * 1_000_000,
            observation_mid=float(future_price),
            target_ts_ns=int(future_ts_ms) * 1_000_000,
            observation_source=str(observation_source),
            observation_age_ms=float(observation_age_ms),
        )
        if order_id not in self.state.local_order_value_open_records:
            return
        self._finalize_local_order_value_observation(
            order_id,
            (
                "censored"
                if horizon_censored
                else "favorable_fill"
                if markout_bps >= 0.0
                else "adverse_fill"
            ),
            int(fill_ts_ms),
            fill_markout_bps=float(markout_bps),
            fill_horizon_censored=horizon_censored,
        )

    def _record_local_order_value_cancel(self,
        order: dict,
        cancel_ts_ms: int,
    ) -> None:
        self._finalize_local_order_value_observation(
            int(order.get("trace_id", -1)),
            "cancel",
            int(cancel_ts_ms),
        )

    def _record_local_order_value_inventory_lifecycle_repair(self,
        inventory_lifecycle_id: int,
        repair_ts_ms: int,
    ) -> None:
        for order_id, record in list(
            self.state.local_order_value_open_records.items()
        ):
            if int(record.get("inventory_lifecycle_id", 0) or 0) != int(inventory_lifecycle_id):
                continue
            self._finalize_local_order_value_observation(
                int(order_id),
                "inventory_lifecycle_repair",
                int(repair_ts_ms),
            )

    def _update_local_order_value_price_jump(self,
        now_ts_ms: int,
        mark_px: float,
    ) -> None:
        if (
            self.state.trace_local_order_value is None
            or self.state.local_order_value_price_jump_ticks <= 0.0
        ):
            return
        threshold = (
            self.state.local_order_value_price_jump_ticks * float(self.state.TICK)
        )
        for order_id, record in list(
            self.state.local_order_value_open_records.items()
        ):
            decision_mid = 0.5 * (
                float(record["best_bid"]) + float(record["best_ask"])
            )
            side = str(record["side"])
            adverse = (
                float(mark_px) <= decision_mid - threshold
                if side == "BUY"
                else float(mark_px) >= decision_mid + threshold
            )
            if adverse:
                self._finalize_local_order_value_observation(
                    int(order_id),
                    "adverse_price_jump",
                    int(now_ts_ms),
                )

    def _update_local_order_value_path_counts(self,
        *,
        is_execution_event: bool,
        is_sell_trade: bool,
        trade_price_now: float,
        trade_quantity: float,
        bid_cancel_qty: float,
        bid_refill_qty: float,
        ask_cancel_qty: float,
        ask_refill_qty: float,
    ) -> None:
        if self.state.local_order_lifecycle.records_dynamic_state:
            for orders, cancel_size, refill_size in (
                (self.state.bid_orders, bid_cancel_qty, bid_refill_qty),
                (self.state.ask_orders, ask_cancel_qty, ask_refill_qty),
            ):
                for order in orders:
                    if order.get("state") not in (
                        self.state.ORDER_OPEN,
                        self.state.ORDER_PENDING_CANCEL,
                    ):
                        continue
                    if cancel_size > 0.0:
                        order["policy_visible_cancel_count"] = float(
                            order.get("policy_visible_cancel_count", 0.0)
                            + 1.0
                        )
                        order["policy_visible_cancel_size"] = float(
                            order.get("policy_visible_cancel_size", 0.0)
                            + float(cancel_size)
                        )
                    if refill_size > 0.0:
                        order["policy_visible_refill_count"] = float(
                            order.get("policy_visible_refill_count", 0.0)
                            + 1.0
                        )
                        order["policy_visible_refill_size"] = float(
                            order.get("policy_visible_refill_size", 0.0)
                            + float(refill_size)
                        )
        if self.state.trace_local_order_value is None:
            return
        for record in self.state.local_order_value_open_records.values():
            side = str(record["side"])
            adverse_trade = bool(
                is_execution_event
                and (
                    (side == "BUY" and is_sell_trade)
                    or (side == "SELL" and not is_sell_trade)
                )
            )
            if adverse_trade:
                record["adverse_market_order_count"] = float(
                    record["adverse_market_order_count"] + 1.0
                )
                record["adverse_market_order_qty"] = float(
                    record["adverse_market_order_qty"]
                    + max(0.0, float(trade_quantity))
                )
                if (
                    self.state.exchange_book_scheduler is not None
                    and self.services._price_to_tick(float(record["order_price"]), self.state.TICK)
                    == self.services._price_to_tick(float(trade_price_now), self.state.TICK)
                ):
                    record["_exchange_book_trade_since_update_qty"] = float(
                        record.get(
                            "_exchange_book_trade_since_update_qty",
                            0.0,
                        )
                        + max(0.0, float(trade_quantity))
                    )
            cancel_qty = (
                bid_cancel_qty if side == "BUY" else ask_cancel_qty
            )
            refill_qty = (
                bid_refill_qty if side == "BUY" else ask_refill_qty
            )
            if cancel_qty > 0.0:
                record["cancel_count"] = float(
                    record["cancel_count"] + 1.0
                )
                record["cancel_qty"] = float(
                    record["cancel_qty"] + cancel_qty
                )
            if refill_qty > 0.0:
                record["refill_count"] = float(
                    record["refill_count"] + 1.0
                )
                record["refill_qty"] = float(
                    record["refill_qty"] + refill_qty
                )
