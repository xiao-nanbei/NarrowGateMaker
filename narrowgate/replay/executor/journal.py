"""Replay journal owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping


class JournalAdapter:
    def _order_trace_fields(self, order):
        diag = order.get("quote_diag") or {}
        fields = {
            "order_id": int(order.get("trace_id", -1)),
            "side": order["side"],
            "inventory_lifecycle_id_at_submit": int(
                order.get("inventory_lifecycle_id_at_submit", 0) or 0
            ),
            "inventory_role_at_submit": str(
                order.get("inventory_role_at_submit", "")
            ),
            "submit_ts": int(order["submit_ts"]),
            "activate_ts": int(order["activate_ts"]),
            "quote_ts": int(order["quote_ts"]),
            "price": float(order["price"]),
            "quantity": float(order["quantity"]),
            "reduce_only": bool(order.get("reduce_only", False)),
            "circuit_breaker_close": bool(
                order.get("circuit_breaker_close", False)
            ),
            "raw_half_spread": float(diag.get("raw_half_spread", 0.0)),
            "capped_half_spread": float(diag.get("capped_half_spread", 0.0)),
            "raw_mid_shift": float(diag.get("raw_mid_shift", 0.0)),
            "raw_reservation_shift": float(diag.get("raw_reservation_shift", 0.0)),
            "raw_asym_shift": float(diag.get("raw_asym_shift", 0.0)),
            "asym": float(diag.get("asym", 0.0)),
            "inventory": float(diag.get("inventory", 0.0)),
            "dir_signal": float(diag.get("dir_signal", 0.0)),
            "pred_dir": float(diag.get("pred_dir", 0.5)),
            "pred_ret": float(diag.get("pred_ret", 0.0)),
            "tox_bid": float(diag.get("tox_bid", 0.5)),
            "tox_ask": float(diag.get("tox_ask", 0.5)),
            "book_imb": float(diag.get("book_imb", 0.0)),
            "weighted_mid_proxy_shift_bps": float(diag.get("weighted_mid_proxy_shift_bps", 0.0)),
            "near_depth_total": float(diag.get("near_depth_total", 0.0)),
            "l2_near_depth_total": float(diag.get("l2_near_depth_total", diag.get("near_depth_total", 0.0))),
            "l2_quote_flip_rate": float(diag.get("l2_quote_flip_rate", 0.0)),
            "l2_book_refresh_ratio": float(diag.get("l2_book_refresh_ratio", 0.0)),
            "l2_book_cancel_ratio": float(diag.get("l2_book_cancel_ratio", 0.0)),
            "mo_ema_bid": float(diag.get("mo_ema_bid", 0.0)),
            "mo_ema_ask": float(diag.get("mo_ema_ask", 0.0)),
            "fair": float(diag.get("fair", 0.0)),
            "mid": float(diag.get("mid", 0.0)),
            "best_bid": float(diag.get("best_bid", 0.0)),
            "best_ask": float(diag.get("best_ask", 0.0)),
            "raw_pair_spread": float(diag.get("raw_pair_spread", 0.0)),
            "capped_pair_spread": float(diag.get("capped_pair_spread", 0.0)),
            "final_pair_spread": float(diag.get("final_pair_spread", 0.0)),
            "raw_price": float(diag.get("raw_price", 0.0)),
            "pre_guard_price": float(diag.get("pre_guard_price", 0.0)),
            "final_price": float(diag.get("final_price", order["price"])),
            "raw_quote_delta_to_bbo": float(diag.get("raw_quote_delta_to_bbo", 0.0)),
            "pre_guard_delta_to_bbo": float(diag.get("pre_guard_delta_to_bbo", 0.0)),
            "final_quote_delta_to_bbo": float(diag.get("final_quote_delta_to_bbo", 0.0)),
            "raw_distance_to_mid": float(diag.get("raw_distance_to_mid", 0.0)),
            "final_distance_to_mid": float(diag.get("final_distance_to_mid", 0.0)),
            "raw_quote_skew": float(diag.get("raw_quote_skew", 0.0)),
            "final_quote_skew": float(diag.get("final_quote_skew", 0.0)),
            "raw_bias_side": diag.get("raw_bias_side", "balanced"),
            "final_bias_side": diag.get("final_bias_side", "balanced"),
            "favored_by_raw_shift": bool(diag.get("favored_by_raw_shift", False)),
            "delta_cap": bool(diag.get("delta_cap", False)),
            "mid_guard": bool(diag.get("mid_guard", False)),
            "post_only": bool(diag.get("post_only", False)),
            "side_adverse": bool(diag.get("side_adverse", False)),
            "side_adverse_pause": bool(diag.get("side_adverse_pause", False)),
            "adverse_toxicity": bool(diag.get("adverse_toxicity", False)),
            "adverse_markout": bool(diag.get("adverse_markout", False)),
            "adverse_direction": bool(diag.get("adverse_direction", False)),
            "adverse_ret": bool(diag.get("adverse_ret", False)),
            "adverse_weighted_mid_proxy": bool(diag.get("adverse_weighted_mid_proxy", False)),
            "adverse_thin_depth": bool(diag.get("adverse_thin_depth", False)),
            "local_extreme_guard": bool(diag.get("local_extreme_guard", False)),
            "local_extreme_pause": bool(diag.get("local_extreme_pause", False)),
            "local_extreme_rank": float(diag.get("local_extreme_rank", 0.5)),
            "local_extreme_window_s": float(diag.get("local_extreme_window_s", 0.0)),
            "defense_guard": bool(diag.get("defense_guard", False)),
            "defense_pause": bool(diag.get("defense_pause", False)),
            "defense_reducing": bool(diag.get("defense_reducing", False)),
            "defense_emergency": bool(diag.get("defense_emergency", False)),
            "defense_markout": bool(diag.get("defense_markout", False)),
            "defense_direction": bool(diag.get("defense_direction", False)),
            "defense_ret": bool(diag.get("defense_ret", False)),
            "defense_weighted_mid_proxy": bool(diag.get("defense_weighted_mid_proxy", False)),
            "defense_spread_mult": float(diag.get("defense_spread_mult", 1.0)),
            "final_compressed": bool(diag.get("final_compressed", False)),
            "bid_adverse": bool(diag.get("bid_adverse", False)),
            "ask_adverse": bool(diag.get("ask_adverse", False)),
            "bid_quote_ev": bool(diag.get("bid_quote_ev", False)),
            "bid_quote_ev_action": diag.get("bid_quote_ev_action", "none"),
            "bid_quote_ev_30s": float(diag.get("bid_quote_ev_30s", 0.0)),
            "bid_quote_toxic_30s": float(diag.get("bid_quote_toxic_30s", 0.0)),
            "bid_quote_fill_prob": float(diag.get("bid_quote_fill_prob", 0.0)),
            "bid_quote_fill_markout_30s": float(diag.get("bid_quote_fill_markout_30s", 0.0)),
            "ask_quote_ev": bool(diag.get("ask_quote_ev", False)),
            "ask_quote_ev_action": diag.get("ask_quote_ev_action", "none"),
            "ask_quote_ev_30s": float(diag.get("ask_quote_ev_30s", 0.0)),
            "ask_quote_toxic_30s": float(diag.get("ask_quote_toxic_30s", 0.0)),
            "ask_quote_fill_prob": float(diag.get("ask_quote_fill_prob", 0.0)),
            "ask_quote_fill_markout_30s": float(diag.get("ask_quote_fill_markout_30s", 0.0)),
            "buy_fill_selection_live_score": float(diag.get("buy_fill_selection_live_score", 0.0)),
            "buy_fill_selection_live_hit": bool(diag.get("buy_fill_selection_live_hit", False)),
            "buy_fill_selection_live_missing_features": int(
                diag.get("buy_fill_selection_live_missing_features", 0) or 0
            ),
            "random_passive_mirrored": bool(
                diag.get("random_passive_mirrored", False)
            ),
            "post_fill_quote_response_enabled": bool(
                diag.get("post_fill_quote_response_enabled", False)
            ),
            "post_fill_quote_response_mode": str(
                diag.get("post_fill_quote_response_mode", self.services.NOOP_MODE)
            ),
            "post_fill_quote_response_active": bool(
                diag.get("post_fill_quote_response_active", False)
            ),
            "post_fill_quote_response_role": str(
                diag.get("post_fill_quote_response_role", "flat")
            ),
            "post_fill_inventory_shift_ticks": int(
                diag.get("post_fill_inventory_shift_ticks", 0) or 0
            ),
            "post_fill_raw_add_widen_ticks": int(
                diag.get("post_fill_raw_add_widen_ticks", 0) or 0
            ),
            "post_fill_add_widen_ticks": int(
                diag.get("post_fill_add_widen_ticks", 0) or 0
            ),
            "post_fill_response_excitation": float(
                diag.get("post_fill_response_excitation", 0.0) or 0.0
            ),
            "post_fill_response_half_life_s": float(
                diag.get("post_fill_response_half_life_s", 0.0) or 0.0
            ),
            "post_fill_response_refill_edge": float(
                diag.get("post_fill_response_refill_edge", 0.0) or 0.0
            ),
            "post_fill_response_repair_probability": float(
                diag.get("post_fill_response_repair_probability", self.services.math.nan)
            ),
            "post_fill_response_cap_headroom_ticks": int(
                diag.get("post_fill_response_cap_headroom_ticks", 0) or 0
            ),
            "post_fill_response_cap_limited": bool(
                diag.get("post_fill_response_cap_limited", False)
            ),
            "post_fill_response_post_only_clamped": bool(
                diag.get("post_fill_response_post_only_clamped", False)
            ),
            "final_guard_changed": bool(diag.get("final_guard_changed", False)),
            "any_constraint_changed": bool(diag.get("any_constraint_changed", False)),
            "queue_regime_mult": float(order.get("queue_regime_mult", 1.0)),
            "queue_mo_mult": float(order.get("queue_mo_mult", 1.0)),
            "queue_inventory_mult": float(order.get("queue_inventory_mult", 1.0)),
            "queue_inventory_exposure_increasing": bool(
                order.get("queue_inventory_exposure_increasing", False)
            ),
            "queue_deplete_mult": float(order.get("queue_deplete_mult", 1.0)),
            "queue_reason_bucket": order.get("queue_reason_bucket", "none"),
            "queue_local_rank": float(order.get("queue_local_rank", 0.5)),
            "fill_eligible": bool(order.get("fill_eligible", True)),
            "queue_value_keep_cancel_intervention_id": int(
                order.get(
                    "queue_value_keep_cancel_intervention_id", -1
                )
                or -1
            ),
            "queue_value_keep_cancel_action": str(
                order.get("queue_value_keep_cancel_action", "") or ""
            ),
        }
        if order.get("activation_book_status") == "UNKNOWN":
            fields["activation_book_status"] = "UNKNOWN"
        if "activation_book_evidence" in order:
            fields["activation_book_evidence"] = order["activation_book_evidence"]
        if "simulator_queue_source" in order:
            fields["simulator_queue_source"] = order["simulator_queue_source"]
            fields["exchange_book_queue_asof_ts_ns"] = order.get("exchange_book_queue_asof_ts_ns")
            fields["exchange_book_queue_segment_id"] = order.get("exchange_book_queue_segment_id")
        if (self.state.new_order_latency_split_enabled or order.get("restored_order", False)
                or order.get("activation_book_status") == "UNKNOWN"):
            fields["new_ack_ts"] = int(order.get("new_ack_ts", 0) or 0)
            fields["exchange_accepted"] = bool(
                order.get("exchange_accepted", False)
            )
            fields["local_new_ack_published"] = bool(
                order.get("local_new_ack_published", False)
            )
        if order.get("restored_order", False):
            fields["restored_order"] = True
            fields["restored_queue_status"] = str(order.get("restored_queue_status", "unknown"))
            fields["simulator_queue_source"] = str(order.get("simulator_queue_source", "unknown"))
            fields["exact_queue_path_valid"] = False
        if (
            self.state.new_order_latency_split_enabled
            or self.state.cancel_latency_split_enabled
            or self.state.rest_gateway_timing_enabled
            or self.state.decision_to_gateway_latency_enabled
        ):
            fields["gateway_request_ts"] = int(
                order.get("gateway_request_ts", order.get("submit_ts", 0)) or 0
            )
            fields["cancel_request_ts"] = int(
                order.get("cancel_request_ts", -1) or -1
            )
            fields["cancel_effective_ts"] = int(
                order.get("cancel_effective_ts", -1) or -1
            )
            fields["cancel_ack_ts"] = int(order.get("cancel_ts", -1) or -1)
        if self.state.serial_rest_return_enabled:
            fields["new_rest_return_ts"] = int(order.get("new_rest_return_ts", -1))
            fields["cancel_rest_return_ts"] = int(order.get("cancel_rest_return_ts", -1))
            fields["restored_order"] = bool(order.get("restored_order", False))
            fields["cancel_terminal_suppressed_full_fill"] = bool(
                order.get("cancel_terminal_suppressed_full_fill", False)
            )
            if fields["cancel_terminal_suppressed_full_fill"]:
                # This timestamp was only the sampled success-path callback.
                # An exchange-filled order cannot produce that cancel ACK.
                fields["cancel_ack_ts"] = -1
        if self.state.private_fill_visibility_enabled:
            fields["exchange_remaining"] = float(
                order.get("exchange_remaining", order.get("remaining", 0.0))
            )
            fields["last_exchange_fill_ts_ms"] = int(
                order.get("last_exchange_fill_ts_ms", 0) or 0
            )
            fields["last_private_fill_visible_ts_ms"] = int(
                order.get("last_private_fill_visible_ts_ms", 0) or 0
            )
        if self.state.async_rest_gateway:
            fields["async_new_queued"] = order.get("gateway_request_ts") == self.state.async_rest_unset_ms
            fields["async_cancel_queued"] = order.get("cancel_request_ts") == self.state.async_rest_unset_ms
            for name in (
                "gateway_request_ts", "activate_ts", "new_ack_ts", "new_rest_return_ts",
                "cancel_request_ts", "cancel_effective_ts", "cancel_ack_ts",
                "cancel_rest_return_ts",
            ):
                if fields.get(name) == self.state.async_rest_unset_ms:
                    fields[name] = -1
            fields["new_private_visibility_ts"] = int(order.get("new_private_visibility_ts", -1))
            fields["cancel_private_visibility_ts"] = int(
                order.get("cancel_private_visibility_ts", -1)
            )
        if order.get("time_in_force") == "IOC":
            fields["ioc_terminal_visible_ts"] = int(order.get("ioc_terminal_visible_ts", -1))
            fields["ioc_local_terminal"] = bool(order.get("ioc_local_terminal", False))
        return fields

    def _append_order_outcome(self, order, outcome_ts: int, outcome: str, reason: str,
                              fill_qty: float = 0.0):
        event = self.state.l2_production.observe("order_outcome", outcome_ts, order["side"])
        self.state.trace_attempt_counts["order_outcomes"] += 1
        if (self.state.l2_journal is None and
                (self.state.trace_orders is None or len(self.state.trace_orders) >= self.state.trace_quotes_max)):
            return
        row = self._order_trace_fields(order)
        row.update({
            "outcome": outcome,
            "outcome_ts": int(outcome_ts),
            "lifetime_ms": int(max(0, outcome_ts - order["quote_ts"])),
            "cancel_reason": reason,
            "fill_qty": float(fill_qty),
            "remaining": float(order.get("remaining", 0.0)),
            "queue_init": float(order.get("queue_init", 0.0)),
            "queue_left": float(order.get("queue_left", 0.0)),
            "pending_cancel": bool(order.get("state") == self.state.ORDER_PENDING_CANCEL),
        })
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("order_outcome", outcome_ts, side=order["side"], payload=row, production_event=event)
        if self.state.trace_orders is not None and len(self.state.trace_orders) < self.state.trace_quotes_max:
            self.state.trace_orders.append(row)

    def _should_trace_queue_event(self, order: dict, event_ts: int) -> bool:
        if self.state.trace_queue_events is None or len(self.state.trace_queue_events) >= self.state.trace_queue_events_max:
            return False
        if self.state.trace_queue_start_ms is not None and event_ts < self.state.trace_queue_start_ms:
            return False
        if self.state.trace_queue_end_ms is not None and event_ts >= self.state.trace_queue_end_ms:
            return False
        if self.state.trace_queue_order_ids and int(order.get("trace_id", -1)) not in self.state.trace_queue_order_ids:
            return False
        return True

    def _append_queue_event(self,
        *,
        event: str,
        trade_idx: int,
        order: dict,
        trade_price: float,
        trade_qty: float,
        deplete_mult: float,
        trade_remaining_before: float,
        trade_remaining_after: float,
        queue_before: float,
        queue_after: float,
        fill_qty: float = 0.0,
        skip_reason: str = "",
    ) -> None:
        production_event = self.state.l2_production.observe("queue_event", int(self.state.trade_ts[trade_idx]), order["side"])
        trace_memory = self._should_trace_queue_event(order, int(self.state.trade_ts[trade_idx]))
        if self.state.l2_journal is None and not trace_memory:
            return
        row = {
            "event": event,
            "trade_idx": int(trade_idx),
            "trade_ts": int(self.state.trade_ts[trade_idx]),
            "timestamp": self.services.pd.to_datetime(int(self.state.trade_ts[trade_idx]), unit="ms", utc=True).isoformat(),
            "order_id": int(order.get("trace_id", -1)),
            "side": order.get("side", ""),
            "state": order.get("state", ""),
            "fill_eligible": bool(order.get("fill_eligible", True)),
            "order_price": float(order.get("price", 0.0)),
            "order_remaining": float(order.get("remaining", 0.0)),
            "order_cancel_ts": int(order.get("cancel_ts", 0) or 0),
            "order_cancel_effective_ts": int(
                self._order_cancel_effective_ts(order)
            ),
            "trade_price": float(trade_price),
            "trade_qty": float(trade_qty),
            "deplete_mult": float(deplete_mult),
            "trade_remaining_before": float(trade_remaining_before),
            "trade_remaining_after": float(trade_remaining_after),
            "queue_before": float(queue_before),
            "queue_after": float(queue_after),
            "fill_qty": float(fill_qty),
            "skip_reason": skip_reason,
        }
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("queue_event", row["trade_ts"], side=order["side"], payload=row, production_event=production_event)
        if trace_memory:
            self.state.trace_queue_events.append(row)

    def _decision_reason_text(self, side_ctx: dict, block_reasons: list[str]) -> str:
        reasons = list(block_reasons)
        if side_ctx.get("policy_markout", False):
            reasons.append("markout")
        if side_ctx.get("side_adverse_pause", False) or side_ctx.get("side_adverse", False):
            reasons.append("adverse")
        if side_ctx.get("defense_pause", False) or side_ctx.get("defense_guard", False):
            reasons.append("defense")
        if side_ctx.get("local_extreme_pause", False) or side_ctx.get("local_extreme_guard", False):
            reasons.append("local_extreme")
        if side_ctx.get("bid_quote_ev_action", "none") in ("pause", "widen", "tighten"):
            reasons.append(f"bid_quote_ev_{side_ctx.get('bid_quote_ev_action')}")
        if side_ctx.get("ask_quote_ev_action", "none") in ("pause", "widen", "tighten"):
            reasons.append(f"ask_quote_ev_{side_ctx.get('ask_quote_ev_action')}")
        if side_ctx.get("adverse_thin_depth", False):
            reasons.append("thin_depth")
        if side_ctx.get("inventory_lifecycle_stop_add", False):
            reasons.append("inventory_lifecycle_stop_add")
        if side_ctx.get("inventory_lifecycle_soft_control", False):
            reasons.append("inventory_lifecycle_soft_control")
        if side_ctx.get("multi_market_policy_effective_block", False):
            reasons.append("multi_market_post_fill_stop_add")
        seen = set()
        unique = []
        for reason in reasons:
            if reason and reason not in seen:
                unique.append(str(reason))
                seen.add(reason)
        return "|".join(unique) if unique else "none"

    def _decision_mode(self, can_post: bool, side_ctx: dict) -> str:
        if not can_post:
            return "pause"
        if (
            side_ctx.get("defense_guard", False)
            or side_ctx.get("policy_markout", False)
            or side_ctx.get("side_adverse", False)
            or side_ctx.get("local_extreme_guard", False)
        ):
            return "defend"
        return "normal"

    def _decision_visible_trade_index(self,
        visible_cutoff_ts_ms: int,
        current_event_index: int,
    ) -> int:
        if self.state.last_execution_event_index.size == 0 or current_event_index < 0:
            return -1
        if self.state.exec_message_schedules:
            parent_idx, _, _ = self._message_clock_state(
                "trade", int(self.state.trade_ts[int(current_event_index)])
            )
            if parent_idx < 0:
                return -1
            child_idx = int(
                self.state.exec_message_payload["trade"]["last_child_row_index"][parent_idx]
            )
            return (
                min(int(current_event_index), int(self.state.exec_message_trade_event_rows[child_idx]))
                if child_idx >= 0 else -1
            )
        right = min(
            int(current_event_index) + 1,
            int(
                self.services.np.searchsorted(
                    self.state.trade_ts,
                    int(visible_cutoff_ts_ms),
                    side="right",
                )
            ),
        )
        if right <= 0:
            return -1
        return int(self.state.last_execution_event_index[right - 1])

    def _market_generation_identity(self, generation: Mapping[str, int]) -> str:
        encoded = self.services.json.dumps(
            {str(key): int(value) for key, value in generation.items()},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        return self.services.hashlib.sha256(encoded).hexdigest()

    def _market_generation_strictly_after(self,
        current: Mapping[str, int],
        previous: Mapping[str, int],
    ) -> bool:
        cursor_fields = (
            "bbo_index",
            "l2_index",
            "trade_index",
            "feature_ready_index",
            "prediction_index",
        )
        if any(
            int(current[field]) < int(previous[field])
            for field in cursor_fields
        ):
            raise RuntimeError("external market generation regressed")
        return any(
            int(current[field]) != int(previous[field])
            for field in (*cursor_fields, "snapshot_mid_tick_x2")
        )

    def _external_market_generation_at_event(self,
        now_ts: int,
        current_event_index: int,
        fallback_mid: float,
    ) -> tuple[dict[str, int], bool]:
        visible_book = self._decision_visible_book(int(now_ts), float(fallback_mid))
        visible_trade_ts = int(visible_book["visible_trade_ts"])
        visible_trade_index = self._decision_visible_trade_index(
            visible_trade_ts,
            int(current_event_index),
        )
        feature_ready_index = (
            int(
                self.services.np.searchsorted(
                    self.context.var_ts_ms,
                    visible_trade_ts - 1_000,
                    side="right",
                )
                - 1
            )
            if len(self.context.var_ts_ms) > 0
            else -1
        )
        prediction_cutoff_ts = (visible_trade_ts // 1_000) * 1_000
        prediction_index = (
            int(
                self.services.np.searchsorted(
                    self.state.ml_ts,
                    prediction_cutoff_ts,
                    side="right",
                )
                - 1
            )
            if len(self.state.ml_ts) > 0
            else -1
        )
        if self.state.exec_message_schedules:
            feature_ready_index = self._message_clock_state("variance", int(now_ts))[0]
            prediction_index = (
                self._message_clock_state("prediction", int(now_ts))[0]
                if "prediction" in self.state.exec_message_schedules else -1
            )
        generation = {
            "bbo_index": int(visible_book["bbo_idx"]),
            "l2_index": int(visible_book["l2_idx"]),
            "trade_index": int(visible_trade_index),
            "feature_ready_index": int(feature_ready_index),
            "prediction_index": int(prediction_index),
            "snapshot_mid_tick_x2": int(
                round(2.0 * float(visible_book["mid"]) / float(self.state.TICK))
            ),
        }
        ready = bool(
            generation["bbo_index"] >= 0
            and generation["l2_index"] >= 0
            and generation["trade_index"] >= 0
            and generation["feature_ready_index"] >= 0
            and generation["prediction_index"] >= 0
            and generation["snapshot_mid_tick_x2"] > 0
        )
        return generation, ready

    def _append_decision_trace(self,
        side: str,
        now_ts: int,
        *,
        scheduled_requote: bool,
        market_event_generation: int,
        market_event_is_execution_trade: bool,
        decision_bbo_index: int,
        decision_l2_index: int,
        trade_visible_ts_ms: int,
        feature_ready_index: int,
        prediction_index: int,
        can_post_before_fill_cooldown: bool,
        can_post_after_fill_cooldown: bool,
        pending_before: bool,
        can_post: bool,
        can_post_after_inventory: bool,
        exposure_increasing: bool,
        allow_exposure_increase: bool,
        needs_update: bool,
        active_before: bool,
        action: str,
        base_price: float,
        final_price: float,
        base_size: float,
        final_size: float,
        spread_mult: float,
        size_mult: float,
        side_ctx: dict,
        quote_diag: dict,
        block_reasons: list[str],
        route_allowed: bool = True,
    ) -> None:
        production_event = self.state.l2_production.observe(
            "decision" if route_allowed else "decision_route_excluded", now_ts, side)
        if self.state.l2_journal is not None:
            # Exact callback/context identity, not a nearest-time guess.
            self.state.l2_decision_links[(id(side_ctx), int(now_ts), side)] = production_event['sequence']
        if not route_allowed:
            self.state.trace_attempt_counts["excluded_route_calls"] += 1
            if self.state.l2_journal is not None:
                self.state.l2_journal.emit("decision_route_excluded", now_ts, side=side,
                    payload={"action": action, "route_allowed": False,
                             "decision_id": f"{self.services.SYMBOL}:{int(now_ts)}:{side}",
                             "feature_ready_index": feature_ready_index,
                             "prediction_index": prediction_index,
                             "decision_bbo_index": decision_bbo_index,
                             "decision_l2_index": decision_l2_index,
                             "trade_visible_ts_ms": trade_visible_ts_ms,
                             "block_reasons": block_reasons, "base_price": base_price,
                             "final_price": final_price, "base_size": base_size,
                             "final_size": final_size, "quote_diag": quote_diag}, production_event=production_event)
            return
        self._cooldown_lineage_observe_decision(
            side,
            int(now_ts),
            can_post=bool(can_post),
            exposure_increasing=bool(exposure_increasing),
            block_reasons=block_reasons,
            side_ctx=side_ctx,
        )
        self.state.trace_attempt_counts["routed_decisions"] += 1
        if (self.state.l2_journal is None and
                (self.state.trace_decisions is None or len(self.state.trace_decisions) >= self.state.trace_decisions_max)):
            return
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
        side_consecutive = self.state.consec_buy if side == "BUY" else self.state.consec_sell
        side_elapsed_ms = max(0, int(now_ts - side_last_fill_ts))
        baseline_wall_active = bool(
            side_consecutive > 1e-10
            and side_cooldown_active_ms > 0.0
            and side_elapsed_ms < side_cooldown_active_ms
        )
        effective_cooldown_active = self._fill_cooldown_active(side, int(now_ts))
        variance_state = self._variance_trace_state(side, int(now_ts))
        visible_trade_index = self._decision_visible_trade_index(
            int(trade_visible_ts_ms),
            int(market_event_generation),
        )
        def absolute_index(value, clock):
            return int(value) + self.state.input_clock_offsets[clock] if int(value) >= 0 else int(value)

        snapshot_mid_tick_x2 = int(round(2.0 * float(self.state.mid) / float(self.state.TICK)))
        market_generation = (
            f"bbo={absolute_index(decision_bbo_index, 'bbo')}|l2={absolute_index(decision_l2_index, 'l2')}|"
            f"trade={absolute_index(visible_trade_index, 'trade')}|feature={absolute_index(feature_ready_index, 'variance')}|"
            f"prediction={absolute_index(prediction_index, 'prediction')}|mid_x2={snapshot_mid_tick_x2}"
        )
        market_readiness = bool(
            int(decision_bbo_index) >= 0
            and int(decision_l2_index) >= 0
            and int(visible_trade_index) >= 0
            and int(feature_ready_index) >= 0
            and int(prediction_index) >= 0
            and snapshot_mid_tick_x2 > 0
        )
        prospective_queue_ahead_btc = self.services.math.nan
        prospective_queue_ahead_source = "not_an_eligible_add_opportunity"
        prospective_add_opportunity = bool(
            scheduled_requote
            and market_readiness
            and self.state.inventory_lifecycle_active
            and not pending_before
            and not active_before
            and can_post_before_fill_cooldown
            and (
                (side == "BUY" and float(self.state.q) > 0.0)
                or (side == "SELL" and float(self.state.q) < 0.0)
            )
        )
        if prospective_add_opportunity:
            distance = (
                max(0.0, float(self.state.mid) - float(final_price))
                if side == "BUY"
                else max(0.0, float(final_price) - float(self.state.mid))
            )
            near_depth_total = float(
                side_ctx.get(
                    "l2_near_depth_total",
                    side_ctx.get("near_depth_total", 0.0),
                )
                or 0.0
            )
            prospective_order = {"quote_diag": quote_diag}
            reason_bucket = self._queue_reason_bucket(
                prospective_order,
                side,
                near_depth_total,
            )
            regime_mult = self._queue_regime_multiplier(
                side,
                distance,
                near_depth_total,
                (
                    self._local_rank_at_ts(int(now_ts))
                    if self.state.queue_regime_lookup
                    else 0.5
                ),
                reason_bucket,
                int(now_ts),
            )
            inventory_mult, _ = self._queue_inventory_multiplier(side, float(self.state.q))
            markout_mult = self._queue_mo_multiplier(
                prospective_order,
                side,
                int(now_ts),
            )
            visible_queue = self._l2_visible_queue_ahead(
                side,
                float(final_price),
                target_ts=int(now_ts),
            )
            prospective_queue_ahead_btc = self._estimate_queue_ahead(
                side,
                float(final_price),
                distance,
                target_ts=int(now_ts),
                regime_queue_mult=(
                    float(regime_mult)
                    * float(markout_mult)
                    * float(inventory_mult)
                ),
            )
            prospective_queue_ahead_source = (
                "decision_visible_l2_exact_price"
                if visible_queue is not None
                else "baseline_fallback_queue_model"
            )
        decision_feature_names = (
            "volatility_5s",
            "volatility_30s",
            "volatility_60s",
            "volatility_300s",
            "volume_imbalance_5s",
            "volume_imbalance_30s",
            "volume_imbalance_60s",
            "volume_imbalance_300s",
            "trade_intensity_5s",
            "trade_intensity_30s",
            "trade_intensity_60s",
            "trade_intensity_300s",
            "vpin_5s",
            "vpin_30s",
            "vpin_60s",
            "vpin_300s",
            "price_change_5s",
            "price_change_30s",
            "price_change_60s",
            "price_change_300s",
            "taker_quote_imbalance_5s",
            "taker_quote_imbalance_10s",
            "taker_quote_imbalance_30s",
            "taker_quote_imbalance_60s",
        )
        decision_feature_values = {
            f"decision_feature_{name}": float(
                self.state.cur_prediction_features.get(name, 0.0) or 0.0
            )
            for name in decision_feature_names
        }
        trace_row = {
            "decision_id": f"{self.services.SYMBOL}:{int(now_ts)}:{side}",
            "timestamp": float(now_ts) / 1000.0,
            "ts_ms": int(now_ts),
            "decision_ts_ns": int(now_ts) * 1_000_000,
            "symbol": self.services.SYMBOL,
            "side": side,
            "canonical_external_decision": int(bool(scheduled_requote)),
            "market_event_generation": absolute_index(market_event_generation, "trade"),
            "market_event_is_execution_trade": int(
                bool(market_event_is_execution_trade)
            ),
            "quote_snapshot_market_generation": market_generation,
            "decision_visible_bbo_index": absolute_index(decision_bbo_index, "bbo"),
            "decision_visible_l2_index": absolute_index(decision_l2_index, "l2"),
            "decision_visible_trade_cutoff_ts_ms": int(trade_visible_ts_ms),
            "decision_visible_trade_index": absolute_index(visible_trade_index, "trade"),
            "feature_ready_generation_index": absolute_index(feature_ready_index, "variance"),
            "prediction_generation_index": absolute_index(prediction_index, "prediction"),
            "quote_snapshot_mid_tick_x2": int(snapshot_mid_tick_x2),
            "external_epoch_support_valid": 0,
            "external_epoch_support_reason": (
                "pending_next_ready_market_generation"
                if self.state.trace_external_market_release
                and bool(scheduled_requote)
                and market_readiness
                else "external_market_release_not_requested"
            ),
            "release_ts_ms": 0,
            "release_market_event_generation": -1,
            "release_market_generation_identity": "",
            "release_bbo_index": -1,
            "release_l2_index": -1,
            "release_trade_index": -1,
            "release_feature_ready_index": -1,
            "release_prediction_index": -1,
            "release_snapshot_mid_tick_x2": -1,
            "feature_ready_ts_ms": int(
                (
                    self._message_clock_state("prediction", int(now_ts))[2] + 999_999
                ) // 1_000_000
                if "prediction" in self.state.exec_message_schedules
                else (
                    self.state.ml_ts[prediction_index]
                    if 0 <= int(prediction_index) < len(self.state.ml_ts)
                    else 0
                )
            ),
            "market_readiness": int(market_readiness),
            "mode": self._decision_mode(can_post, side_ctx),
            "allow_post": int(can_post),
            "allow_exposure_increase": int(allow_exposure_increase),
            "exposure_increasing": int(exposure_increasing),
            "reason_text": self._decision_reason_text(side_ctx, block_reasons),
            "markout_action_observation": {
                "side_adverse_pause": side_ctx.get("side_adverse_pause", "NOT_EVALUATED"),
                "role_scoped_generic_markout_enabled": side_ctx.get(
                    "role_scoped_generic_markout_enabled", "NOT_EVALUATED"),
                "joint_block_reasons": list(block_reasons),
                "independent_veto": "NOT_EVALUATED",
                "mechanical_price_quantity_contribution": "NOT_EVALUATED",
                "scope": "original_decision_callback; no_counterfactual_policy_evaluation",
            },
            "spread_mult": float(spread_mult),
            "size_mult": float(size_mult),
            "inventory": float(self.state.q),
            "inventory_ratio": float(self.state.q / max(self.state.max_inv, 1e-9)),
            "last_side_fill_ts_ms": int(side_last_fill_ts),
            "fill_cooldown_elapsed_ms": int(side_elapsed_ms),
            "fill_cooldown_total_ms": float(side_cooldown_total_ms),
            "fill_cooldown_active_ms": float(side_cooldown_active_ms),
            "fill_cooldown_remaining_ms": float(
                max(0.0, side_cooldown_total_ms - side_elapsed_ms)
            ),
            "fill_cooldown_consecutive_units": float(side_consecutive),
            "fill_cooldown_clock_mode": self.state.fill_cooldown_clock_mode,
            "baseline_wall_fill_cooldown_active": int(baseline_wall_active),
            "effective_fill_cooldown_active": int(effective_cooldown_active),
            **({
                "latent_cooldown_active": bool(effective_cooldown_active),
                "cooldown_blocks_action": self.services.inventory_cooldown_blocks_action(
                    bool(effective_cooldown_active), self.context.inventory_cooldown_mode),
                "inventory_cooldown_mode": self.context.inventory_cooldown_mode,
                "inventory_generic_markout_scope": self.context.inventory_markout_scope,
                "pre_generic_role": side_ctx.get("pre_generic_role", "not_requested"),
                "pre_generic_quantity": side_ctx.get("pre_generic_quantity"),
                "release_rounded_quantity": side_ctx.get("release_rounded_quantity"),
                "final_quantity_role": side_ctx.get("final_quantity_role", "not_requested"),
                "role_scoped_generic_markout_enabled": side_ctx.get(
                    "role_scoped_generic_markout_enabled", True),
            } if self.context.inventory_cooldown_mode != "original"
                or self.context.inventory_markout_scope != "original_all_roles" else {}),
            "can_post_before_fill_cooldown": int(
                bool(can_post_before_fill_cooldown)
            ),
            "can_post_after_fill_cooldown": int(
                bool(can_post_after_fill_cooldown)
            ),
            "same_side_pending_before": int(bool(pending_before)),
            "decision_queue_ahead_btc": float(prospective_queue_ahead_btc),
            "decision_queue_ahead_source": str(prospective_queue_ahead_source),
            "variance_time_mechanical_diff_vs_wall": int(
                self.state.variance_time_enabled
                and baseline_wall_active != effective_cooldown_active
            ),
            **variance_state,
            "inventory_lifecycle_active": int(bool(self.state.inventory_lifecycle_active)),
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
                if self.state.inventory_lifecycle_active and self.state.decision_trace_profile == "full"
                else 0.0
            ),
            "inventory_lifecycle_adverse_excursion_so_far": (
                float(self.state.inventory_lifecycle_cur_min_pnl)
                if self.state.decision_trace_profile == "full"
                else 0.0
            ),
            "inventory_lifecycle_exposure_increasing_fills_so_far": int(
                self.state.inventory_lifecycle_exposure_increasing_fills
            ),
            "inventory_lifecycle_reducing_fills_so_far": int(self.state.inventory_lifecycle_reducing_fills),
            "toxicity": (
                float(self.state.cur_tox_bid if side == "BUY" else self.state.cur_tox_ask)
                if self.state.decision_trace_profile == "full"
                else 0.0
            ),
            "markout_ema": (
                float(self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask)
                if self.state.decision_trace_profile == "full"
                else 0.0
            ),
            "depth_age_s": float(side_ctx.get("depth_age_s", 0.0) or 0.0),
            "pred_dir": float(side_ctx.get("pred_dir", 0.5) or 0.5),
            "pred_ret": float(side_ctx.get("pred_ret", 0.0) or 0.0),
            "fair": float(side_ctx.get("fair", self.state.mid) or self.state.mid),
            "raw_reservation_shift": float(
                side_ctx.get("raw_reservation_shift", 0.0) or 0.0
            ),
            "raw_asym_shift": float(side_ctx.get("raw_asym_shift", 0.0) or 0.0),
            "generic_markout_policy_enabled": self.context.generic_markout_enabled,
            "generic_markout_negative_condition": bool(
                (self.state.mo_ema_bid if side == "BUY" else self.state.mo_ema_ask) < 0.0
                and self.state.markout_spread_scale > 0.0),
            "asym": float(side_ctx.get("asym", 0.0) or 0.0),
            "sigma_sq_raw": float(quote_diag.get("sigma_sq_raw", 0.0) or 0.0),
            **({
                "quote_trade_intensity": float(self.state.cur_ti),
                "quote_trade_intensity_completed_ts_ms": int(self.state.quote_ti_completed_ts_ms),
            } if self.context.var_ti is not None else {}),
            "sigma_sq_blended": float(
                quote_diag.get("sigma_sq_blended", 0.0) or 0.0
            ),
            "quote_horizon_s": float(
                quote_diag.get("quote_horizon_s", 0.0) or 0.0
            ),
            "regime_spread_scale": float(
                float(quote_diag.get("delta_after_regime", 0.0) or 0.0)
                / max(
                    float(quote_diag.get("delta_raw", 0.0) or 0.0),
                    1e-12,
                )
            ),
            "delta_raw": float(quote_diag.get("delta_raw", 0.0) or 0.0),
            "delta_after_regime": float(
                quote_diag.get("delta_after_regime", 0.0) or 0.0
            ),
            "delta_pre_cap": float(quote_diag.get("delta_pre_cap", 0.0) or 0.0),
            "delta_after_cap": float(
                quote_diag.get("delta_after_cap", 0.0) or 0.0
            ),
            "half_d": float(quote_diag.get("half_d", 0.0) or 0.0),
            "cap_bps": float(quote_diag.get("cap_bps", 0.0) or 0.0),
            "max_spread": float(
                quote_diag.get("max_spread", 0.0) or 0.0
            ),
            "delta_cap_hit": int(
                bool(quote_diag.get("delta_cap_hit", False))
            ),
            "cap_hit": int(bool(quote_diag.get("cap_hit", False))),
            "final_compressed": int(
                bool(quote_diag.get("final_compressed", False))
            ),
            "kappa_used": float(
                quote_diag.get("kappa_used", 0.0) or 0.0
            ),
            "weighted_mid_proxy_shift_bps": float(side_ctx.get("weighted_mid_proxy_shift_bps", 0.0)),
            "l2_quote_flip_rate": float(side_ctx.get("l2_quote_flip_rate", 0.0)),
            "l2_book_refresh_ratio": float(side_ctx.get("l2_book_refresh_ratio", 0.0)),
            "l2_book_cancel_ratio": float(side_ctx.get("l2_book_cancel_ratio", 0.0)),
            "l2_near_depth_total": float(side_ctx.get("l2_near_depth_total", side_ctx.get("near_depth_total", 0.0))),
            "bid_quote_ev_30s": float(side_ctx.get("bid_quote_ev_30s", 0.0)),
            "bid_quote_toxic_30s": float(side_ctx.get("bid_quote_toxic_30s", 0.0)),
            "bid_quote_fill_prob": float(side_ctx.get("bid_quote_fill_prob", 0.0)),
            "bid_quote_fill_markout_30s": float(side_ctx.get("bid_quote_fill_markout_30s", 0.0)),
            "ask_quote_ev_30s": float(side_ctx.get("ask_quote_ev_30s", 0.0)),
            "ask_quote_toxic_30s": float(side_ctx.get("ask_quote_toxic_30s", 0.0)),
            "ask_quote_fill_prob": float(side_ctx.get("ask_quote_fill_prob", 0.0)),
            "ask_quote_fill_markout_30s": float(side_ctx.get("ask_quote_fill_markout_30s", 0.0)),
            "inventory_lifecycle_stop_add": int(bool(side_ctx.get("inventory_lifecycle_stop_add", False))),
            "inventory_lifecycle_soft_control": int(bool(side_ctx.get("inventory_lifecycle_soft_control", False))),
            "inventory_lifecycle_soft_spread_mult": float(side_ctx.get("inventory_lifecycle_soft_spread_mult", 1.0) or 1.0),
            "post_fill_quote_response_enabled": int(
                bool(side_ctx.get("post_fill_quote_response_enabled", False))
            ),
            "post_fill_quote_response_mode": str(
                side_ctx.get("post_fill_quote_response_mode", self.services.NOOP_MODE)
            ),
            "post_fill_quote_response_active": int(
                bool(side_ctx.get("post_fill_quote_response_active", False))
            ),
            "post_fill_quote_response_role": str(
                side_ctx.get("post_fill_quote_response_role", "flat")
            ),
            "post_fill_inventory_shift_ticks": int(
                side_ctx.get("post_fill_inventory_shift_ticks", 0) or 0
            ),
            "post_fill_raw_add_widen_ticks": int(
                side_ctx.get("post_fill_raw_add_widen_ticks", 0) or 0
            ),
            "post_fill_add_widen_ticks": int(
                side_ctx.get("post_fill_add_widen_ticks", 0) or 0
            ),
            "post_fill_response_excitation": float(
                side_ctx.get("post_fill_response_excitation", 0.0) or 0.0
            ),
            "post_fill_response_half_life_s": float(
                side_ctx.get("post_fill_response_half_life_s", 0.0) or 0.0
            ),
            "post_fill_response_refill_edge": float(
                side_ctx.get("post_fill_response_refill_edge", 0.0) or 0.0
            ),
            "post_fill_response_repair_probability": float(
                side_ctx.get("post_fill_response_repair_probability", self.services.math.nan)
            ),
            "post_fill_response_cap_limited": int(
                bool(side_ctx.get("post_fill_response_cap_limited", False))
            ),
            "multi_market_policy_mode": str(side_ctx.get("multi_market_policy_mode", self.services.NOOP_MODE)),
            "multi_market_policy_hit": int(bool(side_ctx.get("multi_market_policy_hit", False))),
            "multi_market_policy_effective_block": int(
                bool(side_ctx.get("multi_market_policy_effective_block", False))
            ),
            "multi_market_policy_reason": str(side_ctx.get("multi_market_policy_reason", "noop")),
            "multi_market_policy_baseline_allow_post": int(
                side_ctx.get("multi_market_policy_baseline_allow_post", 0) or 0
            ),
            "multi_market_policy_candidate_allow_post": int(
                side_ctx.get("multi_market_policy_candidate_allow_post", 0) or 0
            ),
            "multi_market_global_state_valid": int(
                side_ctx.get("multi_market_global_state_valid", 0) or 0
            ),
            "multi_market_spot_fresh_venues": int(
                side_ctx.get("multi_market_spot_fresh_venues", 0) or 0
            ),
            "multi_market_perp_fresh_venues": int(
                side_ctx.get("multi_market_perp_fresh_venues", 0) or 0
            ),
            "multi_market_spot_venue_agreement": float(
                side_ctx.get("multi_market_spot_venue_agreement", 0.0) or 0.0
            ),
            "multi_market_perp_venue_agreement": float(
                side_ctx.get("multi_market_perp_venue_agreement", 0.0) or 0.0
            ),
            "multi_market_repair_probability": float(
                side_ctx.get("multi_market_repair_probability", self.services.math.nan)
            ),
            "multi_market_repair_probability_change": float(
                side_ctx.get("multi_market_repair_probability_change", self.services.math.nan)
            ),
            "multi_market_repair_probability_age_ms": float(
                side_ctx.get("multi_market_repair_probability_age_ms", self.services.math.inf)
            ),
            "multi_market_global_flow_pressure": float(
                side_ctx.get("multi_market_global_flow_pressure", self.services.math.nan)
            ),
            "multi_market_local_bridge_move_bps": float(
                side_ctx.get("multi_market_local_bridge_move_bps", self.services.math.nan)
            ),
            "multi_market_spot_move_bps": float(
                side_ctx.get("multi_market_spot_move_bps", self.services.math.nan)
            ),
            "multi_market_perp_move_bps": float(
                side_ctx.get("multi_market_perp_move_bps", self.services.math.nan)
            ),
            "mid": float(self.state.mid),
            "best_bid": float(self.state.cur_best_bid),
            "best_ask": float(self.state.cur_best_ask),
            **{
                key: side_ctx[key]
                for key in (
                    "inventory_mark_price", "inventory_mark_source",
                    "inventory_mark_ready_ts_ns", "inventory_mark_same_ready",
                )
                if key in side_ctx
            },
            "base_price": float(base_price),
            "final_price": float(final_price),
            "base_size": float(base_size),
            "final_size": float(final_size),
            "can_post_after_inventory": int(can_post_after_inventory),
            "order_active_before": int(active_before),
            "needs_update": int(needs_update),
            "action": action,
            "post_cooldown_inventory_budget_active": int(
                str(side).upper()
                in self.state.post_cooldown_inventory_budget_active_by_side
            ),
            "post_cooldown_inventory_budget_episode_id": int(
                side_ctx.get("post_cooldown_inventory_budget_episode_id")
                if side_ctx.get("post_cooldown_inventory_budget_episode_id")
                is not None
                else -1
            ),
            "post_cooldown_inventory_budget_block": int(
                bool(
                    side_ctx.get(
                        "post_cooldown_inventory_budget_block",
                        False,
                    )
                )
            ),
            "post_cooldown_inventory_budget_available_units_before": float(
                side_ctx.get(
                    "post_cooldown_inventory_budget_available_units_before",
                    self.services.math.nan,
                )
            ),
            "post_cooldown_inventory_budget_planned_units": float(
                side_ctx.get(
                    "post_cooldown_inventory_budget_planned_units",
                    0.0,
                )
                or 0.0
            ),
            "final_distance_to_mid": float(side_ctx.get("final_distance_to_mid", 0.0)),
            "final_quote_delta_to_bbo": float(side_ctx.get("final_quote_delta_to_bbo", 0.0)),
            "final_pair_spread": float(side_ctx.get("final_pair_spread", 0.0)),
            "final_quote_skew": float(side_ctx.get("final_quote_skew", 0.0)),
            "side_adverse_pause": int(bool(side_ctx.get("side_adverse_pause", False))),
            "defense_guard": int(bool(side_ctx.get("defense_guard", False))),
            "defense_pause": int(bool(side_ctx.get("defense_pause", False))),
            "defense_reducing": int(bool(side_ctx.get("defense_reducing", False))),
            "defense_emergency": int(bool(side_ctx.get("defense_emergency", False))),
            "defense_markout": int(bool(side_ctx.get("defense_markout", False))),
            "defense_direction": int(bool(side_ctx.get("defense_direction", False))),
            "defense_ret": int(bool(side_ctx.get("defense_ret", False))),
            "defense_weighted_mid_proxy": int(
                bool(side_ctx.get("defense_weighted_mid_proxy", False))
            ),
            "local_extreme_pause": int(bool(side_ctx.get("local_extreme_pause", False))),
            **decision_feature_values,
        }
        if self.state.decision_trace_profile == "mechanics_only":
            for key in (
                "inventory_lifecycle_pnl_so_far",
                "inventory_lifecycle_adverse_excursion_so_far",
                "toxicity",
                "markout_ema",
                "bid_quote_ev_30s",
                "bid_quote_toxic_30s",
                "bid_quote_fill_markout_30s",
                "ask_quote_ev_30s",
                "ask_quote_toxic_30s",
                "ask_quote_fill_markout_30s",
                "post_fill_response_repair_probability",
                "multi_market_repair_probability",
                "multi_market_repair_probability_change",
            ):
                trace_row.pop(key, None)
        if self.state.l2_journal is not None:
            price_capture = {}
            if (self.state.quote_price_runtime is not None
                    and self.state.quote_price_runtime.contract['schema'] == 'quote-price-replay-v2'):
                price_capture = dict(depth_quoting_capture_id=self.state.quote_price_runtime.counts['captures'])
            self.state.l2_journal.emit("decision", now_ts, side=side,
                payload={**trace_row, **price_capture, "opportunity_event_id": production_event['sequence'],
                         "managed_order_ids": [int(o['trace_id']) for o in
                             (self.state.bid_orders if side == 'BUY' else self.state.ask_orders)]},
                production_event=production_event)
        if self.state.trace_decisions is not None and len(self.state.trace_decisions) < self.state.trace_decisions_max:
            self.state.trace_decisions.append(trace_row)
        if (
            self.state.trace_external_market_release
            and bool(scheduled_requote)
            and market_readiness
        ):
            target_generation = {
                "bbo_index": int(decision_bbo_index),
                "l2_index": int(decision_l2_index),
                "trade_index": int(visible_trade_index),
                "feature_ready_index": int(feature_ready_index),
                "prediction_index": int(prediction_index),
                "snapshot_mid_tick_x2": int(snapshot_mid_tick_x2),
            }
            observed_generation, observed_ready = (
                self._external_market_generation_at_event(
                    int(now_ts),
                    int(market_event_generation),
                    float(self.state.mid),
                )
            )
            if not observed_ready or observed_generation != target_generation:
                raise RuntimeError(
                    "decision trace generation disagrees with the external clock"
                )
            self.state.pending_external_market_release_rows.append(
                (
                    trace_row,
                    target_generation,
                )
            )

    def _record_quote_context(self):
        """Decorate the quote context with source clocks and policy diagnostics."""
        # latency can differ materially from bookTicker.  Using the BBO
        # visibility clock here leaks depth updates that were not yet
        # available to live SignalEngine.
        self.state.l2_refill_cancel_metrics = self._l2_refill_cancel_metrics_at(
            int(self.state.decision_book["visible_l2_ts"]),
            observation_ts=int(self.state.t),
        )
        # ``exec_depth_visibility_source_offset_ms`` aligns the archive's
        # exchange-time frame labels with the state observed by live.  It
        # must not alter freshness: paired telemetry already contains the
        # receive-time depth age used by SignalEngine.  Deriving age from
        # the offset-selected archive frame can otherwise make it negative
        # and incorrectly trigger the hard-stale policy gate.
        if self.state.exec_message_schedules:
            self.state.current_book_age_ms = float(self.state.decision_book["depth_visible_age_ms"])
        elif bool(self.state.decision_book["paired_hit"]):
            self.state.current_book_age_ms = int(self.state.decision_book["depth_delay_ms"])
        elif self.state.historical_l2 and self.state.l2_ts is not None and self.state.decision_l2_idx >= 0:
            self.state.current_book_age_ms = max(0, (int(self.state.t) * 1000 - int(self.state.l2_observation_us[self.state.decision_l2_idx])) / 1000.0)
        else:
            self.state.current_book_age_ms = self._current_book_age_ms(
                int(self.state.t), bbo_cursor=self.state.decision_bbo_idx, l2_cursor=self.state.decision_l2_idx,
            )
        self.state.current_book_age_s = (
            self.services.math.inf
            if self.state.current_book_age_ms is None
            else float(self.state.current_book_age_ms) / 1000.0
        )
        self.state.decision_bbo_source_ts_ms = (
            int(self.state.bbo_ts[self.state.decision_bbo_idx])
            if self.state.historical_bbo
            and self.state.bbo_ts is not None
            and self.state.decision_bbo_idx >= 0
            else 0
        )
        self.state.decision_l2_source_ts_ms = (
            int(self.state.l2_ts[self.state.decision_l2_idx])
            if self.state.historical_l2
            and self.state.l2_ts is not None
            and self.state.decision_l2_idx >= 0
            else 0
        )
        if self.state.decision_bbo_source_ts_ms <= 0 and self.state.decision_l2_source_ts_ms > 0:
            self.state.decision_bbo_source_ts_ms = self.state.decision_l2_source_ts_ms
            self.state.decision_bbo_cutoff_ts_ms = int(
                self.state.decision_book["visible_l2_ts"]
            )
            self.state.decision_bbo_source_kind = "l2_fallback"
        else:
            self.state.decision_bbo_cutoff_ts_ms = int(self.state.decision_book["visible_ts"])
            self.state.decision_bbo_source_kind = "bbo"
        self.state.decision_l2_feature_ready_ts_ms = (
            int(
                self.state.decision_l2_source_ts_ms
                - int(self.state.exec_depth_visibility_source_offset_ms)
                + int(self.state.decision_book["depth_delay_ms"])
            )
            if self.state.decision_l2_idx >= 0
            else 0
        )
        self.state.decision_bbo_feature_ready_ts_ms = (
            self.state.decision_l2_feature_ready_ts_ms
            if self.state.decision_bbo_source_kind == "l2_fallback"
            else (
                int(
                    self.state.decision_bbo_source_ts_ms
                    + int(self.state.decision_book["delay_ms"])
                )
                if self.state.decision_bbo_idx >= 0
                else 0
            )
        )
        if self.state.exec_message_schedules:
            self.state.decision_l2_feature_ready_ts_ms = (
                int(self.state.decision_book["depth_feature_ready_ts_ns"]) + 999_999
            ) // 1_000_000
            self.state.decision_bbo_feature_ready_ts_ms = (
                int(self.state.decision_book["bbo_feature_ready_ts_ns"]) + 999_999
            ) // 1_000_000
        if (
            self.state.decision_bbo_feature_ready_ts_ms > int(self.state.t)
            or self.state.decision_l2_feature_ready_ts_ms > int(self.state.t)
        ):
            raise RuntimeError(
                "decision-visible book source became ready after quote time"
            )
        self.state.decision_queue_local_rank = self._local_rank_at_ts(int(self.state.t))
        for loop_item in self.state.quote_context.values():
            self.state._side_ctx = loop_item
            if isinstance(self.state._side_ctx, dict):
                self.state._side_ctx.setdefault("quote_ts_ms", int(self.state.t))
                self.state._side_ctx.setdefault("depth_age_s", self.state.current_book_age_s)
                self.state._side_ctx["decision_visible_mid_usdc_per_btc"] = float(self.state.mid)
                if self.state.exec_message_schedules:
                    for loop_item in (
                        "inventory_mark_price", "inventory_mark_source",
                        "inventory_mark_ready_ts_ns", "inventory_mark_same_ready",
                    ):
                        self.state._mark_key = loop_item
                        self.state._side_ctx[self.state._mark_key] = self.state.decision_book[self.state._mark_key]
                self.state._side_ctx["decision_visible_best_bid_usdc_per_btc"] = float(
                    self.state.cur_best_bid
                )
                self.state._side_ctx["decision_visible_best_ask_usdc_per_btc"] = float(
                    self.state.cur_best_ask
                )
                self.state._side_ctx["decision_visible_bbo_cutoff_ts_ms"] = int(
                    self.state.decision_bbo_cutoff_ts_ms
                )
                self.state._side_ctx["decision_bbo_source_ts_ms"] = int(
                    self.state.decision_bbo_source_ts_ms
                )
                self.state._side_ctx["decision_bbo_source_kind"] = str(
                    self.state.decision_bbo_source_kind
                )
                self.state._side_ctx["decision_visible_l2_cutoff_ts_ms"] = int(
                    self.state.decision_book["visible_l2_ts"]
                )
                self.state._side_ctx["decision_l2_source_ts_ms"] = int(
                    self.state.decision_l2_source_ts_ms
                )
                self.state._side_ctx["decision_visible_trade_cutoff_ts_ms"] = int(
                    self.state.signal_visible_ts
                )
                self.state._side_ctx["decision_bbo_feature_ready_ts_ms"] = int(
                    self.state.decision_bbo_feature_ready_ts_ms
                )
                self.state._side_ctx["decision_l2_feature_ready_ts_ms"] = int(
                    self.state.decision_l2_feature_ready_ts_ms
                )
                self.state._side_ctx["exec_depth_visibility_source_offset_ms"] = int(
                    self.state.exec_depth_visibility_source_offset_ms
                )
                self.state._side_ctx["queue_local_rank"] = float(
                    self.state.decision_queue_local_rank
                )
        for loop_item in self.state.l2_refill_cancel_metrics.items():
            self.state._side_name, self.state._l2_metrics = loop_item
            self.state._side_ctx = self.state.quote_context.get(self.state._side_name) if isinstance(self.state.quote_context, dict) else None
            if isinstance(self.state._side_ctx, dict):
                self.state._side_ctx["l2_quote_flip_rate"] = float(
                    self.state._l2_metrics["l2_quote_flip_rate"]
                )
                self.state._side_ctx["l2_book_refresh_ratio"] = float(self.state._l2_metrics["l2_book_refresh_ratio"])
                self.state._side_ctx["l2_book_cancel_ratio"] = float(self.state._l2_metrics["l2_book_cancel_ratio"])
                self.state._side_ctx["l2_near_depth_total"] = float(self.state._l2_metrics["l2_near_depth_total"])
        self.state.delta_pre_cap_sum += self.state.d_pre_cap
        self.state.cap_bps_sum += self.state.cap_bps
        if self.state.mid_guard_bid:
            self.state.mid_guard_bid_hits += 1
        if self.state.mid_guard_ask:
            self.state.mid_guard_ask_hits += 1
        if self.state.post_only_bid or self.state.post_only_ask:
            self.state.post_only_guard_hits += int(self.state.post_only_bid) + int(self.state.post_only_ask)
            self.state.post_only_bid_hits += int(self.state.post_only_bid)
            self.state.post_only_ask_hits += int(self.state.post_only_ask)
        if self.state.final_compressed:
            self.state.final_cap_compress_count += 1
            self.state.final_cap_excess = float(self.state.diag.get("final_cap_excess", 0.0))
            self.state.final_cap_excess_sum += self.state.final_cap_excess
            if self.state.final_cap_excess > self.state.final_cap_excess_max:
                self.state.final_cap_excess_max = self.state.final_cap_excess
            if bool(self.state.diag.get("final_cap_rounding", False)):
                self.state.final_cap_rounding_count += 1
            if bool(self.state.diag.get("final_cap_mid_guard", False)):
                self.state.final_cap_mid_guard_count += 1
            if bool(self.state.diag.get("final_cap_post_only", False)):
                self.state.final_cap_post_only_count += 1
            if bool(self.state.diag.get("final_cap_delta", False)):
                self.state.final_cap_delta_count += 1
        if self.state.delta_cap_hit:
            self.state.delta_cap_hit_count += 1
        if self.state.cap_hit:
            self.state.cap_hit_count += 1
        if self.state.bid_adverse_active:
            self.state.adverse_guard_count += 1
            self.state.bid_adverse_guard_count += 1
            if bool(self.state.diag.get("bid_adverse_toxicity_active", False)):
                self.state.bid_adverse_toxicity_count += 1
            if bool(self.state.diag.get("bid_adverse_markout_active", False)):
                self.state.bid_adverse_markout_count += 1
            if bool(self.state.diag.get("bid_adverse_direction_active", False)):
                self.state.bid_adverse_direction_count += 1
            if bool(self.state.diag.get("bid_adverse_ret_active", False)):
                self.state.bid_adverse_ret_count += 1
            if bool(self.state.diag.get("bid_adverse_weighted_mid_proxy_active", False)):
                self.state.bid_adverse_weighted_mid_proxy_count += 1
            if bool(self.state.diag.get("bid_adverse_thin_depth_active", False)):
                self.state.bid_adverse_thin_depth_count += 1
            if self.state.bid_adverse_pause_active:
                self.state.adverse_pause_count += 1
                self.state.bid_adverse_pause_count += 1
        if self.state.ask_adverse_active:
            self.state.adverse_guard_count += 1
            self.state.ask_adverse_guard_count += 1
            if bool(self.state.diag.get("ask_adverse_toxicity_active", False)):
                self.state.ask_adverse_toxicity_count += 1
            if bool(self.state.diag.get("ask_adverse_markout_active", False)):
                self.state.ask_adverse_markout_count += 1
            if bool(self.state.diag.get("ask_adverse_direction_active", False)):
                self.state.ask_adverse_direction_count += 1
            if bool(self.state.diag.get("ask_adverse_ret_active", False)):
                self.state.ask_adverse_ret_count += 1
            if bool(self.state.diag.get("ask_adverse_weighted_mid_proxy_active", False)):
                self.state.ask_adverse_weighted_mid_proxy_count += 1
            if bool(self.state.diag.get("ask_adverse_thin_depth_active", False)):
                self.state.ask_adverse_thin_depth_count += 1
            if self.state.ask_adverse_pause_active:
                self.state.adverse_pause_count += 1
                self.state.ask_adverse_pause_count += 1
        if self.state.bid_defense_active:
            self.state.defense_guard_count += 1
            self.state.bid_defense_guard_count += 1
            if self.state.bid_defense_pause_active:
                self.state.defense_pause_count += 1
                self.state.bid_defense_pause_count += 1
        if self.state.ask_defense_active:
            self.state.defense_guard_count += 1
            self.state.ask_defense_guard_count += 1
            if self.state.ask_defense_pause_active:
                self.state.defense_pause_count += 1
                self.state.ask_defense_pause_count += 1

    def _record_order_decisions(self):
        """Emit original decision identities before venue request side effects."""
        # These links belong only to this quote decision. Synchronous
        # continuations copy their exact sequence into the REST plan, and
        # created orders own their link; no later decision reads this map.
        # Clear here (also after legacy checkpoint restore), not while a
        # partially completed decision still owns its two side links.
        self.state.l2_decision_links.clear()
        self._append_decision_trace(
            "BUY",
            self.state.t,
            scheduled_requote=bool(self.state.scheduled_requote_due),
            market_event_generation=int(self.state.i),
            market_event_is_execution_trade=bool(self.state.is_execution_event),
            decision_bbo_index=int(self.state.decision_bbo_idx),
            decision_l2_index=int(self.state.decision_l2_idx),
            trade_visible_ts_ms=int(self.state.signal_visible_ts),
            feature_ready_index=int(self.state.var_idx),
            prediction_index=int(self.state.ml_idx),
            can_post_before_fill_cooldown=bool(
                self.state.bid_can_post_before_fill_cd
            ),
            can_post_after_fill_cooldown=bool(self.state.bid_can_post_after_fill_cd),
            pending_before=bool(self.state.bid_pending_order is not None),
            can_post=self.state.hb_new,
            can_post_after_inventory=self.state.bid_can_post_after_inventory,
            exposure_increasing=self.state.q >= 0.0,
            allow_exposure_increase=(self.state.hb_new or self.state.q < 0.0),
            needs_update=self.state.bid_updated,
            active_before=self.state.bid_active_before,
            action=self.state.bid_action,
            base_price=self.state.quote_context["BUY"].get("pre_guard_price", self.state.nbid),
            final_price=self.state.nbid,
            base_size=self.state.order_size,
            final_size=self.state.bid_sz,
            spread_mult=self.state.bid_policy_mult,
            size_mult=self.state.bid_sz / max(self.state.order_size, self.state.LOT_SIZE),
            side_ctx=self.state.quote_context["BUY"],
            quote_diag=self.state.diag,
            block_reasons=self.state.bid_block_reasons,
            route_allowed=self.state.bid_route_due,
        )
        self._append_decision_trace(
            "SELL",
            self.state.t,
            scheduled_requote=bool(self.state.scheduled_requote_due),
            market_event_generation=int(self.state.i),
            market_event_is_execution_trade=bool(self.state.is_execution_event),
            decision_bbo_index=int(self.state.decision_bbo_idx),
            decision_l2_index=int(self.state.decision_l2_idx),
            trade_visible_ts_ms=int(self.state.signal_visible_ts),
            feature_ready_index=int(self.state.var_idx),
            prediction_index=int(self.state.ml_idx),
            can_post_before_fill_cooldown=bool(
                self.state.ask_can_post_before_fill_cd
            ),
            can_post_after_fill_cooldown=bool(self.state.ask_can_post_after_fill_cd),
            pending_before=bool(self.state.ask_pending_order is not None),
            can_post=self.state.ha_new,
            can_post_after_inventory=self.state.ask_can_post_after_inventory,
            exposure_increasing=self.state.q <= 0.0,
            allow_exposure_increase=(self.state.ha_new or self.state.q > 0.0),
            needs_update=self.state.ask_updated,
            active_before=self.state.ask_active_before,
            action=self.state.ask_action,
            base_price=self.state.quote_context["SELL"].get("pre_guard_price", self.state.nask),
            final_price=self.state.nask,
            base_size=self.state.order_size,
            final_size=self.state.ask_sz,
            spread_mult=self.state.ask_policy_mult,
            size_mult=self.state.ask_sz / max(self.state.order_size, self.state.LOT_SIZE),
            side_ctx=self.state.quote_context["SELL"],
            quote_diag=self.state.diag,
            block_reasons=self.state.ask_block_reasons,
            route_allowed=self.state.ask_route_due,
        )
        if self.state.bid_route_due and "BUY" not in self.state.risk_selection_wait_sides:
            self._maybe_submit_safe_add_rearm_probe(
                "BUY",
                int(self.state.t),
                final_price=float(self.state.nbid),
                final_size=float(self.state.bid_sz),
                side_ctx=self.state.quote_context["BUY"],
                block_reasons=self.state.bid_block_reasons,
                can_post_after_inventory=bool(
                    self.state.bid_can_post_after_inventory
                ),
                exposure_increasing=bool(self.state.q >= 0.0),
                side_orders=self.state.bid_orders,
            )
        if self.state.ask_route_due and "SELL" not in self.state.risk_selection_wait_sides:
            self._maybe_submit_safe_add_rearm_probe(
                "SELL",
                int(self.state.t),
                final_price=float(self.state.nask),
                final_size=float(self.state.ask_sz),
                side_ctx=self.state.quote_context["SELL"],
                block_reasons=self.state.ask_block_reasons,
                can_post_after_inventory=bool(
                    self.state.ask_can_post_after_inventory
                ),
                exposure_increasing=bool(self.state.q <= 0.0),
                side_orders=self.state.ask_orders,
            )
