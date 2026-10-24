"""Replay restoration owner; venue/input adaptation stays explicit."""

from __future__ import annotations


class OrderRestoration:
    def _emit_markout_observation(self, kind, t_ms, side, payload):
        token = self.state.l2_production.observe(kind, t_ms, side)
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit(kind, t_ms, side=side,
                payload=payload, production_event=token)

    def _queue_markout(self, t_ms, price, is_bid, flags, quantity, order_id):
        self.state.mo_observation_sequence += 1
        flags = {**flags, "markout_observation_id": self.state.mo_observation_sequence,
                 "markout_order_id": order_id}
        self.state.mo_pending.append((t_ms, price, is_bid, flags, quantity))
        self._emit_markout_observation("markout_enqueued", t_ms, "BUY" if is_bid else "SELL", {
            "observation_id": flags["markout_observation_id"], "order_id": order_id,
            "fill_ts_ms": int(t_ms), "fill_price": float(price), "quantity": float(quantity),
            "fill_clock": "modeled_exchange_match_ms",
            "target_ts_ms": int(t_ms + self.state.markout_horizon_ms),
        })

    def _observe_markout_pause(self, t_ms, reason):
        for side, ema, until in (
            ("BUY", self.state.mo_ema_bid, self.state.mo_pause_until_bid_ms),
            ("SELL", self.state.mo_ema_ask, self.state.mo_pause_until_ask_ms),
        ):
            snapshot = {"ema": float(ema), "until": int(until),
                "threshold": self.state.adverse_markout_pause_threshold_eff,
                "active": bool(self.state.adverse_markout_pause_hybrid
                    and ema < -self.state.adverse_markout_pause_threshold_eff and t_ms < until)}
            previous = self.state.mo_observed_pause.get(side)
            transition = self.services.pause_transition(previous, snapshot)
            if transition:
                self._emit_markout_observation("markout_pause_state", t_ms, side, {
                    "transition": transition, "reason": reason,
                    "previous": previous, "current": snapshot,
                    "scope": "latch_state_not_final_action_or_independent_veto",
                })
            self.state.mo_observed_pause[side] = snapshot

    def _observe_markout_item(self, item, t_ms, status, *, mid=None, ema_before=None):
        ft, fpx, is_bid = item[:3]
        flags = item[3] if len(item) > 3 else {}
        self._emit_markout_observation("markout_observation", t_ms, "BUY" if is_bid else "SELL", {
            "observation_id": flags.get("markout_observation_id"),
            "order_id": flags.get("markout_order_id"),
            "fill_ts_ms": int(ft), "fill_price": float(fpx),
            "quantity": float(item[4]) if len(item) > 4 else None,
            "target_ts_ms": int(ft + self.state.markout_horizon_ms),
            "process_ts_ms": int(t_ms), "fill_age_ms": int(t_ms - ft),
            "status": status, "observation_price": mid,
            "signed_value": (mid - fpx if is_bid else fpx - mid) if mid is not None else None,
            "ema_before": ema_before,
            "ema_before_scope": "after_original_decay_before_this_resolve" if ema_before is not None else None,
            "ema_after": {"side": self.state.mo_ema_bid if is_bid else self.state.mo_ema_ask,
                          "all": self.state.mo_ema_all},
            "price_provenance": dict(self.state.mo_observation_price) if mid is not None else None,
            "value_unit": "quote_currency_per_base_unit",
        })

    def _decay_markout_ema(self, t_ms: int) -> None:
        if self.state.adverse_markout_decay_tau_s <= 0.0 or self.state.mo_last_decay_ts <= 0 or t_ms <= self.state.mo_last_decay_ts:
            self.state.mo_last_decay_ts = int(t_ms)
            self._observe_markout_pause(t_ms, "original_decay_evaluation")
            return
        dt_s = max(0.0, float(t_ms - self.state.mo_last_decay_ts) / 1000.0)
        decay = self.services.math.exp(-dt_s / self.state.adverse_markout_decay_tau_s)
        self.state.mo_ema_bid *= decay
        self.state.mo_ema_ask *= decay
        self.state.mo_ema_all *= decay
        self.state.mo_last_decay_ts = int(t_ms)
        self._observe_markout_pause(t_ms, "original_decay_evaluation")

    def _extend_markout_pause_until(self, is_bid: bool, t_ms: int) -> None:
        if not self.state.adverse_markout_pause_hybrid or not self.state.adverse_pause:
            return
        ema = self.state.mo_ema_bid if is_bid else self.state.mo_ema_ask
        if ema >= -self.state.adverse_markout_pause_threshold_eff:
            return
        ttl_s = self.state.adverse_markout_pause_base_s * abs(ema) / self.state.adverse_markout_pause_threshold_eff
        ttl_s = max(self.state.adverse_markout_pause_min_s, min(self.state.adverse_markout_pause_max_s, ttl_s))
        until_ms = int(t_ms + ttl_s * 1000.0)
        if is_bid:
            if until_ms > self.state.mo_pause_until_bid_ms:
                self.state.mo_pause_until_bid_ms = until_ms
                self.state.mo_pause_extend_bid_count += 1
        else:
            if until_ms > self.state.mo_pause_until_ask_ms:
                self.state.mo_pause_until_ask_ms = until_ms
                self.state.mo_pause_extend_ask_count += 1

    def _markout_pause_latch_active(self, is_bid: bool, t_ms: int) -> bool:
        if not self.state.adverse_markout_pause_hybrid:
            return False
        ema = self.state.mo_ema_bid if is_bid else self.state.mo_ema_ask
        until_ms = self.state.mo_pause_until_bid_ms if is_bid else self.state.mo_pause_until_ask_ms
        return ema < -self.state.adverse_markout_pause_threshold_eff and t_ms < until_ms

    def _restore_initial_live_orders(self) -> None:
        if not self.state.initial_live_state_enabled or self.state.n_trades <= 0:
            return
        start_ts = int(self.state.trade_ts[0])
        for raw_order in self.state.initial_live_state.get("active_orders", []) or []:
            if not isinstance(raw_order, dict):
                continue
            side = str(raw_order.get("side", "")).upper()
            if side not in ("BUY", "SELL"):
                continue
            try:
                price = float(raw_order.get("price", 0.0) or 0.0)
                quantity = float(raw_order.get("quantity", self.state.order_size) or self.state.order_size)
                remaining = float(raw_order.get("remaining", quantity) or quantity)
            except Exception:
                continue
            if price <= 0.0 or remaining < self.state.LOT_SIZE:
                continue
            submit_ts = int(raw_order.get("submit_ts_ms", start_ts) or start_ts)
            event_ts = int(raw_order.get("event_ts_ms", start_ts) or start_ts)
            quote_mid = float(raw_order.get("mid_at_quote", self.state.mid) or self.state.mid)
            restored = self._make_order(
                side, price, max(quantity, remaining), submit_ts, quote_mid,
                apply_decision_compute=False,
                restored_order=True,
            )
            restored["remaining"] = min(max(remaining, 0.0), max(quantity, remaining))
            restored["exchange_remaining"] = float(restored["remaining"])
            restored["restore_quote_ts"] = submit_ts
            restored["activate_ts"] = int(raw_order.get("activate_ts_ms", event_ts))
            restored["new_ack_ts"] = int(raw_order.get("new_ack_ts_ms", event_ts))
            restored["exchange_accepted"] = True
            restored["local_new_ack_published"] = True
            restored["quote_ts"] = submit_ts
            restored["mid_at_quote"] = quote_mid
            restored["inventory_at_submit"] = float(raw_order.get("inventory_at_submit", self.state.q) or self.state.q)
            status = str(raw_order.get("status", "OPEN")).upper()
            if status not in ("PENDING_NEW", "SUBMITTING"):
                if not (submit_ts <= restored["activate_ts"] <= restored["new_ack_ts"] <= start_ts):
                    raise ValueError("restored acknowledged order clocks are inconsistent")
                # An existing order must not re-enter post-only admission or
                # receive a native new-order queue seed at the window edge.
                queue_left = raw_order.get("queue_left")
                if queue_left is None:
                    queue_left = self._estimate_queue_ahead(
                        side, price, abs(float(quote_mid) - price), target_ts=start_ts,
                    )
                    self.state.initial_live_state_queue_estimated_count += 1
                    queue_status = "unknown_history_boundary_estimate"
                else:
                    queue_status = "supplied_diagnostic_estimate"
                queue_left = float(queue_left)
                if not self.services.math.isfinite(queue_left) or queue_left < 0.0:
                    raise ValueError("restored queue_left must be finite and non-negative")
                restored.update(
                    queue_left=queue_left, queue_init=queue_left,
                    policy_queue_left=queue_left, policy_queue_init=queue_left,
                    restored_queue_status=queue_status,
                    simulator_queue_source="restored_unknown_queue_estimate",
                    exact_queue_path_valid=False, exchange_book_queue_path_valid=False,
                    exchange_book_queue_invalidated_reason="inherited_queue_history_unavailable",
                )
            if status in ("PENDING_CANCEL", "CANCELING"):
                restored["state"] = self.state.ORDER_PENDING_CANCEL
                restored["cancel_ts"] = int(raw_order.get("cancel_ts_ms", start_ts) or start_ts)
                restored["cancel_effective_ts"] = int(
                    raw_order.get(
                        "cancel_effective_ts_ms",
                        restored["cancel_ts"],
                    )
                    or restored["cancel_ts"]
                )
                restored["cancel_request_ts"] = int(raw_order.get("cancel_request_ts_ms", event_ts) or event_ts)
                restored["cancel_reason"] = str(raw_order.get("cancel_reason", "bootstrap_pending_cancel") or "bootstrap_pending_cancel")
            elif status in ("PENDING_NEW", "SUBMITTING"):
                restored["state"] = self.state.ORDER_PENDING_NEW
                restored["exchange_accepted"] = False
                restored["local_new_ack_published"] = False
            else:
                restored["state"] = self.state.ORDER_OPEN
            if side == "BUY":
                self.state.bid_orders.append(restored)
                self.state.initial_live_state_bid_orders_restored += 1
            else:
                self.state.ask_orders.append(restored)
                self.state.initial_live_state_ask_orders_restored += 1
            self.state.initial_live_state_orders_restored += 1
        expected_orders = self.state.initial_live_state.get("health_orders")
        if expected_orders is not None:
            try:
                expected = int(expected_orders)
                if expected != self.state.initial_live_state_orders_restored:
                    self.state.initial_live_state_warning = (
                        f"health_orders={expected} restored_orders={self.state.initial_live_state_orders_restored}"
                    )
            except Exception:
                pass
