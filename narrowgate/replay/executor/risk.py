"""Replay risk owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any


class RiskAdapter:
    def _sync_local_order_lifecycle_repair(self, now_ts: int) -> None:
        if not self.state.local_order_lifecycle.records_dynamic_state:
            return
        self.state.local_order_lifecycle.sync_repair_state(
            int(now_ts),
            inventory_lifecycle_id=int(self.state.inventory_lifecycle_count),
            inventory_lifecycle_active=bool(self.state.inventory_lifecycle_active),
            inventory=float(self.state.q),
            active_orders=(*self.state.bid_orders, *self.state.ask_orders),
        )

    def _record_local_order_lifecycle_risk_snapshots(self,
        now_ts: int,
        visible_book: dict[str, Any],
    ) -> None:
        """Record dynamic policy-visible order states without trade payloads."""

        if not self.state.local_order_lifecycle.records_dynamic_state:
            return
        best_bid = float(visible_book.get("best_bid", 0.0) or 0.0)
        best_ask = float(visible_book.get("best_ask", 0.0) or 0.0)
        if best_bid <= 0.0 or best_ask <= best_bid:
            return
        bid_size = max(0.0, float(visible_book.get("bid_qty", 0.0) or 0.0))
        ask_size = max(0.0, float(visible_book.get("ask_qty", 0.0) or 0.0))
        size_sum = bid_size + ask_size
        mid_now = 0.5 * (best_bid + best_ask)
        weighted_mid_proxy = (
            (best_ask * bid_size + best_bid * ask_size) / size_sum
            if size_sum > 1e-12
            else mid_now
        )
        imbalance = (
            (bid_size - ask_size) / size_sum
            if size_sum > 1e-12
            else 0.0
        )
        feature_source_ms = max(
            int(visible_book.get("visible_ts", now_ts) or now_ts),
            int(visible_book.get("visible_l2_ts", now_ts) or now_ts),
        )
        if feature_source_ms > int(now_ts):
            return
        feature_source_ts_ns = feature_source_ms * 1_000_000
        feature_ready_ts_ns = int(now_ts) * 1_000_000
        clock_seconds = (int(now_ts) // 1_000) % 86_400
        clock_angle = 2.0 * self.services.math.pi * float(clock_seconds) / 86_400.0
        for side, orders, top_size in (
            ("BUY", self.state.bid_orders, bid_size),
            ("SELL", self.state.ask_orders, ask_size),
        ):
            for order in orders:
                if order.get("state") != self.state.ORDER_OPEN:
                    continue
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
                cancel_count = max(
                    0.0,
                    float(order.get("policy_visible_cancel_count", 0.0) or 0.0),
                )
                refill_count = max(
                    0.0,
                    float(order.get("policy_visible_refill_count", 0.0) or 0.0),
                )
                path_event_count = cancel_count + refill_count
                current_role = self.services.inventory_role_for_quote(
                    side,
                    float(self.state.q),
                    float(self.state.order_size),
                )
                current_inventory_lifecycle_id = (
                    int(self.state.inventory_lifecycle_count)
                    if self.state.inventory_lifecycle_active
                    else int(order.get("inventory_lifecycle_id_at_submit", 0) or 0)
                )
                quote_distance_ticks = (
                    max(0.0, mid_now - float(order["price"])) / float(self.state.TICK)
                    if side == "BUY"
                    else max(0.0, float(order["price"]) - mid_now) / float(self.state.TICK)
                )
                self.state.local_order_lifecycle.risk_snapshot(
                    order,
                    int(now_ts),
                    feature_source_ts_ns=int(feature_source_ts_ns),
                    feature_ready_ts_ns=int(feature_ready_ts_ns),
                    inventory_role=str(current_role),
                    inventory=float(self.state.q),
                    inventory_lifecycle_id=int(current_inventory_lifecycle_id),
                    mid=float(mid_now),
                    weighted_mid_proxy=float(weighted_mid_proxy),
                    top_size=float(top_size),
                    features={
                        "exact_queue_path_valid": int(
                            bool(
                                order.get(
                                    "exchange_book_queue_path_valid",
                                    False,
                                )
                            )
                        ),
                        "queue_path_ambiguous": int(
                            bool(order.get("exchange_book_queue_ambiguous", False))
                        ),
                        "visible_state_age_ms": max(
                            0.0,
                            float(int(now_ts) - feature_source_ms),
                        ),
                        "spread_ticks": float(
                            (best_ask - best_bid) / float(self.state.TICK)
                        ),
                        "quote_distance_ticks": float(quote_distance_ticks),
                        "best_bid": float(best_bid),
                        "best_ask": float(best_ask),
                        "top_bid_size": float(bid_size),
                        "top_ask_size": float(ask_size),
                        "book_imbalance": float(imbalance),
                        "microprice_shift_ticks": float(
                            (weighted_mid_proxy - mid_now) / float(self.state.TICK)
                        ),
                        "side_microprice_adverse_ticks": float(
                            (mid_now - weighted_mid_proxy) / float(self.state.TICK)
                            if side == "BUY"
                            else (weighted_mid_proxy - mid_now) / float(self.state.TICK)
                        ),
                        "policy_queue_initial": float(queue_init),
                        "policy_queue_remaining": float(queue_left),
                        "policy_queue_fraction_left": float(queue_fraction_left),
                        "policy_queue_progress": float(
                            max(0.0, 1.0 - queue_fraction_left)
                        ),
                        "visible_cancel_events": float(cancel_count),
                        "visible_cancel_size": float(
                            order.get("policy_visible_cancel_size", 0.0) or 0.0
                        ),
                        "visible_refill_events": float(refill_count),
                        "visible_refill_size": float(
                            order.get("policy_visible_refill_size", 0.0) or 0.0
                        ),
                        "visible_refill_event_share": float(
                            refill_count / path_event_count
                            if path_event_count > 0.0
                            else 0.5
                        ),
                        "clock_hour_sin": float(self.services.math.sin(clock_angle)),
                        "clock_hour_cos": float(self.services.math.cos(clock_angle)),
                    },
                )

    def _post_cooldown_budget_side_orders(self, side: str) -> list[dict[str, Any]]:
        return self.state.bid_orders if str(side).upper() == "BUY" else self.state.ask_orders

    def _post_cooldown_budget_assert(self, episode: dict[str, Any]) -> None:
        if not self.state.post_cooldown_inventory_budget_enabled:
            return
        self.state.post_cooldown_inventory_budget_conservation_checks += 1
        try:
            episode["budget"].assert_conservation()
        except Exception:
            self.state.post_cooldown_inventory_budget_conservation_failures += 1
            raise

    def _post_cooldown_budget_release_order(self,
        order: dict[str, Any],
        *,
        now_ts: int,
        reason: str,
    ) -> float:
        raw_episode_id = order.pop(
            "post_cooldown_inventory_budget_episode_id",
            None,
        )
        episode_id = (
            int(raw_episode_id) if raw_episode_id is not None else -1
        )
        order.pop("post_cooldown_inventory_budget_reserved_units", None)
        if episode_id < 0:
            return 0.0
        episode = self.state.post_cooldown_inventory_budget_by_id.get(episode_id)
        if episode is None:
            raise RuntimeError(
                "budgeted replay order references an unknown inventory-budget episode"
            )
        released = float(episode["budget"].release(order["trace_id"]))
        record = episode["record"]
        record["released_units"] = float(record["released_units"] + released)
        record["reservation_release_count"] = int(
            record["reservation_release_count"] + int(released > 0.0)
        )
        if released > 0.0:
            reasons = record.setdefault("reservation_release_reasons", {})
            reasons[str(reason)] = int(reasons.get(str(reason), 0) + 1)
            record["last_reservation_release_ts_ms"] = int(now_ts)
        self._post_cooldown_budget_assert(episode)
        return released

    def _post_cooldown_budget_finalize(self,
        side: str,
        *,
        now_ts: int,
        reason: str,
        censored: bool,
        release_live_orders: bool = True,
    ) -> None:
        normalized_side = str(side).upper()
        self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = None
        episode = self.state.post_cooldown_inventory_budget_active_by_side.pop(
            normalized_side,
            None,
        )
        if episode is None:
            return
        if bool(episode.get("finalized", False)):
            raise RuntimeError("inventory-budget episode finalized more than once")
        budget = episode["budget"]
        record = episode["record"]
        record["reserved_units_before_terminal"] = float(budget.reserved_units)
        if release_live_orders:
            for order in self._post_cooldown_budget_side_orders(normalized_side):
                raw_order_episode_id = order.get(
                    "post_cooldown_inventory_budget_episode_id"
                )
                if (
                    int(raw_order_episode_id)
                    if raw_order_episode_id is not None
                    else -1
                ) == int(episode["episode_id"]):
                    self._post_cooldown_budget_release_order(
                        order,
                        now_ts=int(now_ts),
                        reason=f"episode_terminal:{reason}",
                    )
        self._post_cooldown_budget_assert(episode)
        snapshot = budget.snapshot()
        record.update(
            {
                "terminal_ts_ms": int(now_ts),
                "terminal_reason": str(reason),
                "censored": int(bool(censored)),
                "terminal_inventory_btc": float(self.state.q),
                "terminal_inventory_units": float(
                    abs(self.state.q) / max(float(budget.unit_size_btc), 1e-12)
                ),
                "consumed_units": float(snapshot["consumed_units"]),
                "reserved_units": float(snapshot["reserved_units"]),
                "available_units": float(snapshot["available_units"]),
                "reserved_order_count": int(snapshot["reserved_order_count"]),
                "budget_hit": int(record["blocked_submission_count"] > 0),
                "final_quote_action_changed": int(
                    record["blocked_submission_count"] > 0
                ),
                "supported": 1,
            }
        )
        episode["finalized"] = True
        if self.state.trace_post_cooldown_inventory_budget is None:
            raise RuntimeError("inventory-budget trace is unexpectedly disabled")
        if (
            len(self.state.trace_post_cooldown_inventory_budget)
            >= self.state.trace_post_cooldown_inventory_budget_max
        ):
            raise RuntimeError("post-cooldown inventory-budget trace limit exceeded")
        self.state.trace_post_cooldown_inventory_budget.append(dict(record))

    def _post_cooldown_budget_note_release_due(self,
        side: str,
        *,
        ready_ts_ms: int,
        observed_ts_ms: int,
        last_fill_ts_ms: int,
        consecutive_units: float,
    ) -> None:
        if not self._post_cooldown_budget_enabled_for_side(side):
            return
        normalized_side = str(side).upper()
        if normalized_side in self.state.post_cooldown_inventory_budget_active_by_side:
            return
        self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = {
            "ready_ts_ms": int(ready_ts_ms),
            "observed_ts_ms": int(observed_ts_ms),
            "last_fill_ts_ms": int(last_fill_ts_ms),
            "consecutive_same_side_fill_units": float(consecutive_units),
            "masked_decision_count": 0,
            "masked_reason_counts": {},
        }

    def _post_cooldown_budget_observe_release_surface(self,
        side: str,
        *,
        now_ts: int,
        can_post: bool,
        side_orders: list[dict[str, Any]],
        block_reasons: list[str],
    ) -> None:
        if not self._post_cooldown_budget_enabled_for_side(side):
            return
        normalized_side = str(side).upper()
        pending = self.state.post_cooldown_inventory_budget_pending_release.get(
            normalized_side
        )
        if pending is None or normalized_side in self.state.post_cooldown_inventory_budget_active_by_side:
            return
        exposure_increasing = (
            normalized_side == "BUY" and self.state.q > 1e-12
        ) or (
            normalized_side == "SELL" and self.state.q < -1e-12
        )
        if not self.state.inventory_lifecycle_active or abs(self.state.q) < self.state.LOT_SIZE * 0.5:
            self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = None
            return
        if not exposure_increasing or not can_post:
            pending["masked_decision_count"] = int(
                pending["masked_decision_count"] + 1
            )
            reason_counts = pending["masked_reason_counts"]
            for reason in block_reasons or ("not_baseline_eligible",):
                token = str(reason or "not_baseline_eligible")
                reason_counts[token] = int(reason_counts.get(token, 0) + 1)
            return

        episode_id = int(self.state.post_cooldown_inventory_budget_next_id)
        self.state.post_cooldown_inventory_budget_next_id += 1
        unit_size_btc = max(float(self.state.order_size), float(self.state.LOT_SIZE))
        record = {
            "schema_version": "post_cooldown_incremental_inventory_budget_trace.v1",
            "episode_id": episode_id,
            "arm_id": self.state.post_cooldown_inventory_budget_arm_id,
            "side": normalized_side,
            "budget_units": float(self.state.post_cooldown_inventory_budget_units),
            "unit_size_btc": float(unit_size_btc),
            "assignment_ts_ms": int(now_ts),
            "scheduled_release_ts_ms": int(pending["ready_ts_ms"]),
            "release_observed_delay_ms": int(
                max(0, int(now_ts) - int(pending["ready_ts_ms"]))
            ),
            "trigger_fill_ts_ms": int(pending["last_fill_ts_ms"]),
            "assignment_consecutive_same_side_fill_units": float(
                pending["consecutive_same_side_fill_units"]
            ),
            "assignment_inventory_btc": float(self.state.q),
            "assignment_inventory_units": float(abs(self.state.q) / unit_size_btc),
            "inventory_lifecycle_id": int(self.state.inventory_lifecycle_count),
            "masked_decision_count_before_assignment": int(
                pending["masked_decision_count"]
            ),
            "masked_reason_counts_before_assignment": dict(
                pending["masked_reason_counts"]
            ),
            "admission_attempt_count": 0,
            "admitted_order_count": 0,
            "blocked_submission_count": 0,
            "blocked_planned_units": 0.0,
            "fill_count": 0,
            "filled_quantity_btc": 0.0,
            "released_units": 0.0,
            "reservation_release_count": 0,
            "max_reserved_units": 0.0,
            "max_consumed_units": 0.0,
            "max_abs_inventory_units": float(abs(self.state.q) / unit_size_btc),
            "reducing_order_budget_bypass_count": 0,
            "one_order_overshoot_count": 0,
            "assignment_preexisting_exposure_order_count": 0,
        }
        preexisting = [
            order
            for order in side_orders
            if float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE
            and order.get("state") in {
                self.state.ORDER_PENDING_NEW,
                self.state.ORDER_OPEN,
                self.state.ORDER_PENDING_CANCEL,
            }
        ]
        if preexisting:
            record.update(
                {
                    "assignment_preexisting_exposure_order_count": len(preexisting),
                    "terminal_ts_ms": int(now_ts),
                    "terminal_reason": "preexisting_exposure_order_unsupported",
                    "censored": 1,
                    "supported": 0,
                    "budget_hit": 0,
                    "final_quote_action_changed": 0,
                }
            )
            self.state.post_cooldown_inventory_budget_unsupported_count += 1
            self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = None
            if self.state.trace_post_cooldown_inventory_budget is None:
                raise RuntimeError("inventory-budget trace is unexpectedly disabled")
            if (
                len(self.state.trace_post_cooldown_inventory_budget)
                >= self.state.trace_post_cooldown_inventory_budget_max
            ):
                raise RuntimeError("post-cooldown inventory-budget trace limit exceeded")
            self.state.trace_post_cooldown_inventory_budget.append(record)
            return

        if self.context.PostCooldownIncrementalInventoryBudget is None:
            raise RuntimeError("inventory-budget state machine was not loaded")
        budget = self.context.PostCooldownIncrementalInventoryBudget(
            normalized_side,
            self.state.post_cooldown_inventory_budget_units,
            unit_size_btc,
        )
        episode = {
            "episode_id": episode_id,
            "side": normalized_side,
            "budget": budget,
            "record": record,
            "finalized": False,
        }
        self.state.post_cooldown_inventory_budget_by_id[episode_id] = episode
        self.state.post_cooldown_inventory_budget_active_by_side[normalized_side] = episode
        self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = None
        self._post_cooldown_budget_assert(episode)

    def _post_cooldown_budget_prepare_submission(self,
        side: str,
        *,
        quantity_btc: float,
        exposure_increasing: bool,
        side_ctx: dict[str, Any],
        reserve: bool = True,
    ) -> tuple[bool, str]:
        if not self.state.post_cooldown_inventory_budget_enabled or not exposure_increasing:
            if self.state.post_cooldown_inventory_budget_enabled and not exposure_increasing:
                episode = self.state.post_cooldown_inventory_budget_active_by_side.get(
                    str(side).upper()
                )
                if episode is not None:
                    episode["record"]["reducing_order_budget_bypass_count"] = int(
                        episode["record"]["reducing_order_budget_bypass_count"] + 1
                    )
            return True, "reducing_or_disabled"
        normalized_side = str(side).upper()
        episode = self.state.post_cooldown_inventory_budget_active_by_side.get(normalized_side)
        if episode is None:
            return True, "no_active_budget_episode"
        if not reserve:
            # WAIT still needs the baseline's admission answer, but must not
            # reserve inventory, consume a token, or count a submitted order.
            probe = self.services.replace(
                episode["budget"],
                _reserved_by_order=dict(episode["budget"]._reserved_by_order),
            ).reserve("risk-selection-probe", float(quantity_btc), exposure_increasing=True)
            return bool(probe.allowed), str(probe.reason)
        preparation_token = (
            f"prepared:{normalized_side}:"
            f"{self.state.post_cooldown_inventory_budget_next_preparation_id}"
        )
        self.state.post_cooldown_inventory_budget_next_preparation_id += 1
        record = episode["record"]
        record["admission_attempt_count"] = int(record["admission_attempt_count"] + 1)
        decision = episode["budget"].reserve(
            preparation_token,
            float(quantity_btc),
            exposure_increasing=True,
        )
        self._post_cooldown_budget_assert(episode)
        side_ctx["post_cooldown_inventory_budget_episode_id"] = int(
            episode["episode_id"]
        )
        side_ctx["post_cooldown_inventory_budget_available_units_before"] = float(
            decision.available_units_before
        )
        side_ctx["post_cooldown_inventory_budget_planned_units"] = float(
            decision.planned_units
        )
        if not decision.allowed:
            record["blocked_submission_count"] = int(
                record["blocked_submission_count"] + 1
            )
            record["blocked_planned_units"] = float(
                record["blocked_planned_units"] + decision.planned_units
            )
            self.state.post_cooldown_inventory_budget_block_count += 1
            return False, str(decision.reason)
        self.state.post_cooldown_inventory_budget_prepared_orders[preparation_token] = int(
            episode["episode_id"]
        )
        side_ctx["post_cooldown_inventory_budget_preparation_token"] = str(
            preparation_token
        )
        record["admitted_order_count"] = int(record["admitted_order_count"] + 1)
        record["max_reserved_units"] = max(
            float(record["max_reserved_units"]),
            float(episode["budget"].reserved_units),
        )
        return True, str(decision.reason)

    def _post_cooldown_budget_attach_prepared_order(self,
        order: dict[str, Any],
        *,
        exposure_increasing: bool,
    ) -> None:
        if not self.state.post_cooldown_inventory_budget_enabled:
            return
        trace_id = int(order["trace_id"])
        preparation_token = str(
            order.pop(
                "post_cooldown_inventory_budget_preparation_token",
                "",
            )
            or ""
        )
        episode_id = self.state.post_cooldown_inventory_budget_prepared_orders.pop(
            preparation_token,
            None,
        )
        active = self.state.post_cooldown_inventory_budget_active_by_side.get(
            str(order.get("side", "")).upper()
        )
        if episode_id is None:
            if exposure_increasing and active is not None:
                raise RuntimeError(
                    "exposure-increasing order bypassed incremental inventory "
                    "budget admission"
                )
            return
        if active is None or int(active["episode_id"]) != int(episode_id):
            raise RuntimeError("prepared inventory-budget admission lost its episode")
        reserved = float(
            active["budget"].rename_reservation(
                preparation_token,
                trace_id,
            )
        )
        if reserved <= 0.0:
            raise RuntimeError("prepared inventory-budget order has no reservation")
        order["post_cooldown_inventory_budget_episode_id"] = int(episode_id)
        order["post_cooldown_inventory_budget_reserved_units"] = reserved
        self._post_cooldown_budget_assert(active)

    def _post_cooldown_budget_on_fill(self,
        order: dict[str, Any],
        *,
        side: str,
        fill_qty_btc: float,
        now_ts: int,
    ) -> None:
        if not self.state.post_cooldown_inventory_budget_enabled:
            return
        normalized_side = str(side).upper()
        raw_episode_id = order.get(
            "post_cooldown_inventory_budget_episode_id"
        )
        episode_id = (
            int(raw_episode_id) if raw_episode_id is not None else -1
        )
        if episode_id >= 0:
            episode = self.state.post_cooldown_inventory_budget_by_id.get(episode_id)
            if episode is None:
                raise RuntimeError("budgeted fill references an unknown episode")
            episode["budget"].fill(
                order["trace_id"],
                float(fill_qty_btc),
                exposure_increasing=True,
            )
            record = episode["record"]
            record["fill_count"] = int(record["fill_count"] + 1)
            record["filled_quantity_btc"] = float(
                record["filled_quantity_btc"] + float(fill_qty_btc)
            )
            record["max_consumed_units"] = max(
                float(record["max_consumed_units"]),
                float(episode["budget"].consumed_units),
            )
            record["max_reserved_units"] = max(
                float(record["max_reserved_units"]),
                float(episode["budget"].reserved_units),
            )
            record["max_abs_inventory_units"] = max(
                float(record["max_abs_inventory_units"]),
                float(abs(self.state.q) / max(episode["budget"].unit_size_btc, 1e-12)),
            )
            order["post_cooldown_inventory_budget_reserved_units"] = float(
                episode["budget"].reservation_units(order["trace_id"])
            )
            if order["post_cooldown_inventory_budget_reserved_units"] <= 1e-10:
                order.pop("post_cooldown_inventory_budget_episode_id", None)
                order.pop("post_cooldown_inventory_budget_reserved_units", None)
            self._post_cooldown_budget_assert(episode)

        self.state.post_cooldown_inventory_budget_pending_release[normalized_side] = None
        opposite = "SELL" if normalized_side == "BUY" else "BUY"
        self._post_cooldown_budget_finalize(
            opposite,
            now_ts=int(now_ts),
            reason="opposite_side_fill",
            censored=False,
        )

    def _select_quote_risk_actions(self):
        """Select actual wait/cancel/place/replace actions under the original risk set."""
        self.state.risk_selection_wait_sides = set()
        self.state.risk_selection_cancel_sides = set()
        if self.state.risk_selection is not None:
            self.state.bid_action = self.services._baseline_quote_action(
                route_due=self.state.bid_route_due, forced_cancel=self.state.ranked_guard_bid_cancel_order_id,
                cancel_first=self.state.bid_cancel_first, pending_coalesce=self.state.bid_pending_coalesce,
                enabled=self.state.hb_new, updated=self.state.bid_updated, active=self.state.bid_active_before,
            )
            self.state.ask_action = self.services._baseline_quote_action(
                route_due=self.state.ask_route_due, forced_cancel=self.state.ranked_guard_ask_cancel_order_id,
                cancel_first=self.state.ask_cancel_first, pending_coalesce=self.state.ask_pending_coalesce,
                enabled=self.state.ha_new, updated=self.state.ask_updated, active=self.state.ask_active_before,
            )
            self.state.risk_selection_rows = []
            self.state.risk_selection_pending = tuple(
                self.services.PendingExposure(self._ranked_guard_order_id(order), str(order["side"]),
                                float(order["remaining"]))
                for order in (*self.state.bid_orders, *self.state.ask_orders)
                if float(order["remaining"]) > 0.0
            )
            for loop_item in (
                ("BUY", self.state.bid_action, self.state.bid_orders, self.state.bid_ref_order, self.state.nbid, self.state.bid_sz,
                 self.state.quote_context["BUY"]),
                ("SELL", self.state.ask_action, self.state.ask_orders, self.state.ask_ref_order, self.state.nask, self.state.ask_sz,
                 self.state.quote_context["SELL"]),
            ):
                self.state.side, self.state.action, self.state.orders, self.state.ref_order, self.state.price, self.state.quantity, self.state.side_ctx = loop_item
                self.state.risk_selection.record_route(
                    self.state.side, self.state.action, "considered", "all",
                )
                self.state.kind = ""
                self.state.target_order_id = ""
                if self.state.action == "place" and not self.state.orders and abs(float(self.state.q)) <= 1e-10:
                    self.state.kind = "E"
                elif (self.state.action == "keep" and len(self.state.orders) == 1 and self.state.ref_order is not None
                      and self.state.ref_order["state"] == self.state.ORDER_OPEN
                      and float(self.state.ref_order.get("cancel_ts", 0.0)) <= 0.0
                      and float(self.state.ref_order["remaining"]) > 0.0):
                    self.state.kind = "C"
                    self.state.target_order_id = self._ranked_guard_order_id(self.state.ref_order)
                    self.state.price = float(self.state.ref_order["price"])
                    self.state.quantity = float(self.state.ref_order["remaining"])
                if not self.state.kind:
                    self.state.risk_selection.record_route(
                        self.state.side, self.state.action, "excluded",
                        "not_flat" if self.state.action == "place" and abs(float(self.state.q)) > 1e-10
                        else "not_due" if not (self.state.bid_route_due if self.state.side == "BUY" else self.state.ask_route_due)
                        else "ownership_not_eligible" if self.state.action in {"place", "keep"}
                        else "baseline_action_not_covered",
                    )
                    continue
                self.state.identity = self.services.opportunity_id(
                    self.services.SYMBOL, int(self.state.t), int(self.state.nrq), self.state.side, self.state.kind, self.state.target_order_id
                )
                self.state.role = self.services.candidate_role(
                    self.services.RiskSelectionObservation(int(self.state.t) * 1_000_000, int(self.state.t) * 1_000_000,
                                             float(self.state.q), self.state.risk_selection_pending,
                                             selection_scope=self.state.risk_selection.selection_scope),
                    self.services.RiskSelectionCandidate(self.state.identity, self.state.kind, self.state.side, float(self.state.quantity),
                                           "POST" if self.state.kind == "E" else "KEEP",
                                           order_id=self.state.target_order_id),
                )
                if (self.state.kind == "E" and self.state.role != "opener") or (
                    self.state.kind == "C" and self.state.role not in {"opener", "add", "opener_or_add"}
                ):
                    self.state.risk_selection.record_route(
                        self.state.side, self.state.action, "excluded", self.state.role,
                    )
                    continue
                if self.state.kind == "E":
                    # Establish baseline admission without reserving budget.
                    # Both sides are scored before either can mutate it.
                    self.state.budget_allowed, self.state._ = self._post_cooldown_budget_prepare_submission(
                        self.state.side, quantity_btc=float(self.state.quantity), exposure_increasing=True,
                        side_ctx=self.state.side_ctx, reserve=False,
                    )
                    if not self.state.budget_allowed:
                        self.state.risk_selection.record_route(
                            self.state.side, self.state.action, "excluded", "budget_blocked",
                        )
                        continue
                self.state.risk_selection.record_route(
                    self.state.side, self.state.action, "eligible", f"{self.state.kind}:{self.state.role}",
                )
                # Explicit policy-visible fields only. In particular never
                # expose exchange_inventory, exchange_remaining, queue_left,
                # native scheduler cursors, or future markout labels here.
                self.state.features = {
                    "mid": float(self.state.mid), "best_bid": float(self.state.cur_best_bid),
                    "best_ask": float(self.state.cur_best_ask), "inventory_btc": float(self.state.q),
                    "price": float(self.state.price), "quantity_btc": float(self.state.quantity),
                    "other_same_side_pending_btc": sum(
                        order.remaining_qty_btc for order in self.state.risk_selection_pending
                        if order.side == self.state.side and order.order_id != self.state.target_order_id
                    ),
                    "opposite_side_pending_btc": sum(
                        order.remaining_qty_btc for order in self.state.risk_selection_pending
                        if order.side != self.state.side
                    ),
                    "toxicity": float(self.state.cur_tox_bid if self.state.side == "BUY" else self.state.cur_tox_ask),
                    "markout_ema": float(self.state.mo_ema_bid if self.state.side == "BUY" else self.state.mo_ema_ask),
                    **{name: self.state.side_ctx[name] for name in (
                        "weighted_mid_proxy_shift_bps", "l2_quote_flip_rate",
                        "l2_book_refresh_ratio", "l2_book_cancel_ratio",
                        "l2_near_depth_total",
                    ) if name in self.state.side_ctx},
                    **{name: self.state.cur_prediction_features[name] for name in (
                        "volatility_5s", "volatility_30s", "volatility_60s",
                        "volume_imbalance_5s", "volume_imbalance_30s",
                        "trade_intensity_5s", "trade_intensity_30s",
                        "price_change_5s", "price_change_30s",
                    ) if name in self.state.cur_prediction_features},
                }
                self.state.features = self.services.visible_feature_snapshot(self.state.features)
                self.state.source_ready_ts_ns = {
                    feed: max(0, int(self._message_clock_state(feed, int(self.state.t))[2]))
                    for feed in self.state.exec_message_schedules
                }
                self.state.prediction_ready_ts_ns = (
                    int(self._message_clock_state("prediction", int(self.state.t))[2])
                    if "prediction" in self.state.exec_message_schedules else
                    int(self.state.ml_ts[self.state.ml_idx]) * 1_000_000 if 0 <= int(self.state.ml_idx) < len(self.state.ml_ts) else 0
                )
                self.state.feature_ready_ts_ns = self.services.feature_ready_time(
                    self.state.source_ready_ts_ns, self.state.prediction_ready_ts_ns, int(self.state.t) * 1_000_000,
                )
                if self.state.risk_selection_feature_contract == self.services.PREFILL_FEATURE_CONTRACT:
                    # Current E/C protocol uses only strategy-visible market
                    # and ownership state, not the historical post-fill inputs.
                    # The current FeatureFrame has no exact 5000ms flow/BBO
                    # return: these remain unknown, never replaced by 10s flow
                    # or a trade-close return. Freeze fitting support separately.
                    self.state.features = self.services.prefill_features(
                        kind=self.state.kind, side=self.state.side,
                        decision_ns=int(self.state.t)*1_000_000,
                        market_ready_ns=self.state.feature_ready_ts_ns,
                        tick_size=self.state.TICK, lot_size=self.state.LOT_SIZE,
                        best_bid=self.state.decision_book['best_bid'],
                        best_ask=self.state.decision_book['best_ask'],
                        bid_quantity=(self.state.decision_book['bid_qty']
                                      if self.state.decision_book['book_usable'] else None),
                        ask_quantity=(self.state.decision_book['ask_qty']
                                      if self.state.decision_book['book_usable'] else None),
                        order_price=self.state.price, visible_inventory=self.state.q,
                        other_same_side_pending=self.state.features['other_same_side_pending_btc'],
                        opposite_side_pending=self.state.features['opposite_side_pending_btc'],
                        absolute_price_variance_rate=(float(self.context.var_ssq[self.state.var_idx])
                            if 0 <= self.state.var_idx < len(self.context.var_ssq) else None),
                        local_submit_ns=(int(self.state.ref_order['submit_ts'])*1_000_000
                            if self.state.kind == 'C' and 'submit_ts' in self.state.ref_order else None),
                    )
                self.state.risk_selection_rows.append({
                    "opportunity_id": self.state.identity, "role": self.state.role,
                    "kind": self.state.kind, "side": self.state.side, "order_id": self.state.target_order_id,
                    "decision_ts_ns": int(self.state.t) * 1_000_000,
                    "feature_ready_ts_ns": self.state.feature_ready_ts_ns,
                    "decision_sequence": int(self.state.nrq),
                    "baseline_action": "POST" if self.state.kind == "E" else "KEEP",
                    "baseline_allowed": True, "quantity_btc": float(self.state.quantity),
                    "price": float(self.state.price), "inventory_btc": float(self.state.q),
                    "pending_orders": [
                        {"order_id": self._ranked_guard_order_id(order),
                         "side": str(order["side"]),
                         "remaining_qty_btc": float(order["remaining"])}
                        for order in (*self.state.bid_orders, *self.state.ask_orders)
                        if float(order["remaining"]) > 0.0
                    ],
                    "features": self.state.features,
                    **({'feature_contract': self.services.PREFILL_FEATURE_CONTRACT}
                       if self.state.risk_selection_feature_contract is not None else {}),
                    "visible_context": {
                        "decision_bbo_index": (int(self.state.decision_bbo_idx) + self.state.input_clock_offsets['bbo']
                            if self.state.decision_bbo_idx >= 0 else int(self.state.decision_bbo_idx)),
                        "decision_l2_index": (int(self.state.decision_l2_idx) + self.state.input_clock_offsets['l2']
                            if self.state.decision_l2_idx >= 0 else int(self.state.decision_l2_idx)),
                        "trade_visible_ts_ms": int(self.state.signal_visible_ts),
                        "feature_ready_index": (int(self.state.var_idx) + self.state.input_clock_offsets['variance']
                            if self.state.var_idx >= 0 else int(self.state.var_idx)),
                        "prediction_index": (int(self.state.ml_idx) + self.state.input_clock_offsets['prediction']
                            if self.state.ml_idx >= 0 else int(self.state.ml_idx)),
                        "prediction_ready_ts_ns": self.state.prediction_ready_ts_ns,
                        "source_feature_ready_ts_ns": self.state.source_ready_ts_ns,
                        "feature_ready_clock": (
                            "latest_source_ready" if self.state.source_ready_ts_ns else "decision_snapshot"
                        ),
                        "scheduled_requote": bool(self.state.scheduled_requote_due),
                    },
                })
            self.state.risk_selection_sides = tuple(row["side"] for row in self.state.risk_selection_rows)
            if self.context.prefill_fork is not None:
                for loop_item in self.state.risk_selection_rows:
                    self.context.row = loop_item
                    if self.context.row['opportunity_id'] != self.context.prefill_fork.opportunity_id:
                        continue
                    if (self.context.prefill_evidence is not None or self.context.row['decision_ts_ns'] != self.context.prefill_fork.decision_ns
                            or self.context.row['decision_sequence'] != self.context.prefill_fork.decision_sequence
                            or self.context.row['order_id'] != self.context.prefill_fork.order_id or self.context.row['kind'] != self.context.prefill_fork.kind):
                        raise ValueError('prefill target differs from the exact parent decision')
                    self.context.prefill_evidence = dict(
                        parent_checkpoint_id=f"{self.context.resume_checkpoint['public_binding']['input_manifest_id']}:{self.context.resume_checkpoint['cut_ts_ms']}:{self.context.resume_checkpoint['next_event']}",
                        phase='baseline_intent_before_order_budget',
                        opportunity_id=self.context.row['opportunity_id'], decision_ts_ns=self.context.row['decision_ts_ns'],
                        decision_sequence=self.context.row['decision_sequence'], order_id=self.context.row['order_id'],
                        execution_end_ns=self.context.prefill_fork.execution_end_ns,
                        economic_prefix_count=len(self.state.economic_fill_facts),
                        notification_prefix_count=len(self.state.trace_fills or []))
            for loop_item in zip(
                self.state.risk_selection_sides, self.state.risk_selection.observe_batch(self.state.risk_selection_rows), strict=True,
            ):
                self.state.side, self.state.chosen = loop_item
                if self.state.chosen == "WAIT":
                    self.state.risk_selection_wait_sides.add(self.state.side)
                elif self.state.chosen == "CANCEL":
                    self.state.risk_selection_cancel_sides.add(self.state.side)

        if self.state.hb_new and self.state.bid_updated and not self.state.bid_cancel_first and not self.state.bid_pending_coalesce:
            self.state.bid_budget_allowed, self.state.bid_budget_reason = self._post_cooldown_budget_prepare_submission(
                "BUY", quantity_btc=float(self.state.bid_sz), exposure_increasing=bool(self.state.q >= 0.0),
                side_ctx=self.state.quote_context["BUY"], reserve="BUY" not in self.state.risk_selection_wait_sides,
            )
            if not self.state.bid_budget_allowed:
                self.state.hb_new = False
                self.state.bid_block_reasons.append(self.state.bid_budget_reason)
                self.state.quote_context["BUY"]["post_cooldown_inventory_budget_block"] = True
        if self.state.ha_new and self.state.ask_updated and not self.state.ask_cancel_first and not self.state.ask_pending_coalesce:
            self.state.ask_budget_allowed, self.state.ask_budget_reason = self._post_cooldown_budget_prepare_submission(
                "SELL", quantity_btc=float(self.state.ask_sz), exposure_increasing=bool(self.state.q <= 0.0),
                side_ctx=self.state.quote_context["SELL"], reserve="SELL" not in self.state.risk_selection_wait_sides,
            )
            if not self.state.ask_budget_allowed:
                self.state.ha_new = False
                self.state.ask_block_reasons.append(self.state.ask_budget_reason)
                self.state.quote_context["SELL"]["post_cooldown_inventory_budget_block"] = True

        self.state.bid_action = self.services._baseline_quote_action(
            route_due=self.state.bid_route_due, forced_cancel=self.state.ranked_guard_bid_cancel_order_id,
            cancel_first=self.state.bid_cancel_first, pending_coalesce=self.state.bid_pending_coalesce,
            enabled=self.state.hb_new, updated=self.state.bid_updated, active=self.state.bid_active_before,
        )
        self.state.ask_action = self.services._baseline_quote_action(
            route_due=self.state.ask_route_due, forced_cancel=self.state.ranked_guard_ask_cancel_order_id,
            cancel_first=self.state.ask_cancel_first, pending_coalesce=self.state.ask_pending_coalesce,
            enabled=self.state.ha_new, updated=self.state.ask_updated, active=self.state.ask_active_before,
        )
        if "BUY" in self.state.risk_selection_wait_sides:
            self.state.hb_new, self.state.bid_updated, self.state.bid_action = False, False, "pause"
            self.state.bid_block_reasons.append("risk_selection_wait")
        if "SELL" in self.state.risk_selection_wait_sides:
            self.state.ha_new, self.state.ask_updated, self.state.ask_action = False, False, "pause"
            self.state.ask_block_reasons.append("risk_selection_wait")
        if "BUY" in self.state.risk_selection_cancel_sides:
            self.state.hb_new, self.state.bid_updated, self.state.bid_action = False, True, "cancel"
        if "SELL" in self.state.risk_selection_cancel_sides:
            self.state.ha_new, self.state.ask_updated, self.state.ask_action = False, True, "cancel"
