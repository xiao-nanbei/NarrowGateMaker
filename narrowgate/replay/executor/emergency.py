"""Replay emergency owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional


class EmergencyActions:
    def _dynamic_fill_hazard_deep_book(self, now_ns: int) -> dict[str, Any]:
        scheduler = (
            self.state.dynamic_fill_hazard_visibility_scheduler
            if self.state.dynamic_fill_hazard_visibility_scheduler is not None
            else self.state.exchange_book_scheduler
        )
        if scheduler is None:
            return {"valid": 0, "age_ms": self.services.math.inf}
        bids, asks = scheduler.top_levels(1)
        ready_ns = int(
            scheduler.last_feature_ready_ts_ns
            if self.state.dynamic_fill_hazard_visibility_scheduler is not None
            else scheduler.last_local_receive_ts_ns
        )
        source_ns = int(
            self._dynamic_fill_hazard_policy_source_ts_ns(
                exchange_ts_ns=(
                    scheduler.last_truth_exchange_ts_ns
                    if self.state.dynamic_fill_hazard_visibility_scheduler is not None
                    else scheduler.stats().last_exchange_ts_ns
                ),
                provider_receive_ts_ns=(
                    scheduler.last_provider_receive_ts_ns
                    if self.state.dynamic_fill_hazard_visibility_scheduler is not None
                    else scheduler.last_local_receive_ts_ns
                ),
            )
        )
        valid = bool(
            scheduler.sequence.initialized
            and bids
            and asks
            and float(asks[0][0]) > float(bids[0][0])
            and ready_ns > 0
            and ready_ns <= int(now_ns)
        )
        return {
            "valid": int(valid),
            "best_bid": float(bids[0][0]) * float(self.state.TICK) if bids else 0.0,
            "best_ask": float(asks[0][0]) * float(self.state.TICK) if asks else 0.0,
            "best_bid_qty": float(bids[0][1]) if bids else 0.0,
            "best_ask_qty": float(asks[0][1]) if asks else 0.0,
            "age_ms": (
                max(0.0, (int(now_ns) - ready_ns) / 1_000_000.0)
                if ready_ns > 0
                else self.services.math.inf
            ),
            "generation": int(scheduler.segment_id),
            "last_receive_ts_ns": source_ns,
            "last_trade_receive_ts_ns": 0,
            "last_trade_feature_ready_ts_ns": 0,
            "feature_ready_ts_ns": ready_ns,
        }

    def _release_dynamic_fill_hazard_hold(self,
        *,
        terminal: bool,
        now_ts: int,
    ) -> None:
        hold = self.state.dynamic_fill_hazard_hold
        if hold is None:
            return
        self.state.dynamic_fill_hazard_paths.pop(str(hold["client_order_id"]), None)
        self.state.dynamic_fill_hazard_hold = None
        if terminal:
            self.state.dynamic_fill_hazard_reentry_count += 1
            self.state.last_rq_ts = min(
                int(self.state.last_rq_ts),
                int(now_ts) - max(1, int(self.state.cur_rq_ms)),
            )

    def _dynamic_fill_hazard_order_terminal(self,
        order: Mapping[str, Any],
        *,
        event: str,
        now_ts: int,
    ) -> None:
        hazard_id = str(order.get("trace_id", ""))
        hold = self.state.dynamic_fill_hazard_hold
        remaining_quantity = float(order.get("remaining", 0.0) or 0.0)
        route = self.services.terminal_policy_route(event, remaining_quantity)
        if route == self.services.TerminalPolicyRoute.UNSUPPORTED:
            self.state.dynamic_fill_hazard_unsupported_terminal_route_count += 1
            raise RuntimeError(
                "q90 lifecycle received unsupported terminal reason: "
                f"{event!r}"
            )
        q90_lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(hazard_id)
        if (
            q90_lifecycle is not None
            and event != "fill"
            and q90_lifecycle.phase != self.services.OrderLifecyclePhase.EXCHANGE_TERMINAL
        ):
            terminal_ns = int(now_ts) * 1_000_000
            q90_lifecycle.exchange_terminal(
                terminal_ns,
                reason=str(event),
                exchange_ts_ns=terminal_ns,
            )
            self._append_dynamic_fill_hazard_lifecycle_journal(
                order,
                str(event),
            )
        if self.state.dynamic_fill_hazard_cpp_runtime is not None and event != "fill":
            matching_hold = bool(
                hold is not None
                and str(hold["client_order_id"]) == hazard_id
            )
            expected_transition = "none"
            if matching_hold:
                expected_transition = {
                    self.services.TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY: (
                        "post_cancel_recovery"
                    ),
                    self.services.TerminalPolicyRoute.TERMINAL_COMPLETE: (
                        "terminal_complete_no_reentry"
                    ),
                    self.services.TerminalPolicyRoute.BASELINE_RESUBMIT: "baseline_resubmit",
                    self.services.TerminalPolicyRoute.SHUTDOWN_NO_REENTRY: (
                        "shutdown_no_reentry"
                    ),
                    self.services.TerminalPolicyRoute.UNSUPPORTED: (
                        "unsupported_terminal_reason"
                    ),
                }[route]
            if event == "cancel_ack":
                cpp_transition = (
                    self.state.dynamic_fill_hazard_cpp_runtime.on_cancel_ack(
                        hazard_id,
                        int(now_ts) * 1_000_000,
                        remaining_quantity,
                    )
                )
            else:
                cpp_transition = (
                    self.state.dynamic_fill_hazard_cpp_runtime.on_order_terminal(
                        hazard_id,
                        int(now_ts) * 1_000_000,
                        str(event),
                        remaining_quantity,
                    )
                )
            self.state.dynamic_fill_hazard_cpp_lifecycle_count += 1
            self._dynamic_fill_hazard_cpp_check(
                cpp_transition == expected_transition,
                "order_terminal",
                now_ts_ms=int(now_ts),
                client_order_id=hazard_id,
                event=str(event),
                python_transition=expected_transition,
                cpp_transition=cpp_transition,
            )
        if hold is None or str(hold["client_order_id"]) != hazard_id:
            self.state.dynamic_fill_hazard_paths.pop(hazard_id, None)
            return
        self.state.dynamic_fill_hazard_paths.pop(hazard_id, None)
        hold["terminal"] = True
        hold["phase"] = "EXCHANGE_TERMINAL"
        hold["exchange_terminal_ts_ms"] = int(now_ts)
        hold["terminal_reason"] = str(event)
        hold["terminal_policy_route"] = route.value
        if route == self.services.TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY:
            hold["phase"] = "POST_CANCEL_RECOVERY"
            if (
                q90_lifecycle is not None
                and q90_lifecycle.phase == self.services.OrderLifecyclePhase.EXCHANGE_TERMINAL
            ):
                q90_lifecycle.enter_post_cancel_recovery(
                    int(now_ts) * 1_000_000
                )
                self._append_dynamic_fill_hazard_lifecycle_journal(
                    order,
                    "post_cancel_recovery",
                )
            self.state.dynamic_fill_hazard_post_cancel_recovery_count += 1
            self.state.dynamic_fill_hazard_cancel_ack_count += 1
            return
        if route != self.services.TerminalPolicyRoute.UNSUPPORTED:
            self._release_dynamic_fill_hazard_hold(
                terminal=False,
                now_ts=int(now_ts),
            )

    def _evaluate_dynamic_fill_hazard_prospective_recovery(self,
        *,
        candidate_price: float,
        now_ts: int,
    ) -> None:

        hold = self.state.dynamic_fill_hazard_hold
        if (
            hold is None
            or str(hold.get("phase", "")) != "POST_CANCEL_RECOVERY"
        ):
            return
        if self.state.dynamic_fill_hazard_runtime is None or self.state.dynamic_fill_hazard_policy is None:
            raise RuntimeError("q90 prospective recovery runtime is unavailable")

        hazard_id = str(hold.get("client_order_id", ""))
        lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(hazard_id)
        if lifecycle is None:
            raise RuntimeError("q90 prospective recovery lifecycle is missing")
        if self.state.dynamic_fill_hazard_runtime.has_active_order_state(hazard_id):
            self.state.dynamic_fill_hazard_post_terminal_hazard_reuse_count += 1
            raise RuntimeError("q90 post-terminal active hazard state was retained")
        if hazard_id in self.state.dynamic_fill_hazard_paths:
            self.state.dynamic_fill_hazard_post_terminal_cursor_reuse_count += 1
            raise RuntimeError("q90 post-terminal depth cursor was retained")

        now_ns = int(now_ts) * 1_000_000
        snapshot = lifecycle.snapshot(now_ns=now_ns)
        route = str(snapshot.get("terminal_policy_route", ""))
        reason = str(snapshot.get("terminal_reason", ""))
        remaining = float(snapshot.get("remaining_quantity", 0.0) or 0.0)
        if route != self.services.TerminalPolicyRoute.PROSPECTIVE_CANCEL_REENTRY.value:
            raise RuntimeError(
                "q90 prospective recovery received a non-cancel route: "
                f"{route!r}"
            )

        scheduler = (
            self.state.dynamic_fill_hazard_visibility_scheduler
            if self.state.dynamic_fill_hazard_visibility_scheduler is not None
            else self.state.exchange_book_scheduler
        )
        if scheduler is None:
            raise RuntimeError("q90 prospective recovery book is unavailable")
        deep_book = self._dynamic_fill_hazard_deep_book(now_ns)
        price_tick = self.services._price_to_tick(float(candidate_price), self.state.TICK)
        level = scheduler.lookup("BUY", price_tick)
        level_ready_ns = int(deep_book.get("feature_ready_ts_ns", 0) or 0)
        level_source_ns = int(deep_book.get("last_receive_ts_ns", 0) or 0)
        candidate_level = {
            "valid": bool(level.strict_usable),
            "covered": bool(level.strict_usable),
            "price": float(price_tick) * float(self.state.TICK),
            "quantity": (
                float(level.quantity) if level.quantity is not None else 0.0
            ),
            "receive_ts_ns": level_source_ns,
            "feature_ready_ts_ns": level_ready_ns,
            "age_ms": float(deep_book.get("age_ms", self.services.math.inf)),
        }
        result = self.state.dynamic_fill_hazard_runtime.evaluate_prospective_cancel_reentry(
            terminal_policy_route=route,
            terminal_reason=reason,
            remaining_quantity=remaining,
            candidate_price=float(candidate_price),
            inventory=float(self.state.q),
            deep_book=deep_book,
            candidate_level=candidate_level,
            now_ns=now_ns,
            prospective_id=hazard_id,
        )
        observation = result.observation
        self.state.dynamic_fill_hazard_prospective_eval_count += 1
        if result.activation_supported:
            self.state.dynamic_fill_hazard_prospective_valid_count += 1
        else:
            self.state.dynamic_fill_hazard_prospective_invalid_count += 1

        recovered = bool(
            result.activation_supported
            and (
                observation.inventory_role == "reducing"
                or self.state.dynamic_fill_hazard_policy.recovered(observation)
            )
        )
        expected_action = (
            "reenter"
            if recovered
            else ("hold" if result.activation_supported else "hold_invalid")
        )
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            cpp_value = self.state.dynamic_fill_hazard_cpp_runtime.evaluate_prospective_cancel_reentry(
                candidate_price=float(candidate_price),
                inventory=float(self.state.q),
                now_ns=now_ns,
            )
            self._compare_dynamic_fill_hazard_cpp_observation(
                client_order_id=hazard_id,
                now_ts=int(now_ts),
                observation=observation,
                cpp_value=cpp_value,
                expected_action=expected_action,
            )
        if not recovered:
            return
        self.state.dynamic_fill_hazard_recovery_count += 1
        lifecycle.mark_reentry_eligible(now_ns)
        self._append_dynamic_fill_hazard_lifecycle_journal(
            {
                "trace_id": hazard_id,
                "side": "BUY",
                "state": "REENTRY_ELIGIBLE",
            },
            "reentry_eligible",
        )
        hold["recovered"] = True
        hold["phase"] = "REENTRY_ELIGIBLE"
        self._release_dynamic_fill_hazard_hold(
            terminal=True,
            now_ts=int(now_ts),
        )

    def _compare_dynamic_fill_hazard_cpp_observation(self,
        *,
        client_order_id: str,
        now_ts: int,
        observation: Any,
        cpp_value: Mapping[str, Any],
        expected_action: str,
    ) -> None:
        self.state.dynamic_fill_hazard_cpp_evaluation_count += 1
        if observation is None:
            self._dynamic_fill_hazard_cpp_check(
                not bool(cpp_value.get("emitted", False)),
                "evaluation_edge",
                now_ts_ms=int(now_ts),
                client_order_id=client_order_id,
                python_emitted=False,
                cpp=cpp_value,
            )
            return

        prediction = dict(cpp_value.get("prediction") or {})
        exact_fields = {
            "valid": bool(observation.valid),
            "reason": str(observation.reason),
            "inventory_role": str(observation.inventory_role),
            "edge_ms": int(observation.edge_ms),
            "missed_edges": int(observation.missed_edges),
            "feature_source_ts_ns": int(observation.feature_source_ts_ns),
            "feature_ready_ts_ns": int(observation.feature_ready_ts_ns),
            "deep_generation": int(observation.deep_generation),
            "cancel_events": int(observation.cancel_events),
            "refill_events": int(observation.refill_events),
        }
        cpp_exact = {
            "valid": bool(cpp_value.get("valid", False)),
            "reason": str(cpp_value.get("reason", "")),
            "inventory_role": str(cpp_value.get("inventory_role", "")),
            "edge_ms": int(cpp_value.get("edge_ms", 0) or 0),
            "missed_edges": int(cpp_value.get("missed_edges", 0) or 0),
            "feature_source_ts_ns": int(
                cpp_value.get("feature_source_ts_ns", 0) or 0
            ),
            "feature_ready_ts_ns": int(
                cpp_value.get("feature_ready_ts_ns", 0) or 0
            ),
            "deep_generation": int(
                cpp_value.get("deep_generation", 0) or 0
            ),
            "cancel_events": int(cpp_value.get("cancel_events", 0) or 0),
            "refill_events": int(cpp_value.get("refill_events", 0) or 0),
        }
        float_pairs = {
            "elapsed_ms": (observation.elapsed_ms, cpp_value.get("elapsed_ms")),
            "deep_age_ms": (observation.deep_age_ms, cpp_value.get("deep_age_ms")),
            "order_price": (observation.order_price, cpp_value.get("order_price")),
            "mid": (observation.mid, cpp_value.get("mid")),
            "weighted_mid_proxy": (observation.weighted_mid_proxy, cpp_value.get("weighted_mid_proxy")),
            "queue_initial": (
                observation.queue_initial,
                cpp_value.get("queue_initial"),
            ),
            "queue_remaining": (
                observation.queue_remaining,
                cpp_value.get("queue_remaining"),
            ),
            "cancel_qty": (observation.cancel_qty, cpp_value.get("cancel_qty")),
            "refill_qty": (observation.refill_qty, cpp_value.get("refill_qty")),
        }
        if observation.valid:
            float_pairs.update(
                {
                    "favorable_probability": (
                        observation.favorable_probability,
                        prediction.get("favorable_probability"),
                    ),
                    "adverse_probability": (
                        observation.adverse_probability,
                        prediction.get("adverse_probability"),
                    ),
                    "favorable_raw_probability": (
                        observation.favorable_raw_probability,
                        prediction.get("favorable_raw_probability"),
                    ),
                    "adverse_raw_probability": (
                        observation.adverse_raw_probability,
                        prediction.get("adverse_raw_probability"),
                    ),
                }
            )
        bad_floats = {
            key: {"python": values[0], "cpp": values[1]}
            for key, values in float_pairs.items()
            if not self._dynamic_fill_hazard_float_equal(values[0], values[1])
        }
        cpp_action = str(cpp_value.get("action", ""))
        self._dynamic_fill_hazard_cpp_check(
            bool(cpp_value.get("emitted", False))
            and exact_fields == cpp_exact
            and not bad_floats
            and cpp_action == str(expected_action),
            "evaluation",
            now_ts_ms=int(now_ts),
            client_order_id=client_order_id,
            python_exact=exact_fields,
            cpp_exact=cpp_exact,
            float_mismatches=bad_floats,
            python_action=str(expected_action),
            cpp_action=cpp_action,
        )

    def _evaluate_dynamic_fill_hazard_action(self, now_ts: int) -> None:

        if self.state.dynamic_fill_hazard_runtime is None or self.state.dynamic_fill_hazard_policy is None:
            return
        if self.state.dynamic_fill_hazard_hold is not None and self.state.q < -max(
            float(self.state.LOT_SIZE) * 0.5,
            1e-12,
        ):
            if self.state.dynamic_fill_hazard_cpp_runtime is not None:
                hold_id = str(self.state.dynamic_fill_hazard_hold["client_order_id"])
                expected_transition = (
                    "reenter"
                    if bool(self.state.dynamic_fill_hazard_hold.get("terminal", False))
                    else "release"
                )
                cpp_value = self.state.dynamic_fill_hazard_cpp_runtime.evaluate(
                    hold_id,
                    float(self.state.q),
                    int(now_ts) * 1_000_000,
                )
                self._dynamic_fill_hazard_cpp_check(
                    str(cpp_value.get("action", ""))
                    == expected_transition,
                    "reducing_release",
                    now_ts_ms=int(now_ts),
                    client_order_id=hold_id,
                    python_action=expected_transition,
                    cpp=cpp_value,
                )
            self._release_dynamic_fill_hazard_hold(
                terminal=bool(self.state.dynamic_fill_hazard_hold.get("terminal", False)),
                now_ts=int(now_ts),
            )
            return

        order_by_id = {
            str(order.get("trace_id", "")): order
            for order in self.state.bid_orders
            if order.get("state") in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
        }
        active_ids = set(order_by_id)
        self.state.dynamic_fill_hazard_runtime.drop_inactive(sorted(active_ids))
        if self.state.dynamic_fill_hazard_cpp_runtime is not None:
            self.state.dynamic_fill_hazard_cpp_runtime.drop_inactive(sorted(active_ids))
            self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_sync_count += (
                self.services.synchronize_visibility_batch_ambiguity_to_cpp(
                    self.state.dynamic_fill_hazard_paths,
                    self.state.dynamic_fill_hazard_cpp_runtime,
                    self.state.dynamic_fill_hazard_cpp_visibility_ambiguity_synced_ids,
                )
            )
        if not active_ids:
            return

        now_ns = int(now_ts) * 1_000_000
        deep_book = self._dynamic_fill_hazard_deep_book(now_ns)
        for hazard_id in sorted(active_ids):
            path = self.state.dynamic_fill_hazard_paths.get(hazard_id)
            if path is None:
                continue
            path.update_age(now_ns)
            order = order_by_id.get(hazard_id)
            order_price = (
                float(order["price"])
                if order is not None
                else float(self.state.dynamic_fill_hazard_hold["order_price"])
            )
            observation = self.state.dynamic_fill_hazard_runtime.evaluate(
                client_order_id=hazard_id,
                side="BUY",
                order_price=order_price,
                inventory=float(self.state.q),
                path=path,
                deep_book=deep_book,
                now_ns=now_ns,
            )
            cpp_value = (
                self.state.dynamic_fill_hazard_cpp_runtime.evaluate(
                    hazard_id,
                    float(self.state.q),
                    now_ns,
                )
                if self.state.dynamic_fill_hazard_cpp_runtime is not None
                else None
            )
            if cpp_value is not None:
                hold_before = self.state.dynamic_fill_hazard_hold
                expected_action = "none"
                if observation is not None:
                    if hold_before is None:
                        if self.state.dynamic_fill_hazard_policy.eligible(observation):
                            expected_action = (
                                "cancel"
                                if self.state.dynamic_fill_hazard_policy.cancel_required(
                                    observation
                                )
                                else "keep"
                            )
                    elif str(hold_before["client_order_id"]) == hazard_id:
                        if not observation.valid:
                            expected_action = "hold_invalid"
                        else:
                            recovered_before = bool(
                                observation.inventory_role == "reducing"
                                or self.state.dynamic_fill_hazard_policy.recovered(
                                    observation
                                )
                            )
                            if not recovered_before:
                                expected_action = "hold"
                            else:
                                cancel_pending_before = bool(
                                    order is not None
                                    and order.get("state")
                                    == self.state.ORDER_PENDING_CANCEL
                                )
                                expected_action = (
                                    "recover_wait_ack"
                                    if cancel_pending_before
                                    else "release"
                                )
                self._compare_dynamic_fill_hazard_cpp_observation(
                    client_order_id=hazard_id,
                    now_ts=int(now_ts),
                    observation=observation,
                    cpp_value=cpp_value,
                    expected_action=expected_action,
                )
            if observation is None:
                continue
            self.state.dynamic_fill_hazard_eval_count += 1
            role = str(observation.inventory_role)
            if self.state.dynamic_fill_hazard_mechanics_telemetry_enabled:
                role_counts = self.state.dynamic_fill_hazard_eval_by_role.setdefault(
                    role,
                    {"evaluations": 0, "valid": 0, "invalid": 0},
                )
                role_counts["evaluations"] += 1
            if observation.valid:
                self.state.dynamic_fill_hazard_valid_eval_count += 1
                if self.state.dynamic_fill_hazard_mechanics_telemetry_enabled:
                    role_counts["valid"] += 1
                    self.state.dynamic_fill_hazard_valid_scores.append(
                        float(self.state.dynamic_fill_hazard_policy.score(observation))
                    )
            else:
                self.state.dynamic_fill_hazard_invalid_eval_count += 1
                if self.state.dynamic_fill_hazard_mechanics_telemetry_enabled:
                    role_counts["invalid"] += 1
                invalid_reason = str(observation.reason or "unattributed")
                self.state.dynamic_fill_hazard_invalid_reason_counts[invalid_reason] = int(
                    self.state.dynamic_fill_hazard_invalid_reason_counts.get(
                        invalid_reason,
                        0,
                    )
                    + 1
                )

            hold = self.state.dynamic_fill_hazard_hold
            if hold is None:
                if not self.state.dynamic_fill_hazard_policy.eligible(observation):
                    continue
                if not self.state.dynamic_fill_hazard_policy.cancel_required(observation):
                    self.state.dynamic_fill_hazard_keep_count += 1
                    if self.state.dynamic_fill_hazard_mechanics_telemetry_enabled:
                        action_counts = self.state.dynamic_fill_hazard_action_by_role.setdefault(
                            role,
                            {"keep": 0, "cancel_signal": 0},
                        )
                        action_counts["keep"] += 1
                    continue
                if self.state.dynamic_fill_hazard_mechanics_telemetry_enabled:
                    action_counts = self.state.dynamic_fill_hazard_action_by_role.setdefault(
                        role,
                        {"keep": 0, "cancel_signal": 0},
                    )
                    action_counts["cancel_signal"] += 1
                if self.state.dynamic_fill_hazard_action_application == "shadow":
                    self.state.dynamic_fill_hazard_shadow_cancel_signal_count += 1
                    continue
                self.state.dynamic_fill_hazard_hold = {
                    "client_order_id": hazard_id,
                    "order_price": order_price,
                    "entry_score": float(
                        self.state.dynamic_fill_hazard_policy.score(observation)
                    ),
                    "entered_ts_ms": int(now_ts),
                    "phase": "CANCEL_PENDING",
                    "recovered": False,
                    "terminal": False,
                }
                self.state.dynamic_fill_hazard_cancel_request_count += 1
                if order is not None:
                    self._request_cancel_all(
                        [order],
                        int(now_ts),
                        reason="dynamic_fill_hazard_q90",
                    )
                continue

            if str(hold["client_order_id"]) != hazard_id:
                continue
            if not observation.valid:
                self.state.dynamic_fill_hazard_retain_invalid_count += 1
                continue
            recovered = bool(
                observation.inventory_role == "reducing"
                or self.state.dynamic_fill_hazard_policy.recovered(observation)
            )
            if not recovered:
                continue
            if not bool(hold.get("recovered", False)):
                self.state.dynamic_fill_hazard_recovery_count += 1
            hold["recovered"] = True
            cancel_pending = bool(
                order is not None
                and order.get("state") == self.state.ORDER_PENDING_CANCEL
            )
            if not cancel_pending:
                self._release_dynamic_fill_hazard_hold(
                    terminal=False,
                    now_ts=int(now_ts),
                )

    def _dynamic_fill_hazard_buy_blocked(self) -> bool:
        return bool(
            self.state.dynamic_fill_hazard_hold is not None
            and self.state.q >= -max(float(self.state.LOT_SIZE) * 0.5, 1e-12)
        )

    def _requote_circuit_breaker_close(self,
        now_ts: int, close_mid: float, *, emergency_market: bool = False,
    ) -> None:
        """Mirror live maker-close escalation from passive GTX to LIMIT IOC."""


        if abs(self.state.q) < self.state.LOT_SIZE:
            return
        close_side = "SELL" if self.state.q > 0.0 else "BUY"
        close_orders = self.state.ask_orders if close_side == "SELL" else self.state.bid_orders
        opening_orders = self.state.bid_orders if close_side == "SELL" else self.state.ask_orders
        if not self.state.async_rest_gateway:
            self._request_cancel_all(
                opening_orders,
                int(now_ts),
                reason="circuit_breaker_opening_side",
            )

        close_qty = self.services.math.floor(
            (abs(self.state.q) if emergency_market else min(abs(self.state.q), self.state.order_size)) / self.state.LOT_SIZE + 1e-12
        ) * self.state.LOT_SIZE
        if close_qty < self.state.LOT_SIZE:
            return

        stale_ms = max(0, int(now_ts - self.state.circuit_breaker_close_start_ts))
        use_ioc = (
            emergency_market
            or stale_ms >= 60_000
            or self.state.circuit_breaker_close_gtx_reject_streak >= 3
        )
        aggressive_passive = not use_ioc and stale_ms >= 30_000
        if use_ioc and close_side == "SELL":
            touch = self.state.cur_best_bid if self.state.cur_best_bid > 0.0 else close_mid
            close_price = self.services.math.floor(
                (touch - 2.0 * self.state.TICK) / self.state.TICK + 1e-12
            ) * self.state.TICK
        elif use_ioc:
            touch = self.state.cur_best_ask if self.state.cur_best_ask > 0.0 else close_mid
            close_price = self.services.math.ceil(
                (touch + 2.0 * self.state.TICK) / self.state.TICK - 1e-12
            ) * self.state.TICK
        elif close_side == "SELL":
            close_price = self.services.math.ceil(close_mid / self.state.TICK - 1e-12) * self.state.TICK
            if aggressive_passive:
                close_price -= self.state.TICK
        else:
            close_price = self.services.math.floor(close_mid / self.state.TICK + 1e-12) * self.state.TICK
            if aggressive_passive:
                close_price += self.state.TICK
        if self.state.diagnostic_passive_close_bbo_clip and not use_ioc:
            if close_side == "SELL" and self.state.cur_best_bid > 0.0:
                close_price = max(close_price, self.state.cur_best_bid + self.state.TICK)
            elif close_side == "BUY" and self.state.cur_best_ask > 0.0:
                close_price = min(close_price, self.state.cur_best_ask - self.state.TICK)
        close_price = round(close_price / self.state.TICK) * self.state.TICK

        if self.state.async_rest_gateway:
            # The live close call is synchronous even with an async ordinary
            # gateway. Keep its already-calculated price/quantity/snapshot
            # across cancel.result(), while the event stream and FIFO worker
            # continue. Do not turn the return into a fresh quote decision.
            close_context = {close_side: {
                "reduce_only": True, "circuit_breaker_close": True,
                "order_ttl_ms": 0, "inventory": float(self.state.q), "mid": float(close_mid),
                "best_bid": float(self.state.cur_best_bid), "best_ask": float(self.state.cur_best_ask),
            }}
            if self.state.ema_add_wait_fork_assigned:
                close_context[close_side]["ema_add_wait_fork_id"] = (
                    f"{self.state.ema_add_wait_fork_target_inventory_lifecycle_id}:"
                    f"{self.state.ema_add_wait_fork_target_ts_ms}:{self.state.ema_add_wait_fork_action}"
                )
            would_cross = self.services._order_would_cross_book_tick(
                close_side, float(close_price), float(self.state.cur_best_bid), float(self.state.cur_best_ask), self.state.TICK,
            )

            continuation = {
                "action": "continue", "side": close_side,
                "close_orders": close_orders, "opening_orders": opening_orders,
                "price": float(close_price), "quantity": float(close_qty),
                "mid": float(close_mid), "quote_context": close_context,
                "would_cross": would_cross, "use_ioc": use_ioc,
                "emergency_market": bool(emergency_market),
            }

            if emergency_market:
                # Emergency MARKET is a one-shot call after cancel-all HTTP,
                # not the maker-close cancel/requote state machine. Its caller
                # has checked only the side needed for the market order.
                self._resume_maker_close({**continuation, "action": "submit"}, int(now_ts))
                return
            if not close_orders and opening_orders:
                if any(order.get("state") != self.state.ORDER_OPEN for order in opening_orders):
                    return
                self._request_cancel_all(
                    opening_orders, int(now_ts), reason="circuit_breaker_opening_side",
                )
                if self.state.main_loop_waiting_request is not None:
                    self.state.main_loop_waiting_request["resume_close"] = continuation
                return
            self._resume_maker_close(continuation, int(now_ts))
            return

        existing = next(
            (
                order
                for order in close_orders
                if order.get("circuit_breaker_close", False)
                and order.get("state") in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL)
                and order.get("remaining", 0.0) >= self.state.LOT_SIZE
                ),
                None,
            )
        if existing is not None and not use_ioc:
            drift_bps = (
                abs(close_price - float(existing["price"]))
                / max(float(existing["price"]), self.state.TICK)
                * 10_000.0
            )
            if drift_bps <= max(
                0.0, float(self.context.params.get("requote_threshold_bps", 0.0) or 0.0)
            ):
                self.state.circuit_breaker_close_keep_count += 1
                return

        if close_orders:
            self._request_cancel_all(
                close_orders,
                int(now_ts),
                reason="circuit_breaker_close_requote",
            )

        would_cross = self.services._order_would_cross_book_tick(
            close_side,
            float(close_price),
            float(self.state.cur_best_bid),
            float(self.state.cur_best_ask),
            self.state.TICK,
        )
        if would_cross and not use_ioc:
            self.state.gtx_rejects += 1
            self.state.circuit_breaker_close_gtx_reject_count += 1
            self.state.circuit_breaker_close_gtx_reject_streak += 1
            return

        close_context = {
            close_side: {
                "reduce_only": True,
                "circuit_breaker_close": True,
                "order_ttl_ms": 0,
                "inventory": float(self.state.q),
                "mid": float(close_mid),
                "best_bid": float(self.state.cur_best_bid),
                "best_ask": float(self.state.cur_best_ask),
            }
        }
        if self.state.ema_add_wait_fork_assigned:
            close_context[close_side]["ema_add_wait_fork_id"] = (
                f"{self.state.ema_add_wait_fork_target_inventory_lifecycle_id}:"
                f"{self.state.ema_add_wait_fork_target_ts_ms}:"
                f"{self.state.ema_add_wait_fork_action}"
            )
        order = self._make_order(
            close_side,
            float(close_price),
            float(close_qty),
            int(now_ts),
            float(close_mid),
            quote_context=close_context,
            apply_decision_compute=False,
            creation_source="requote_circuit_breaker_close",
        )
        order["time_in_force"] = "IOC" if use_ioc else "GTX"
        order["emergency_market"] = bool(emergency_market)
        close_orders.append(order)
        self.state.circuit_breaker_close_place_count += 1
        if use_ioc:
            self.state.circuit_breaker_close_ioc_place_count += 1

    def _attempt_async_emergency_close(self, now_ts: int) -> None:
        """Mirror the stopped live caller's one ownership attempt after HTTP."""
        if self.state.risk_emergency_attempt_complete:
            return
        self.state.risk_emergency_attempt_complete = True
        if abs(self.state.q) < self.state.LOT_SIZE:
            return
        side = "SELL" if self.state.q > 0.0 else "BUY"
        owners = self.state.ask_orders if side == "SELL" else self.state.bid_orders
        if owners:
            # Bulk HTTP success does not clear private-stream ownership. Live
            # stops here; a later terminal callback does not retry the MARKET.
            # Its fatal cancel/query recovery is unmodeled, not synthetic flat.
            self.state.risk_emergency_ownership_conflict_count += 1
            if self.state.trace_decisions is not None and len(self.state.trace_risk_actions) < self.state.trace_decisions_max:
                self.state.trace_risk_actions.append({
                    "ts_ms": int(now_ts), "side": side,
                    "reason": "order_ownership_conflict",
                    "risk_action": "stop_reconciliation_required",
                })
            return
        self._requote_circuit_breaker_close(
            int(now_ts), self.state.hard_risk_mark_price, emergency_market=True,
        )

    def _record_economic_match(self, side, order, timestamp, quantity, price, fee_rate):
        """Capture once at matching; never advance strategy-visible state."""
        sequence = self.state.private_fill_exchange_match_count - 1
        row = {
            "match_sequence": int(sequence), "order_id": int(order["trace_id"]),
            "fill_ts": int(timestamp), "side": str(side),
            "fill_qty": float(quantity), "quote_px": float(price),
            "fill_fee_usdc": float(quantity) * float(price) * float(fee_rate),
        }
        previous = self.state.economic_fill_facts
        if previous and row["fill_ts"] < previous[-1]["fill_ts"]:
            raise ValueError("economic matching clock regressed at producer")
        size = float(quantity) * (1 if side == "BUY" else -1)
        self.state.economic_match_inventory += size
        self.state.economic_match_cash -= size * float(price) + row["fill_fee_usdc"]
        if len(previous) < self.state.trace_fills_max:
            previous.append(row)
        return int(sequence)

    def _append_fill_trace(self, side: str, idx: int, quote_px: float, fill_qty: float,
                           quote_ts: int, quote_mid: float, queue_init: float,
                           queue_before: float, rem_before: float,
                           inventory_before_fill: float, order=None,
                           fee_rate: Optional[float] = None,
                           fill_ts_ms: Optional[int] = None,
                           clock_context: Optional[dict] = None,
                           economic_match_sequence: Optional[int] = None):
        self.state.trace_attempt_counts["fills"] += 1
        t_fill = self.state.trade_ts[idx] if fill_ts_ms is None else int(fill_ts_ms)
        production_event = self.state.l2_production.observe("fill", t_fill, side)
        if (self.state.l2_journal is None and
                (self.state.trace_fills is None or len(self.state.trace_fills) >= self.state.trace_fills_max)):
            return
        t_fill = self.state.trade_ts[idx] if fill_ts_ms is None else int(fill_ts_ms)
        left5 = self.services.np.searchsorted(self.state.trade_ts, t_fill - 5000, side="left")
        right5 = self.services.np.searchsorted(self.state.trade_ts, t_fill + 5000, side="right")
        left10 = self.services.np.searchsorted(self.state.trade_ts, t_fill - 10000, side="left")
        right10 = self.services.np.searchsorted(self.state.trade_ts, t_fill + 10000, side="right")
        left60 = self.services.np.searchsorted(self.state.trade_ts, t_fill - 60000, side="left")
        right60 = self.services.np.searchsorted(self.state.trade_ts, t_fill + 60000, side="right")
        qleft = self.services.np.searchsorted(self.state.trade_ts, quote_ts, side="left")
        qright = self.services.np.searchsorted(self.state.trade_ts, min(t_fill, quote_ts + self.state.trace_window_ms), side="right")
        px5 = self.state.trade_price[left5:right5]
        px10 = self.state.trade_price[left10:right10]
        px60 = self.state.trade_price[left60:right60]
        pxq = self.state.trade_price[qleft:qright]
        if len(px5) == 0:
            px5 = self.state.trade_price[idx:idx + 1]
        if len(px10) == 0:
            px10 = self.state.trade_price[idx:idx + 1]
        if len(px60) == 0:
            px60 = self.state.trade_price[idx:idx + 1]
        if len(pxq) == 0:
            pxq = self.state.trade_price[idx:idx + 1]
        quote_dist = (quote_mid - quote_px) if side == "BUY" else (quote_px - quote_mid)
        if side == "BUY":
            quote_window_extreme = float(pxq.min())
            quote_window_move = quote_mid - quote_window_extreme
        else:
            quote_window_extreme = float(pxq.max())
            quote_window_move = quote_window_extreme - quote_mid
        effective_fee_rate = (
            float(self.state.maker_fee) if fee_rate is None else float(fee_rate)
        )
        window120_min = float(px60.min())
        window120_max = float(px60.max())
        window120_span = window120_max - window120_min
        if window120_span > self.state.TICK:
            window120_rank = (self.state.trade_price[idx] - window120_min) / window120_span
            window120_rank = float(max(0.0, min(1.0, window120_rank)))
        else:
            window120_rank = 0.5
        row = {
            # Local notification order, NOT matching order when latency reorders
            # callbacks. Economic matching facts carry their own producer index.
            "fill_sequence": self.state.trace_attempt_counts["fills"] - 1,
            "side": side,
            "fill_ts": int(t_fill),
            "quote_ts": int(quote_ts),
            "age_ms": int(t_fill - quote_ts),
            "quote_mid": float(quote_mid),
            "quote_px": float(quote_px),
            "fill_trade_px": float(self.state.trade_price[idx]),
            "quote_dist": float(quote_dist),
            "quote_window_extreme": quote_window_extreme,
            "quote_window_move": float(quote_window_move),
            "window5_min": float(px5.min()),
            "window5_max": float(px5.max()),
            "window10_min": float(px10.min()),
            "window10_max": float(px10.max()),
            "window120_min": window120_min,
            "window120_max": window120_max,
            "window120_rank": window120_rank,
            "move_from_quote_mid_to_fill": float(abs(self.state.trade_price[idx] - quote_mid)),
            "queue_init": float(queue_init),
            "queue_before": float(queue_before),
            "rem_before": float(rem_before),
            "fill_qty": float(fill_qty),
            "fill_fee_rate": float(effective_fee_rate),
            "fill_fee_usdc": float(
                max(0.0, float(fill_qty))
                * max(0.0, float(quote_px))
                * effective_fee_rate
            ),
            "fill_fee_asset": "USDC",
            "fill_fee_semantics": self.services.FEE_ACCOUNTING_SEMANTICS,
            "inventory_before_fill": float(inventory_before_fill),
            "inventory_after_fill": float(
                inventory_before_fill + (fill_qty if side == "BUY" else -fill_qty)
            ),
        }
        row.update(self.services.fill_markout_diagnostics(
            self.state.trade_ts, self.state.trade_price, fill_ts_ms=t_fill,
            quote_px=quote_px, side=side, fee_rate=effective_fee_rate,
            outcome_end_ts_ms=self.context.params.get("replay_event_clock_end_ts_ms"),
        ))
        if order is not None:
            row.update(self._order_trace_fields(order))
        if clock_context is not None:
            row["fill_clock_context"] = dict(
                # A single nullable struct for passive and IOC fills. Arrow
                # otherwise adds absent keys only on Parquet readback.
                {"exchange_reserved": None, "match_processed_ts_ms": None,
                 **clock_context},
                input_event_index=int(idx) + self.state.input_event_offset,
                indexed_trade_ts_ms=int(self.state.trade_ts[idx]),
                outer_loop_ts_ms=int(self.state.t),
                exchange_inventory=float(self.state.exchange_inventory),
                local_inventory=float(self.state.q),
                time_source="indexed_trade" if fill_ts_ms is None else "explicit_match",
            )
        if economic_match_sequence is not None:
            row["economic_match_sequence"] = int(economic_match_sequence)
        if self.state.l2_journal is not None:
            self.state.l2_journal.emit("fill", t_fill, side=side, payload=row, production_event=production_event)
        if self.state.trace_fills is not None and len(self.state.trace_fills) < self.state.trace_fills_max:
            self.state.trace_fills.append(row)

    def _process_circuit_breaker_ioc_orders(self,
        orders: list,
        side: str,
        now_ts: int,
        trade_idx: int,
        fallback_mid: float,
    ) -> bool:
        """Sweep once on exchange time; publish economics only when locally visible."""


        filled = False
        idx = 0
        while idx < len(orders):
            order = orders[idx]
            if (
                order.get("time_in_force") != "IOC"
                or not order.get("exchange_accepted", False)
                or int(order.get("activate_ts", now_ts)) > now_ts
            ):
                idx += 1
                continue

            exchange_ts = int(order["activate_ts"])
            if "_ioc_match" not in order:
                best_bid_at, best_ask_at, bid_qty_at, ask_qty_at, _, activation_source = self._book_snapshot_at(
                    exchange_ts, fallback_mid,
                )
                if activation_source == "unavailable_book":
                    order["activation_book_status"] = "UNKNOWN"
                    idx += 1
                    continue
                fill_price = float(best_ask_at if side == "BUY" else best_bid_at)
                top_qty = float(ask_qty_at if side == "BUY" else bid_qty_at)
                physical_inventory = self.state.exchange_inventory if self.state.private_fill_visibility_enabled else self.state.q
                reducible = max(0.0, -physical_inventory if side == "BUY" else physical_inventory)
                depth_pos = (
                    int(self.services.np.searchsorted(self.state.l2_ts, exchange_ts, side="right")) - 1
                    if self.state.historical_l2 and self.state.l2_ts is not None else -1
                )
                if depth_pos >= 0:
                    prices = self.state.l2_ask_px[depth_pos] if side == "BUY" else self.state.l2_bid_px[depth_pos]
                    quantities = self.state.l2_ask_qty[depth_pos] if side == "BUY" else self.state.l2_bid_qty[depth_pos]
                else:
                    prices, quantities = (fill_price,), (top_qty,)
                fill_qty, fill_price = self.services._match_ioc_order(
                    side, float(order["price"]),
                    min(float(order.get("remaining", 0.0)), reducible),
                    prices, quantities, tick_size=self.state.TICK, lot_size=self.state.LOT_SIZE,
                    market=bool(order.get("emergency_market", False)),
                )
                order["_ioc_match"] = (float(fill_qty), float(fill_price))
                order["_ioc_first_match_processed_ts_ms"] = int(now_ts)
                # An IOC never rests, including its unfilled remainder.
                order["exchange_remaining"] = 0.0
                order["exchange_fill_terminal"] = fill_qty >= float(order["remaining"])
                order["fill_eligible"] = False
                if fill_qty >= self.state.LOT_SIZE:
                    visible_ts = self._sample_private_fill_visible_ts(
                        exchange_ts=exchange_ts, side=side,
                        order_ts=int(order.get("submit_ts", 0)),
                    )
                    order["last_exchange_fill_ts_ms"] = exchange_ts
                    if self.state.private_fill_visibility_enabled:
                        self.state.exchange_inventory += fill_qty if side == "BUY" else -fill_qty
                        self.state.private_fill_exchange_match_count += 1
                        order["_ioc_economic_match_sequence"] = self._record_economic_match(
                            side, order, exchange_ts, fill_qty, fill_price, self.state.taker_fee,
                        )
                else:
                    visible_ts = max(exchange_ts, int(order.get("new_ack_ts", exchange_ts)))
                    if self.state.async_rest_gateway:
                        # The synchronous close caller requests a validated
                        # RESULT. Its explicit zero-filled EXPIRED result can
                        # establish terminal, never a positive economic fill.
                        visible_ts = min(visible_ts, int(order["new_rest_return_ts"]))
                order["ioc_terminal_visible_ts"] = int(visible_ts)
                if self.state.local_lifecycle_boundary_scheduler is not None:
                    self.state.local_lifecycle_boundary_scheduler.schedule(
                        ts_ms=int(visible_ts), phase="private_fill_visible",
                        event_id=f"ioc-visible:{order['trace_id']}:{visible_ts}",
                    )
            fill_qty, fill_price = order["_ioc_match"]
            if int(now_ts) < int(order["ioc_terminal_visible_ts"]):
                idx += 1
                continue
            order["local_new_ack_published"] = True
            order["state"] = self.state.ORDER_OPEN
            order["ioc_local_terminal"] = True
            self._record_order_lifecycle_activate(
                order, visibility_ts_ms=int(now_ts), exchange_ts_ms=exchange_ts,
                mid=float(order.get("mid_at_quote", fallback_mid)),
            )
            self._ranked_guard_order_activated(
                order, visibility_ts_ms=int(now_ts), exchange_ts_ms=exchange_ts,
            )
            if fill_qty < self.state.LOT_SIZE:
                self.state.circuit_breaker_close_ioc_expire_count += 1
                self._record_order_lifecycle_cancel_request(
                    order,
                    int(now_ts),
                    reason="ioc_no_top_liquidity",
                )
                self._record_order_lifecycle_cancel_ack(
                    order,
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=exchange_ts,
                    reason="ioc_no_top_liquidity",
                )
                self._ranked_guard_cancel_requested(
                    order,
                    visibility_ts_ms=int(now_ts),
                    reason="ioc_no_top_liquidity",
                )
                self._ranked_guard_exchange_terminal(
                    order,
                    reason="expired",
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=exchange_ts,
                )
                self._append_order_outcome(
                    order,
                    now_ts,
                    "cancel",
                    "ioc_no_top_liquidity",
                )
                self._post_cooldown_budget_release_order(
                    order,
                    now_ts=int(now_ts),
                    reason="ioc_no_top_liquidity",
                )
                orders.pop(idx)
                if (
                    self.state.main_loop_waiting_request is not None
                    and self.state.main_loop_waiting_request.get("order") is order
                ):
                    self._complete_synchronous_rest_request(self.state.main_loop_waiting_request, int(now_ts))
                continue

            q_before_fill = self.state.q
            if side == "BUY":
                previous_fill_ts = int(self.state.last_buy_fill_ts)
                previous_cooldown_ms = float(self.state.last_buy_fill_cooldown_ms)
                self.state.cash -= fill_price * fill_qty * (1.0 + self.state.taker_fee)
                self.state.q += fill_qty
                self.state.nfb += 1
                self.state.buy_fill_qty_total += fill_qty
                self.state.buy_fill_notional += fill_price * fill_qty
                self.state.fills_bid_not_final_compressed += 1
                self.state.consec_buy += fill_qty / max(self.state.order_size, self.state.LOT_SIZE)
                self.state.consec_sell = 0.0
                self.state.last_buy_fill_ts = int(now_ts)
                new_cooldown_ms = self._cooldown_ms_for_fill(
                    "BUY",
                    q_before_fill,
                    self.state.consec_buy,
                    int(now_ts),
                    previous_fill_ts_ms=previous_fill_ts,
                    previous_cooldown_ms=previous_cooldown_ms,
                    order=order,
                )
                self.state.last_buy_fill_cooldown_ms = self.services.cooldown_duration_after_same_side_fill(
                    previous_fill_ts_ms=previous_fill_ts,
                    previous_cooldown_ms=previous_cooldown_ms,
                    current_fill_ts_ms=int(now_ts),
                    new_cooldown_ms=new_cooldown_ms,
                )
                self.state.last_buy_fill_cooldown_total_ms = self.state.last_buy_fill_cooldown_ms
                self._cooldown_rearm_on_fill('BUY', int(now_ts), new_cooldown_ms)
                self._cooldown_lineage_on_fill(
                    "BUY",
                    prev_q=q_before_fill,
                    fill_ts_ms=int(now_ts),
                    fill_price=float(fill_price),
                    fill_qty=float(fill_qty),
                    fee_rate=float(self.state.taker_fee),
                    consecutive_units=float(self.state.consec_buy),
                    baseline_cooldown_ms=float(new_cooldown_ms),
                )
                self._variance_time_on_fill(
                    "BUY",
                    prev_q=q_before_fill,
                    fill_ts_ms=int(now_ts),
                    consecutive_units=self.state.consec_buy,
                    baseline_cooldown_ms=new_cooldown_ms,
                )
                self.state.last_sell_fill_cooldown_ms = 0.0
                self.state.last_sell_fill_cooldown_total_ms = 0.0
            else:
                previous_fill_ts = int(self.state.last_sell_fill_ts)
                previous_cooldown_ms = float(self.state.last_sell_fill_cooldown_ms)
                self.state.cash += fill_price * fill_qty * (1.0 - self.state.taker_fee)
                self.state.q -= fill_qty
                self.state.nfa += 1
                self.state.sell_fill_qty_total += fill_qty
                self.state.sell_fill_notional += fill_price * fill_qty
                self.state.fills_ask_not_final_compressed += 1
                self.state.consec_sell += fill_qty / max(self.state.order_size, self.state.LOT_SIZE)
                self.state.consec_buy = 0.0
                self.state.last_sell_fill_ts = int(now_ts)
                new_cooldown_ms = self._cooldown_ms_for_fill(
                    "SELL",
                    q_before_fill,
                    self.state.consec_sell,
                    int(now_ts),
                    previous_fill_ts_ms=previous_fill_ts,
                    previous_cooldown_ms=previous_cooldown_ms,
                    order=order,
                )
                self.state.last_sell_fill_cooldown_ms = self.services.cooldown_duration_after_same_side_fill(
                    previous_fill_ts_ms=previous_fill_ts,
                    previous_cooldown_ms=previous_cooldown_ms,
                    current_fill_ts_ms=int(now_ts),
                    new_cooldown_ms=new_cooldown_ms,
                )
                self.state.last_sell_fill_cooldown_total_ms = self.state.last_sell_fill_cooldown_ms
                self._cooldown_rearm_on_fill('SELL', int(now_ts), new_cooldown_ms)
                self._cooldown_lineage_on_fill(
                    "SELL",
                    prev_q=q_before_fill,
                    fill_ts_ms=int(now_ts),
                    fill_price=float(fill_price),
                    fill_qty=float(fill_qty),
                    fee_rate=float(self.state.taker_fee),
                    consecutive_units=float(self.state.consec_sell),
                    baseline_cooldown_ms=float(new_cooldown_ms),
                )
                self._variance_time_on_fill(
                    "SELL",
                    prev_q=q_before_fill,
                    fill_ts_ms=int(now_ts),
                    consecutive_units=self.state.consec_sell,
                    baseline_cooldown_ms=new_cooldown_ms,
                )
                self.state.last_buy_fill_cooldown_ms = 0.0
                self.state.last_buy_fill_cooldown_total_ms = 0.0

            self._loss_cooldown_on_fill(
                side,
                float(fill_qty),
                float(fill_price),
                float(self.state.taker_fee),
            )

            order["remaining"] = self.services.subtract_lot_quantity(
                float(order.get("remaining", 0.0)), fill_qty, self.state.LOT_SIZE,
            )
            if self.state.private_fill_visibility_enabled:
                self.state.private_fill_visible_count += 1
                order["last_private_fill_visible_ts_ms"] = int(now_ts)
            self._post_cooldown_budget_on_fill(
                order,
                side=side,
                fill_qty_btc=float(fill_qty),
                now_ts=int(now_ts),
            )
            lifecycle_remaining_before = float(
                order.get("remaining", 0.0) + fill_qty
            )
            (
                physical_fill_identity,
                economic_fill_legs,
            ) = self._inventory_lifecycle_legs_for_order_fill(
                order,
                ts_ms=int(now_ts),
                side=side,
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                fill_qty=float(fill_qty),
                fill_price=float(fill_price),
                fee_rate=float(self.state.taker_fee),
                remaining_before=lifecycle_remaining_before,
                remaining_after=float(order.get("remaining", 0.0)),
            )
            self._record_order_lifecycle_fill(
                order,
                int(now_ts),
                exchange_ts_ms=exchange_ts,
                fill_qty=float(fill_qty),
                remaining_before=lifecycle_remaining_before,
                remaining_after=float(order.get("remaining", 0.0)),
                fill_price=float(fill_price),
                inventory_before=float(q_before_fill),
                inventory_after=float(self.state.q),
                inventory_lifecycle_id=int(economic_fill_legs[0]["inventory_lifecycle_id"]),
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            ioc_binding_full_fill = float(
                order.get("remaining", 0.0) or 0.0
            ) < self.state.LOT_SIZE
            self._ranked_guard_order_fill(
                order,
                remaining_after=(
                    0.0
                    if ioc_binding_full_fill
                    else float(order.get("remaining", 0.0) or 0.0)
                ),
                visibility_ts_ms=int(now_ts),
                exchange_ts_ms=exchange_ts,
                full_fill=bool(ioc_binding_full_fill),
            )
            self.state.circuit_breaker_close_fill_count += 1
            self.state.circuit_breaker_close_ioc_fill_count += 1
            trace_order = dict(order)
            trace_order["price"] = fill_price
            self._append_fill_trace(
                side,
                trade_idx,
                fill_price,
                fill_qty,
                int(order.get("quote_ts", now_ts)),
                float(order.get("mid_at_quote", fallback_mid)),
                0.0,
                0.0,
                float(order.get("quantity", fill_qty)),
                q_before_fill,
                order=trace_order,
                fee_rate=self.state.taker_fee,
                fill_ts_ms=exchange_ts,
                economic_match_sequence=order.get("_ioc_economic_match_sequence"),
                clock_context={
                    "path": "ioc", "match_ts_ms": int(exchange_ts),
                    "visible_ts_ms": int(order["ioc_terminal_visible_ts"]),
                    "processed_ts_ms": int(now_ts),
                    "match_processed_ts_ms": order["_ioc_first_match_processed_ts_ms"],
                },
            )
            self._append_order_outcome(
                order,
                now_ts,
                "fill",
                "ioc_fill",
                fill_qty,
            )
            self._inventory_lifecycle_on_fill(
                side,
                q_before_fill,
                fill_qty,
                int(now_ts),
                fill_price,
                self.state.taker_fee,
                physical_fill_identity=physical_fill_identity,
                economic_legs=economic_fill_legs,
            )
            if float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE:
                self._record_order_lifecycle_cancel_request(
                    order,
                    int(now_ts),
                    reason="ioc_unfilled_remainder",
                )
                self._record_order_lifecycle_cancel_ack(
                    order,
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=exchange_ts,
                    reason="ioc_unfilled_remainder",
                )
                self._ranked_guard_cancel_requested(
                    order,
                    visibility_ts_ms=int(now_ts),
                    reason="ioc_unfilled_remainder",
                )
                self._ranked_guard_exchange_terminal(
                    order,
                    reason="expired",
                    visibility_ts_ms=int(now_ts),
                    exchange_ts_ms=exchange_ts,
                )
            self._post_cooldown_budget_release_order(
                order,
                now_ts=int(now_ts),
                reason="ioc_terminal",
            )
            self._sync_local_order_lifecycle_repair(int(now_ts))
            if self.state.markout_ema_span_fills > 0:
                self._queue_markout(
                    exchange_ts, fill_price, side == "BUY", {
                        "final_compressed": False,
                    }, fill_qty, int(order["trace_id"])
                )
            if abs(self.state.q) < 1e-10:
                self.state.entry_price = 0.0
            filled = True
            # IOC cancels every unfilled remainder.
            orders.pop(idx)
            self._apply_visible_fill_inventory_state(
                q_before_visible=float(q_before_fill), visibility_ts_ms=int(now_ts),
                filled_buy=side == "BUY", filled_sell=side == "SELL",
            )
            if (
                self.state.main_loop_waiting_request is not None
                and self.state.main_loop_waiting_request.get("order") is order
            ):
                self._complete_synchronous_rest_request(self.state.main_loop_waiting_request, int(now_ts))
        return filled

    def _drain_planned_quote_stop(self) -> bool:
        """Cancel once at the planned stop; inherited orders remain fill-eligible."""
        if self.state.planned_quote_stop_ts_ms > 0 and int(self.state.t) >= self.state.planned_quote_stop_ts_ms:
            if self.state.pending_quote_compute is not None or self.state.active_quote_compute is not None:
                self.state.pre_snapshot_compute_discarded_count += 1
                self.state.pending_quote_compute = None
                self.state.active_quote_compute = None
            if not self.state.planned_quote_stop_triggered:
                self.state.planned_quote_stop_triggered = True
                self.state.planned_quote_stop_trigger_ts_ms = int(self.state.t)
                self.state.planned_shutdown_orders_at_trigger = sum(
                    float(order.get("remaining", 0.0) or 0.0) >= self.state.LOT_SIZE
                    for order in (*self.state.bid_orders, *self.state.ask_orders)
                )
                if self.state.rest_gateway_timing_enabled:
                    self._prepare_rest_gateway_timing(
                        decision_ts=int(self.state.t),
                        requested_slots=tuple(
                            slot
                            for slot, requested in (
                                (
                                    "cancel_buy",
                                    self._has_cancel_request_candidate(self.state.bid_orders),
                                ),
                                (
                                    "cancel_sell",
                                    self._has_cancel_request_candidate(self.state.ask_orders),
                                ),
                            )
                            if requested
                        ),
                    )
                self._request_cancel_all(
                    self.state.bid_orders,
                    int(self.state.t),
                    reason="planned_maintenance",
                )
                self._request_cancel_all(
                    self.state.ask_orders,
                    int(self.state.t),
                    reason="planned_maintenance",
                )
                if self.state.rest_gateway_timing_enabled:
                    self._clear_rest_gateway_timing()
            # Existing orders remain fill-eligible until cancel ACK. The replay
            # continues through the drain interval, but no policy state or new
            # quote is generated after the planned stop.
            return True
        return False
