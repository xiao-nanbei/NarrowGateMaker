"""Replay book owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import Any, Optional


class BookAdapter:
    def _get_l2_rows_at(self, target_ts: int):
        if not self.state.historical_l2 or self.state.l2_ts is None or len(self.state.l2_ts) == 0:
            return None, None, None, None
        idx = self.services.np.searchsorted(self.state.l2_ts, target_ts, side="right") - 1
        if idx < 0:
            return None, None, None, None
        return self.state.l2_bid_px[idx], self.state.l2_bid_qty[idx], self.state.l2_ask_px[idx], self.state.l2_ask_qty[idx]

    def _l2_near_depth(self, bid_qty_row, ask_qty_row):
        if bid_qty_row is None or ask_qty_row is None or self.state.l2_near_levels <= 0:
            return 0.0, 0.0, 0.0
        bid_near = float(self.services.np.clip(bid_qty_row[:self.state.l2_near_levels], 0.0, None).sum())
        ask_near = float(self.services.np.clip(ask_qty_row[:self.state.l2_near_levels], 0.0, None).sum())
        return bid_near, ask_near, bid_near + ask_near

    def _l2_refill_cancel_metrics_at(self,
        target_ts: int,
        *,
        observation_ts: Optional[int] = None,
    ) -> dict[str, dict[str, float]]:
        """Reproduce live ``_current_l2_policy_metrics`` causally.

        Live averages frame-to-frame changes in combined top-20 depth over the
        source-time window ending at the last visible depth. BUY and SELL share flip,
        refill and cancel values; only the side policy consuming them differs.
        The old replay compared one side's near depth with a single snapshot
        ten seconds earlier, which was a different variable by construction.
        """
        empty = {
            "BUY": {
                "l2_quote_flip_rate": 0.0,
                "l2_book_refresh_ratio": 0.0,
                "l2_book_cancel_ratio": 0.0,
                "l2_near_depth_total": 0.0,
            },
            "SELL": {
                "l2_quote_flip_rate": 0.0,
                "l2_book_refresh_ratio": 0.0,
                "l2_book_cancel_ratio": 0.0,
                "l2_near_depth_total": 0.0,
            },
        }
        if (
            not self.state.historical_l2
            or self.state.l2_ts is None
            or self.state.l2_bid_qty is None
            or self.state.l2_ask_qty is None
            or len(self.state.l2_ts) == 0
            or self.state.l2_near_levels <= 0
            or self.state.l2_policy_depth_levels <= 0
        ):
            return empty

        cur_idx = self.services.np.searchsorted(self.state.l2_ts, target_ts, side="right") - 1
        if cur_idx < 0:
            return empty

        # Transport lag changes which frame is visible, not the duration of
        # its history. Preserve the legacy non-message/native path here.
        window_end_ts = (
            int(self.state.l2_ts[cur_idx]) if self.state.exec_message_schedules
            else int(target_ts if observation_ts is None else observation_ts)
        )
        start_ts = window_end_ts - max(0, int(self.state.l2_refill_cancel_lookback_ms))
        start_idx = int(self.services.np.searchsorted(self.state.l2_ts, start_ts, side="left"))
        if start_idx > cur_idx:
            start_idx = cur_idx

        sample_count = 0
        flip_count = 0.0
        refresh_sum = 0.0
        cancel_sum = 0.0
        previous_total: self.services.Optional[float] = None
        previous_bid: self.services.Optional[float] = None
        previous_ask: self.services.Optional[float] = None
        for idx in range(start_idx, cur_idx + 1):
            bid = float(self.state.l2_bid_px[idx, 0])
            ask = float(self.state.l2_ask_px[idx, 0])
            if bid <= 0.0 or ask <= bid:
                continue
            total_depth = float(
                self.services.np.clip(
                    self.state.l2_bid_qty[idx, :self.state.l2_policy_depth_levels], 0.0, None
                ).sum()
                + self.services.np.clip(
                    self.state.l2_ask_qty[idx, :self.state.l2_policy_depth_levels], 0.0, None
                ).sum()
            )
            if previous_total is not None:
                if bid != previous_bid or ask != previous_ask:
                    flip_count += 1.0
                if previous_total > 1e-12:
                    delta = total_depth - previous_total
                    if delta > 0.0:
                        refresh_sum += delta / previous_total
                    elif delta < 0.0:
                        cancel_sum += -delta / previous_total
            previous_total = total_depth
            previous_bid = bid
            previous_ask = ask
            sample_count += 1

        _, _, cur_near_total = self._l2_near_depth(
            self.state.l2_bid_qty[cur_idx], self.state.l2_ask_qty[cur_idx]
        )
        if sample_count <= 0:
            return empty
        shared = {
            "l2_quote_flip_rate": float(flip_count / sample_count),
            "l2_book_refresh_ratio": float(refresh_sum / sample_count),
            "l2_book_cancel_ratio": float(cancel_sum / sample_count),
            "l2_near_depth_total": float(cur_near_total),
        }
        return {
            "BUY": dict(shared),
            "SELL": dict(shared),
        }

    def _l2_visible_queue_ahead(self, side: str, quote_price: float, target_ts: Optional[int] = None) -> Optional[float]:
        if not self.state.historical_l2:
            return None

        if target_ts is None:
            bid_px_row = self.state.cur_l2_bid_px_row
            bid_qty_row = self.state.cur_l2_bid_qty_row
            ask_px_row = self.state.cur_l2_ask_px_row
            ask_qty_row = self.state.cur_l2_ask_qty_row
        else:
            bid_px_row, bid_qty_row, ask_px_row, ask_qty_row = self._get_l2_rows_at(target_ts)

        if bid_px_row is None or ask_px_row is None:
            return None

        quote_tick = self.services._price_to_tick(float(quote_price), self.state.TICK)
        if side == "BUY":
            mask, finite_ticks = self.services._price_tick_mask(bid_px_row, quote_tick, self.state.TICK)
            visible = self.services.np.clip(bid_qty_row[mask], 0.0, None)
            if visible.size == 0:
                # exact L2 只覆盖 top-N。若报价已经在可见 bid 之外，不能把 queue 当成 0；
                # 必须返回 None，让 fallback queue model 接管。
                if finite_ticks.size and quote_tick < int(self.services.np.min(finite_ticks)):
                    return None
        else:
            mask, finite_ticks = self.services._price_tick_mask(ask_px_row, quote_tick, self.state.TICK)
            visible = self.services.np.clip(ask_qty_row[mask], 0.0, None)
            if visible.size == 0:
                # SELL 同理：报价高于可见 ask 最远档时，top-N L2 不足以判断真实排队量。
                if finite_ticks.size and quote_tick > int(self.services.np.max(finite_ticks)):
                    return None
        if visible.size == 0:
            return 0.0
        return float(visible.sum())

    def _day_tag_from_ts(self, target_ts: Optional[int]) -> Optional[str]:
        if target_ts is None:
            return None
        try:
            return self.services.pd.to_datetime(int(target_ts), unit="ms", utc=True).strftime("%Y-%m-%d")
        except Exception:
            return None

    def _queue_reason_bucket(self, order: Optional[dict], side: str, near_depth_total: float) -> str:
        diag = (order or {}).get("quote_diag") or {}
        side_l = str(side).lower()
        adverse = bool(
            diag.get("side_adverse", False)
            or diag.get("adverse_guard", False)
            or (order or {}).get(f"quote_{side_l}_adverse", False)
        )
        markout = bool(
            diag.get("adverse_markout", False)
            or diag.get("defense_markout", False)
            or diag.get("markout_guard", False)
        )
        thin_threshold = max(
            float(self.context.params.get("thin_depth_threshold", 0.0) or 0.0),
            float(self.context.params.get("adverse_thin_depth_threshold", 0.0) or 0.0),
        )
        thin_depth = bool(
            diag.get("adverse_thin_depth", False)
            or diag.get("defense_thin_depth", False)
            or (thin_threshold > 0.0 and 0.0 < near_depth_total < thin_threshold)
        )
        return self.services.reason_bucket_from_flags(
            adverse=adverse,
            markout=markout,
            thin_depth=thin_depth,
        )

    def _queue_regime_multiplier(self,
        side: str,
        dist_from_mid: float,
        near_depth_total: float,
        local_rank: float,
        reason_bucket: str,
        target_ts: Optional[int],
    ) -> float:
        # quote-time 队列厚薄校准：按 side / quote distance / causal rank / reason bucket 查表。
        # 去重后的 exact L2 仍可能比 live 成交选择更厚，所以该 multiplier 同时作用
        # exact visible queue 和 fallback queue；默认 no-op，只有显式校准表会改变它。
        if not self.state.queue_regime_lookup:
            return 1.0
        day_tag = self._day_tag_from_ts(target_ts)
        if not day_tag:
            return 1.0
        return self.services.lookup_queue_regime_multiplier(
            self.state.queue_regime_lookup,
            day=day_tag,
            side=side,
            dist_usd=dist_from_mid,
            near_depth_total=near_depth_total,
            local_rank=local_rank,
            reason_bucket=reason_bucket,
        )

    def _local_rank_at_ts(self, target_ts: Optional[int], window_ms: int = 120_000) -> float:
        # 因果 local rank，用于 live/replay 同口径对齐。不要替换成 trace 的前后窗口 rank。
        if target_ts is None or len(self.state.trade_ts) == 0:
            return 0.5
        left = int(self.services.np.searchsorted(self.state.trade_ts, int(target_ts) - int(window_ms), side="left"))
        right = int(self.services.np.searchsorted(self.state.trade_ts, int(target_ts), side="right"))
        if right <= left:
            return 0.5
        window = self.state.trade_price[left:right]
        if len(window) == 0:
            return 0.5
        lo = float(self.services.np.min(window))
        hi = float(self.services.np.max(window))
        if not self.services.np.isfinite(lo) or not self.services.np.isfinite(hi) or hi - lo <= self.state.TICK:
            return 0.5
        cur = float(window[-1])
        return float(max(0.0, min(1.0, (cur - lo) / (hi - lo))))

    def _queue_deplete_multiplier(self, side: str, target_ts: Optional[int]) -> float:
        # fill-time 校准：缩放当前 taker trade 对队列的有效消耗量。
        # 用来让“哪些 SELL 会被扫到”更接近 live，而不是调总 fill probability。
        if not self.state.queue_deplete_lookup:
            return self.state.queue_deplete_base_mult
        day_tag = self._day_tag_from_ts(target_ts)
        if not day_tag:
            return self.state.queue_deplete_base_mult
        return self.state.queue_deplete_base_mult * self.services.lookup_queue_deplete_multiplier(
            self.state.queue_deplete_lookup,
            day=day_tag,
            side=side,
            local_rank=self._local_rank_at_ts(target_ts),
        )

    def _queue_mo_multiplier(self, order: Optional[dict], side: str, target_ts: Optional[int]) -> float:
        # 实验性 order-level 校准：按 side-specific markout EMA 调整初始 fallback queue。
        # 如果某个 OOS 窗口发现它会压低总 fills，生成校准表时把 min=max=1.0 即可 no-op。
        if not self.state.queue_mo_lookup:
            return 1.0
        day_tag = self._day_tag_from_ts(target_ts)
        if not day_tag:
            return 1.0
        diag = (order or {}).get("quote_diag") or {}
        mo_key = "mo_ema_bid" if side == "BUY" else "mo_ema_ask"
        try:
            markout_ema = float(diag.get(mo_key, 0.0) or 0.0)
        except Exception:
            markout_ema = 0.0
        return self.services.lookup_queue_mo_multiplier(
            self.state.queue_mo_lookup,
            day=day_tag,
            side=side,
            markout_ema=markout_ema,
        )

    def _queue_inventory_multiplier(self, side: str, inventory_at_quote: float) -> tuple[float, bool]:
        """Replay-only queue multiplier by side and quote-time inventory state.

        中文说明：这是 live/replay 机制对齐工具，不是策略 alpha。它只在订单
        激活并初始化 queue ahead 时生效，用于校准“某侧在加仓/减仓状态下更容易
        被真实成交”的选择偏差。默认四个 multiplier 都是 1.0。
        """
        inv = float(inventory_at_quote or 0.0)
        exposure_increasing = (side == "BUY" and inv >= 0.0) or (side == "SELL" and inv <= 0.0)
        if side == "BUY":
            return (
                self.state.queue_ahead_buy_exposure_mult if exposure_increasing else self.state.queue_ahead_buy_reducing_mult,
                exposure_increasing,
            )
        return (
            self.state.queue_ahead_sell_exposure_mult if exposure_increasing else self.state.queue_ahead_sell_reducing_mult,
            exposure_increasing,
        )

    def _estimate_queue_ahead(self, side: str, quote_price: float, dist_from_mid: float,
                              target_ts: Optional[int] = None, regime_queue_mult: float = 1.0) -> float:
        """Estimate queue ahead at quote placement."""
        visible_l2 = self._l2_visible_queue_ahead(side, quote_price, target_ts=target_ts)
        if visible_l2 is not None:
            # Live/replay calibration may discover that visible exact-level L2
            # is still too thick after duplicate-price cleanup.  Apply the same
            # quote-time queue multiplier here so matched live-only fills can be
            # represented as a thinner initial queue instead of an absurdly large
            # fill-time depletion multiplier.
            queue_mult = self.state.queue_ahead_base_mult
            if self.services.np.isfinite(regime_queue_mult) and regime_queue_mult > 0.0:
                queue_mult *= float(regime_queue_mult)
            visible_l2 *= float(queue_mult)
            return max(visible_l2, 0.0)

        # 只有 L2 看不到目标价位时才进入 fallback queue。
        q_base = self.state.queue_base
        q_decay_local = self.state.queue_decay
        if target_ts is not None and len(self.state.trade_ts_seed) > 0:
            idx = self.services.np.searchsorted(self.state.trade_ts_seed, target_ts, side="right") - 1
            if idx >= 0:
                q_base = self.state.queue_base_arr[idx]
                q_decay_local = self.state.queue_decay_arr[idx]
        q_est = q_base * self.services.math.exp(-q_decay_local * dist_from_mid)
        queue_mult = self.state.queue_ahead_base_mult
        if self.services.np.isfinite(regime_queue_mult) and regime_queue_mult > 0.0:
            queue_mult *= float(regime_queue_mult)
        q_est *= float(queue_mult)
        return max(q_est, 0.0)

    def _active_order_queue_seed(self,
        side: str,
        quote_price: float,
        activate_ts_ms: int,
    ) -> Optional[float]:

        if self.context.active_order_queue_data is None:
            return None
        self.state.active_order_queue_lookup_count += 1
        seed = self.context.active_order_queue_data.lookup_seed(
            side=side,
            price=float(quote_price),
            activate_ts_ms=int(activate_ts_ms),
        )
        failure = ""
        if seed is None:
            self.state.active_order_queue_missing_count += 1
            failure = "missing"
        elif not seed.strict_usable:
            self.state.active_order_queue_unusable_count += 1
            failure = (
                f"{seed.status}:{seed.reason}"
                + (":ambiguous" if seed.ambiguous else "")
            )
        else:
            if seed.status == "exact":
                self.state.active_order_queue_exact_count += 1
            else:
                self.state.active_order_queue_known_zero_count += 1
            return float(seed.quantity)

        if self.state.active_order_queue_missing_trace_unbounded or (
            self.state.active_order_queue_missing_trace_max > 0
            and len(self.state.active_order_queue_missing_trace)
            < self.state.active_order_queue_missing_trace_max
        ):
            self.state.active_order_queue_missing_trace.append(
                {
                    "side": str(side),
                    "price": float(quote_price),
                    "price_tick": self.services._price_to_tick(float(quote_price), self.state.TICK),
                    "activate_ts_ms": int(activate_ts_ms),
                    "failure": failure,
                }
            )
        if self.state.active_order_queue_mode == "strict_sparse":
            raise self.services.ActiveOrderQueueCoverageError(
                "strict sparse queue seed unavailable for "
                f"{side} {quote_price} at {activate_ts_ms}: {failure}"
            )
        return None

    def _exchange_book_queue_seed(self,
        order: dict,
        side: str,
        activate_ts_ms: int,
    ) -> Optional[float]:
        """Capture a native queue seed preceding activation in causal order."""


        if self.state.exchange_book_scheduler is None:
            return None
        if "_exchange_book_queue_seed_status" in order:
            value = order.get("_exchange_book_queue_seed_qty")
            return (
                float(value)
                if value is not None and self.services.np.isfinite(float(value))
                else None
            )

        price_tick = self.services._order_price_tick(order, self.state.TICK)
        causal = order.get("zero_service_execution_context")
        causal_before_activation = bool(
            causal is not None
            and causal["activation_ts_ms"] == int(activate_ts_ms)
            and causal["side"] == side and causal["price_tick"] == price_tick
        )
        lookup = (
            causal["queue"] if causal_before_activation
            else self.state.exchange_book_scheduler.lookup_strictly_before(
                side, price_tick, int(activate_ts_ms) * 1_000_000)
        )
        strict_before_activation = (
            int(lookup.asof_exchange_ts_ns)
            < int(activate_ts_ms) * 1_000_000
            or (causal_before_activation
                and int(lookup.asof_exchange_ts_ns) <= int(activate_ts_ms) * 1_000_000)
        )
        self.state.exchange_book_queue_lookup_count += 1
        seed_status = (
            str(lookup.status)
            if strict_before_activation
            else "ambiguous"
        )
        seed_reason = (
            str(lookup.reason)
            if strict_before_activation
            else "same_ms_state_not_strictly_before_activation"
        )
        seed_quantity = (
            lookup.quantity if strict_before_activation else None
        )
        order["_exchange_book_queue_seed_status"] = seed_status
        order["_exchange_book_queue_seed_reason"] = seed_reason
        order["_exchange_book_queue_seed_qty"] = seed_quantity
        order["exchange_book_queue_asof_ts_ns"] = int(
            lookup.asof_exchange_ts_ns
        )
        order["exchange_book_queue_segment_id"] = int(lookup.segment_id)
        order["exchange_book_queue_path_valid"] = bool(
            lookup.strict_usable and strict_before_activation
        )
        order["exchange_book_queue_price_tick"] = int(price_tick)
        order["exchange_book_queue_ambiguous"] = False
        order["exchange_book_queue_trade_since_update"] = 0.0
        if (
            lookup.status == "exact"
            and lookup.strict_usable
            and strict_before_activation
        ):
            self.state.exchange_book_queue_exact_count += 1
        elif (
            lookup.status == "known_zero"
            and lookup.strict_usable
            and strict_before_activation
        ):
            self.state.exchange_book_queue_known_zero_count += 1
        else:
            self.state.exchange_book_queue_missing_count += 1
            if self.state.active_order_queue_missing_trace_unbounded or (
                self.state.active_order_queue_missing_trace_max > 0
                and len(self.state.exchange_book_queue_missing_trace)
                < self.state.active_order_queue_missing_trace_max
            ):
                self.state.exchange_book_queue_missing_trace.append(
                    {
                        "order_id": int(order.get("trace_id", -1)),
                        "side": str(side),
                        "price": float(order["price"]),
                        "price_tick": int(price_tick),
                        "activate_ts_ms": int(activate_ts_ms),
                        "status": seed_status,
                        "reason": seed_reason,
                        "asof_exchange_ts_ns": int(
                            lookup.asof_exchange_ts_ns
                        ),
                        "segment_id": int(lookup.segment_id),
                        "snapshot_min_tick": (
                            int(lookup.snapshot_min_tick)
                            if lookup.snapshot_min_tick is not None
                            else None
                        ),
                        "snapshot_max_tick": (
                            int(lookup.snapshot_max_tick)
                            if lookup.snapshot_max_tick is not None
                            else None
                        ),
                    }
                )
            # Formal native mode is strict about source completeness and
            # sequence continuity, but an isolated price outside the native
            # snapshot range is support loss rather than a reason to discard
            # the whole UTC day. Keep the operational baseline trajectory on
            # its delayed-book fallback, tag the simulator source below, and
            # make the order ineligible for native action evidence.
            return None
        return float(seed_quantity or 0.0)

    def _l2_exact_visible_at_index(self,
        side: str,
        quote_price: float,
        target_idx: int,
    ) -> Optional[float]:
        """Return exact-level public quantity, or None outside visible top-N."""

        if not self.state.historical_l2 or target_idx < 0 or target_idx >= len(self.state.l2_ts):
            return None
        quote_tick = self.services._price_to_tick(float(quote_price), self.state.TICK)
        if side == "BUY":
            px_row = self.state.l2_bid_px[target_idx]
            qty_row = self.state.l2_bid_qty[target_idx]
            mask, finite_ticks = self.services._price_tick_mask(px_row, quote_tick, self.state.TICK)
            if not self.services.np.any(mask):
                if (
                    finite_ticks.size
                    and quote_tick < int(self.services.np.min(finite_ticks))
                ):
                    return None
                return 0.0
        else:
            px_row = self.state.l2_ask_px[target_idx]
            qty_row = self.state.l2_ask_qty[target_idx]
            mask, finite_ticks = self.services._price_tick_mask(px_row, quote_tick, self.state.TICK)
            if not self.services.np.any(mask):
                if (
                    finite_ticks.size
                    and quote_tick > int(self.services.np.max(finite_ticks))
                ):
                    return None
                return 0.0
        return float(self.services.np.clip(qty_row[mask], 0.0, None).sum())

    def _init_l2_cancel_ahead_state(self,
        order: dict,
        side: str,
        activation_ts: int,
    ) -> None:
        if not self.state.queue_l2_cancel_ahead_enabled or not self.state.historical_l2:
            return
        idx = int(self.services.np.searchsorted(self.state.l2_ts, int(activation_ts), side="right") - 1)
        order["queue_l2_seen_idx"] = idx
        visible = self._l2_exact_visible_at_index(side, float(order["price"]), idx)
        order["queue_visible_prev"] = -1.0 if visible is None else float(visible)
        order["queue_trade_since_l2"] = 0.0

    def _apply_l2_cancel_ahead(self,
        orders: list[dict],
        side: str,
        current_l2_idx: int,
    ) -> None:
        """Causally reduce queue ahead using unprinted exact-level L2 decreases.

        Public quantity increases are behind an already resting simulated order.
        For decreases not explained by same-price trades, the fraction assigned
        ahead follows the order's current position within visible queue.
        """


        if not self.state.queue_l2_cancel_ahead_enabled or current_l2_idx < 0:
            return
        for order in orders:
            if order.get("state") == self.state.ORDER_PENDING_NEW:
                continue
            seen_idx = int(order.get("queue_l2_seen_idx", -1))
            if seen_idx < 0:
                self._init_l2_cancel_ahead_state(order, side, int(self.state.l2_ts[current_l2_idx]))
                continue
            if current_l2_idx <= seen_idx:
                continue
            trade_since = max(0.0, float(order.get("queue_trade_since_l2", 0.0) or 0.0))
            for idx in range(seen_idx + 1, current_l2_idx + 1):
                current_visible = self._l2_exact_visible_at_index(
                    side,
                    float(order["price"]),
                    idx,
                )
                previous_visible = float(order.get("queue_visible_prev", -1.0) or 0.0)
                if current_visible is None:
                    order["queue_visible_prev"] = -1.0
                    trade_since = 0.0
                    continue
                if previous_visible >= 0.0:
                    explained_trade = min(previous_visible, trade_since)
                    cancellation = max(
                        0.0,
                        previous_visible - float(current_visible) - explained_trade,
                    )
                    ahead = max(0.0, float(order.get("queue_left", 0.0) or 0.0))
                    public_after_trade = max(0.0, previous_visible - explained_trade)
                    behind = max(0.0, public_after_trade - ahead)
                    denominator = ahead + behind
                    ahead_probability = ahead / denominator if denominator > 0.0 else 0.0
                    removed = min(ahead, cancellation * ahead_probability)
                    if removed > 0.0:
                        order["queue_left"] = self.services.subtract_lot_quantity(ahead, removed, self.state.LOT_SIZE)
                        self.state.queue_l2_cancel_ahead_event_count += 1
                        self.state.queue_l2_cancel_ahead_qty += removed
                        if side == "BUY":
                            self.state.queue_l2_cancel_ahead_bid_event_count += 1
                        else:
                            self.state.queue_l2_cancel_ahead_ask_event_count += 1
                order["queue_visible_prev"] = float(current_visible)
                trade_since = 0.0
            order["queue_trade_since_l2"] = trade_since
            order["queue_l2_seen_idx"] = int(current_l2_idx)

    def _record_same_price_trade_for_l2_queue(self,
        orders: list[dict],
        trade_price_now: float,
        trade_qty_now: float,
    ) -> None:
        if not self.state.queue_l2_cancel_ahead_enabled or trade_qty_now <= 0.0:
            return
        trade_tick = self.services._price_to_tick(float(trade_price_now), self.state.TICK)
        for order in orders:
            if order.get("state") not in (self.state.ORDER_OPEN, self.state.ORDER_PENDING_CANCEL):
                continue
            if self.services._order_price_tick(order, self.state.TICK) != trade_tick:
                continue
            order["queue_trade_since_l2"] = max(
                0.0,
                float(order.get("queue_trade_since_l2", 0.0) or 0.0),
            ) + float(trade_qty_now)

    def _new_fill_eligible(self, side: str, target_ts: Optional[int] = None) -> bool:
        side_prob = 1.0
        if target_ts is not None and len(self.state.trade_ts_seed) > 0:
            idx = self.services.np.searchsorted(self.state.trade_ts_seed, target_ts, side="right") - 1
            if idx >= 0:
                side_prob = self.state.buy_fill_prob_arr[idx] if side == "BUY" else self.state.sell_fill_prob_arr[idx]
        side_prob *= self.state.maker_fill_prob
        side_prob = max(0.0, min(1.0, side_prob))
        if side_prob >= 1.0:
            return True
        if side_prob <= 0.0:
            return False
        return bool(self.state.fill_rng.random() <= side_prob)

    def _bbo_query_unavailable(self, bbo_cursor: int, l2_cursor: int = -1) -> bool:
        if bbo_cursor < 0 or self.state.bbo_usable is None or self.state.bbo_usable[bbo_cursor]:
            return False
        # A distinct newer real depth observation may restore the book.  An
        # older/equal depth view cannot erase a newer explicit invalidation.
        return not (l2_cursor >= 0
                    and self.state.l2_observation_us[l2_cursor] > self.state.bbo_observation_us[bbo_cursor]
                    and self.state.l2_bid_px[l2_cursor, 0] > 0.0
                    and self.state.l2_ask_px[l2_cursor, 0] > self.state.l2_bid_px[l2_cursor, 0])

    def _book_snapshot_at(self, target_ts: int, fallback_mid: float):
        best_bid = self.state.inferred_best_bid
        best_ask = self.state.inferred_best_ask
        bid_qty = 0.0
        ask_qty = 0.0
        source = "inferred_trade"

        bbo_idx = (int(self.services.np.searchsorted(self.state.bbo_ts, target_ts, side="right")) - 1
                   if self.state.historical_bbo and self.state.bbo_ts is not None else -1)
        l2_idx = (int(self.services.np.searchsorted(self.state.l2_ts, target_ts, side="right")) - 1
                  if self.state.historical_l2 and self.state.l2_ts is not None else -1)
        if self._bbo_query_unavailable(bbo_idx, l2_idx):
            return 0.0, 0.0, 0.0, 0.0, 0.0, "unavailable_book"
        if bbo_idx >= 0 and self.state.bbo_usable[bbo_idx]:
            idx = bbo_idx
            best_bid = self.state.bbo_best_bid[idx]
            best_ask = self.state.bbo_best_ask[idx]
            bid_qty = self.state.bbo_bid_qty[idx]
            ask_qty = self.state.bbo_ask_qty[idx]
            source = self.context.bbo_data.source
        elif l2_idx >= 0 and (not self.state.historical_bbo or bbo_idx >= 0):
            idx = l2_idx
            best_bid = self.state.l2_bid_px[idx, 0]
            best_ask = self.state.l2_ask_px[idx, 0]
            bid_qty = self.state.l2_bid_qty[idx, 0]
            ask_qty = self.state.l2_ask_qty[idx, 0]
            source = self.context.l2_data.source

        mid_ref = fallback_mid
        if best_bid > 0.0 and best_ask > best_bid:
            mid_ref = 0.5 * (best_bid + best_ask)
        return best_bid, best_ask, bid_qty, ask_qty, mid_ref, source

    def _execution_admission_book_at(self, target_ts: int, fallback_mid: float, order=None):
        """Execution-only top before activation, never a policy input.

        The existing scheduler is advanced by the lifecycle phase. Do not
        advance/rewind it here. A touched same-time top or unavailable segment
        remains unknown unless a zero-service request captured that already
        consumed state as its explicit causal predecessor.
        """
        causal = order.get("zero_service_execution_context") if order is not None else None
        if causal is not None and causal["activation_ts_ms"] == int(target_ts):
            return causal["book"], dict(causal["evidence"])
        scheduler = self.state.exchange_book_scheduler
        if scheduler is None:
            return self._book_snapshot_at(target_ts, fallback_mid), None
        bids, asks = scheduler.top_levels(1)
        evidence = {"source": "exchange_book_strict_before_activation",
                    "activation_ts_ms": int(target_ts), "valid": False,
                    "bid": None, "ask": None, "asof_ns": None,
                    "segment": int(scheduler.segment_id), "reason": "missing_top"}
        if bids and asks:
            bid_tick, bid_qty = bids[0]
            ask_tick, ask_qty = asks[0]
            bid = scheduler.lookup_strictly_before("BUY", int(bid_tick), int(target_ts)*1_000_000)
            ask = scheduler.lookup_strictly_before("SELL", int(ask_tick), int(target_ts)*1_000_000)
            evidence.update(bid=float(bid_tick)*self.state.TICK,
                            ask=float(ask_tick)*self.state.TICK,
                            asof_ns=max(bid.asof_exchange_ts_ns, ask.asof_exchange_ts_ns),
                            reason=f"{bid.reason}/{ask.reason}")
            if (bid.strict_usable and ask.strict_usable
                    and bid.status == ask.status == "exact"
                    and evidence["asof_ns"] < int(target_ts)*1_000_000
                    and 0 < bid_tick < ask_tick and bid_qty > 0 and ask_qty > 0):
                evidence["valid"] = True
                mid = .5*(evidence["bid"]+evidence["ask"])
                return (evidence["bid"], evidence["ask"], float(bid.quantity),
                        float(ask.quantity), mid, evidence["source"]), evidence
        return (0., 0., 0., 0., 0., "unavailable_book"), evidence

    def _capture_zero_service_execution_context(self, order, request, now_ts: int) -> None:
        # The FIFO request occurs after the already consumed book events.
        # Zero service preserves that happens-before edge even at equal ms.
        # Capture at request start, never query a later book retroactively.
        # Unconsumed source events and invalid books remain unavailable.
        if not (getattr(self.state, "main_loop_no_poll_wait", False)
                or self.context.params.get("zero_service_causal_order", False)):
            return
        scheduler = self.state.exchange_book_scheduler
        if scheduler is None or int(request["effective_ms"]) != 0:
            return
        timestamp_ns = int(now_ts) * 1_000_000
        if scheduler.boundary_exchange_ts_ns > timestamp_ns:
            raise RuntimeError("zero-service request precedes execution-book state")
        bids, asks = scheduler.top_levels(1)
        book = (0., 0., 0., 0., 0., "unavailable_book")
        evidence = dict(source="exchange_book_causal_before_zero_service_activation",
            activation_ts_ms=int(now_ts), request_sequence=int(request["sequence"]),
            valid=False, bid=None, ask=None, asof_ns=None,
            segment=int(scheduler.segment_id), reason="missing_top",
            order="consumed_book_then_fifo_request_then_zero_service_activation")
        if bids and asks:
            bid_tick, bid_qty = bids[0]
            ask_tick, ask_qty = asks[0]
            bid = scheduler.lookup("BUY", int(bid_tick))
            ask = scheduler.lookup("SELL", int(ask_tick))
            evidence.update(bid=float(bid_tick) * self.state.TICK,
                ask=float(ask_tick) * self.state.TICK,
                asof_ns=max(bid.asof_exchange_ts_ns, ask.asof_exchange_ts_ns),
                reason=f"{bid.reason}/{ask.reason}")
            if (bid.strict_usable and ask.strict_usable
                    and bid.status == ask.status == "exact"
                    and evidence["asof_ns"] <= timestamp_ns
                    and 0 < bid_tick < ask_tick and bid_qty > 0 and ask_qty > 0):
                evidence["valid"] = True
                book = (evidence["bid"], evidence["ask"], float(bid.quantity),
                    float(ask.quantity), .5 * (evidence["bid"] + evidence["ask"]),
                    evidence["source"])
        price_tick = self.services._order_price_tick(order, self.state.TICK)
        queue = scheduler.lookup(order["side"], price_tick)
        if queue.asof_exchange_ts_ns > timestamp_ns:
            raise RuntimeError("zero-service queue snapshot contains future state")
        order["zero_service_execution_context"] = dict(
            activation_ts_ms=int(now_ts), request_sequence=int(request["sequence"]),
            side=order["side"], price_tick=price_tick,
            book=book, evidence=evidence, queue=queue)

    def _fixed_horizon_book_mid(self, target_ts: int) -> tuple[float, int, str, bool]:
        """Return the latest exchange-time BBO/L2 midpoint at a fixed target.

        The observation timestamp is the actual source event, never the
        theoretical target and never a later trade print.  A target beyond all
        available book data is right-censored.
        """

        candidates: list[tuple[int, float, str]] = []
        source_ends: list[int] = []
        if self._book_snapshot_at(int(target_ts), 0.0)[5] == "unavailable_book":
            return self.services.math.nan, 0, "unavailable_book", True
        if self.state.historical_bbo and self.state.bbo_ts is not None and len(self.state.bbo_ts) > 0:
            source_ends.append(int(self.state.bbo_ts[-1]))
            idx = int(self.services.np.searchsorted(self.state.bbo_ts, int(target_ts), side="right") - 1)
            if idx >= 0 and self.state.bbo_usable[idx]:
                bid = float(self.state.bbo_best_bid[idx])
                ask = float(self.state.bbo_best_ask[idx])
                if bid > 0.0 and ask > bid:
                    candidates.append(
                        (int(self.state.bbo_ts[idx]), 0.5 * (bid + ask), str(self.context.bbo_data.source))
                    )
        if self.state.historical_l2 and self.state.l2_ts is not None and len(self.state.l2_ts) > 0:
            source_ends.append(int(self.state.l2_ts[-1]))
            idx = int(self.services.np.searchsorted(self.state.l2_ts, int(target_ts), side="right") - 1)
            if idx >= 0:
                bid = float(self.state.l2_bid_px[idx, 0])
                ask = float(self.state.l2_ask_px[idx, 0])
                if bid > 0.0 and ask > bid:
                    candidates.append(
                        (int(self.state.l2_ts[idx]), 0.5 * (bid + ask), str(self.context.l2_data.source))
                    )
        if not candidates:
            return self.services.math.nan, 0, "missing_book", True
        observed_ts, observed_mid, source = max(candidates, key=lambda row: row[0])
        censored = not source_ends or int(target_ts) > max(source_ends)
        return float(observed_mid), int(observed_ts), str(source), bool(censored)

    def _current_book_age_ms(self,
        now_ts: int,
        *,
        bbo_cursor: Optional[int] = None,
        l2_cursor: Optional[int] = None,
    ):
        if bbo_cursor is None:
            bbo_cursor = self.state.bbo_idx
        if l2_cursor is None:
            l2_cursor = self.state.l2_idx
        if self._bbo_query_unavailable(int(bbo_cursor), int(l2_cursor)):
            return None
        ages = []
        if (
            self.state.historical_bbo
            and self.state.bbo_ts is not None
            and bbo_cursor >= 0
            and self.state.bbo_usable[bbo_cursor]
            and self.state.bbo_ts[bbo_cursor] <= now_ts
        ):
            ages.append((int(now_ts) * 1000 - int(self.state.bbo_observation_us[bbo_cursor])) / 1000.0)
        if (
            self.state.historical_l2
            and self.state.l2_ts is not None
            and l2_cursor >= 0
            and self.state.l2_ts[l2_cursor] <= now_ts
        ):
            ages.append((int(now_ts) * 1000 - int(self.state.l2_observation_us[l2_cursor])) / 1000.0)
        if not ages:
            return None
        return min(ages)

    def _message_clock_state(self, feed: str, now_ts_ms: int) -> tuple[int, int, int]:
        schedule = self.state.exec_message_schedules[feed]
        idx = schedule.latest_visible_index(int(now_ts_ms) * 1_000_000)
        if feed == "variance":
            # Variance arrays retain 1s left labels. Even a malformed/early
            # delivery row cannot reveal a close before its second ends.
            completed_idx = int(self.services.np.searchsorted(
                schedule.exchange_ns_for_channel(),
                (int(now_ts_ms) - 1_000) * 1_000_000, side="right",
            )) - 1
            idx = min(idx, completed_idx)
        if idx < 0:
            return -1, 0, 0
        return (
            idx,
            int(schedule.exchange_ns_for_channel()[idx]),
            int(self.state.exec_message_payload[feed].get(
                "source_feature_ready_ts_ns", schedule.ready_ns_for_channel(),
            )[idx]),
        )

    def _quote_schedule_sources(self) -> tuple[tuple[str, str], ...]:
        # These are delivered inputs consumed by the existing quote path,
        # never the exchange tape used solely for matching.
        return (("depth", "BOOK_VIEW"), ("bbo", "POST_ONLY_BOOK_VIEW"),
                ("variance", "BAR_OR_FEATURE_VIEW"),
                ("prediction", "PREDICTION_VIEW"))

    def _quote_schedule_publish(self, now_ms: int) -> None:
        ledger = self.state.quote_schedule
        if ledger is None:
            return
        for feed, domain in self._quote_schedule_sources():
            index, version, _ = self._quote_schedule_position(feed, now_ms)
            if index >= 0:
                # Absolute delivery watermarks survive input-window compaction;
                # equal-ready rows are one already-visible batch, not fake ns.
                if version > ledger.published.get(domain, -1):
                    ledger.publish(domain, version)
        valid = [(side, f"{order['trace_id']}:{int(self._replace_throttle_params(side, self.state.q)[2])}")
                 for side, orders in (("BUY", self.state.bid_orders), ("SELL", self.state.ask_orders))
                 for order in orders if order.get("state") == self.state.ORDER_OPEN]
        ledger.expire(now_ms * 1_000_000, valid)
        deadlines = self._quote_schedule_risk_deadlines()
        predicates = tuple(due > now_ms for due in deadlines)
        if predicates != getattr(self.state, "quote_schedule_risk_predicates", None):
            ledger.change("RISK_VIEW")
            self.state.quote_schedule_risk_predicates = predicates
        self.state.quote_schedule_visible_ms = int(now_ms)

    def _quote_schedule_risk_deadlines(self) -> tuple[float, ...]:
        return (
            self.state.last_buy_fill_ts + self.state.last_buy_fill_cooldown_ms,
            self.state.last_sell_fill_ts + self.state.last_sell_fill_cooldown_ms,
            self.state.consecutive_loss_cooldown.cooldown_until_ms,
            self.state.mo_pause_until_bid_ms, self.state.mo_pause_until_ask_ms,
            self.state.sync_adjust_degrade_until_ms,
        )

    def _quote_schedule_next_visible(self, now_ms: int) -> int | None:
        if self.state.quote_schedule is None:
            return None
        candidates = []
        for feed, _ in self._quote_schedule_sources():
            _, _, due = self._quote_schedule_position(feed, now_ms)
            if due is not None:
                candidates.append(due)
        candidates.extend((due + 999_999) // 1_000_000
                          for due, _, _ in self.state.quote_schedule.deadlines.values())
        candidates.extend(int(self.services.math.ceil(due)) for due in self._quote_schedule_risk_deadlines() if due > now_ms)
        return min(candidates) if candidates else None

    def _quote_schedule_position(self, feed: str, now_ms: int) -> tuple[int, int, int | None]:
        if self.state.exec_message_schedules:
            if feed not in self.state.exec_message_schedules:
                return -1, -1, None
            schedule = self.state.exec_message_schedules[feed]
            ready = schedule.ready_ns_for_channel()
            index = self._message_clock_state(feed, now_ms)[0]
            following = index + 1
            due = self.context.strict_ready_ms(int(ready[following])) if following < len(ready) else None
            if due is not None and feed == "variance":
                due = max(due, int(schedule.exchange_ns_for_channel()[following]) // 1_000_000 + 1000)
            return index, int(ready[index]) if index >= 0 else -1, due
        # Existing source-time scenario: preserve the actual <= book lookup,
        # completed one-second variance and entry-rounded prediction cutoff.
        # Never manufacture packet delays or draw a new visibility sample.
        values = {"depth": self.state.l2_ts, "bbo": self.state.bbo_ts,
                  "variance": self.context.var_ts_ms, "prediction": getattr(self.state, "ml_ts", ())}[feed]
        if values is None or not len(values):
            return -1, -1, None
        offset = 1000 if feed == "variance" else 0
        cutoff = now_ms // 1000 * 1000 if feed == "prediction" else now_ms - offset
        index = int(self.services.np.searchsorted(values, cutoff, side="right")) - 1
        following = index + 1
        due = int(values[following]) + offset if following < len(values) else None
        if due is not None and feed == "prediction":
            due = (due + 999) // 1000 * 1000
        version = (int(values[index]) + offset) * 1_000_000 if index >= 0 else -1
        return index, version, due

    def _quote_schedule_read(self, feed: str, domain: str, at_ms: int) -> None:
        ledger = self.state.quote_schedule
        if ledger is None or ledger.inflight is None:
            return
        index, version, _ = self._quote_schedule_position(feed, at_ms)
        if index >= 0:
            ledger.read(ledger.inflight.sequence, domain, version, ledger.inflight.sides)

    def _decision_visible_book(self,
        now_ts: int,
        fallback_mid: float,
        *,
        advance_visibility_state: bool = True,
    ) -> dict[str, Any]:
        """Return the receive-time-visible book without changing queue truth."""
        paired_state = (
            self.services._paired_exec_book_visibility_state(
                int(now_ts),
                timestamps_ms=self.state.exec_book_visibility_paired_ts_ms,
                book_delays_ms=self.state.exec_book_visibility_paired_delay_ms,
                depth_delays_ms=self.state.exec_depth_visibility_paired_delay_ms,
                trade_delays_ms=self.state.exec_trade_visibility_paired_delay_ms,
                observed_mid=self.state.exec_book_visibility_paired_mid,
                max_gap_ms=self.state.exec_book_visibility_paired_max_gap_ms,
            )
            if self.state.exec_book_visibility_mode == "paired" and not self.state.use_bar_pricing
            else None
        )
        sampled_joint_state = (
            self.services._sampled_joint_exec_book_visibility_state(
                int(now_ts),
                seed=self.state.exec_book_visibility_delay_seed,
                book_delays_ms=self.state.exec_book_visibility_paired_delay_ms,
                depth_delays_ms=self.state.exec_depth_visibility_paired_delay_ms,
                trade_delays_ms=self.state.exec_trade_visibility_paired_delay_ms,
            )
            if self.state.exec_book_visibility_mode == "sampled_joint" and not self.state.use_bar_pricing
            else None
        )
        source_stratified_state = (
            self.services._source_stratified_exec_visibility_state(
                int(now_ts),
                seed=self.state.exec_book_visibility_delay_seed,
                simulator=self.state.exec_source_profile,
                market_id=self.state.exec_source_profile_market_id,
                transport=self.state.exec_source_profile_transport,
            )
            if self.state.exec_book_visibility_mode == "profile_source_stratified"
            and not self.state.use_bar_pricing
            else None
        )
        if paired_state is not None:
            delay_ms, depth_delay_ms, trade_delay_ms, paired_mid = paired_state
        elif sampled_joint_state is not None:
            delay_ms, depth_delay_ms, trade_delay_ms = sampled_joint_state
            paired_mid = float("nan")
        elif source_stratified_state is not None:
            delay_ms, depth_delay_ms, trade_delay_ms = source_stratified_state
            paired_mid = float("nan")
        else:
            delay_ms = (
                self.services._exec_book_visibility_delay_ms(
                    int(now_ts),
                    mean_ms=self.state.exec_book_visibility_delay_mean_ms,
                    jitter_ms=self.state.exec_book_visibility_delay_jitter_ms,
                    seed=self.state.exec_book_visibility_delay_seed,
                    samples_ms=self.state.exec_book_visibility_delay_samples_ms,
                )
                if self.state.exec_book_visibility_delay_enabled
                and not self.state.use_bar_pricing
                and not self.state.exec_message_schedules
                else 0
            )
            depth_delay_ms = delay_ms
            trade_delay_ms = delay_ms
            paired_mid = float("nan")
        visible_ts = int(now_ts) - int(delay_ms)
        visible_l2_ts = (
            int(now_ts)
            - int(depth_delay_ms)
            + self.state.exec_depth_visibility_source_offset_ms
        )
        visible_trade_ts = int(now_ts) - int(trade_delay_ms)
        message_clock_states = (
            {
                feed: self._message_clock_state(feed, int(now_ts))
                for feed in ("bbo", "depth", "trade")
            }
            if self.state.exec_message_schedules
            else {}
        )
        if message_clock_states:
            visible_ts = message_clock_states["bbo"][1] // 1_000_000
            visible_l2_ts = message_clock_states["depth"][1] // 1_000_000
            visible_trade_ts = message_clock_states["trade"][1] // 1_000_000
            # Age is descriptive only; packet delivery was assigned once in
            # nanoseconds and is never reconstructed as now minus a new draw.
            delay_ms = max(0, int(now_ts) - visible_ts)
            depth_delay_ms = max(0, int(now_ts) - visible_l2_ts)
            trade_delay_ms = max(0, int(now_ts) - visible_trade_ts)
        if self.state.monotonic_exec_visibility_enabled and advance_visibility_state:
            visible_ts = self.services._advance_monotonic_visibility_cutoff(
                self.state.monotonic_exec_visibility_cutoffs_ms,
                feed="bbo",
                candidate_ts_ms=visible_ts,
            )
            visible_l2_ts = self.services._advance_monotonic_visibility_cutoff(
                self.state.monotonic_exec_visibility_cutoffs_ms,
                feed="l2",
                candidate_ts_ms=visible_l2_ts,
            )
            visible_trade_ts = self.services._advance_monotonic_visibility_cutoff(
                self.state.monotonic_exec_visibility_cutoffs_ms,
                feed="trade",
                candidate_ts_ms=visible_trade_ts,
            )
            # Diagnostics describe the state actually consumed after the
            # receive-stream hold, not the discarded draw that would rewind it.
            delay_ms = max(0, int(now_ts) - int(visible_ts))
            depth_delay_ms = max(
                0,
                int(now_ts)
                + int(self.state.exec_depth_visibility_source_offset_ms)
                - int(visible_l2_ts),
            )
            trade_delay_ms = max(0, int(now_ts) - int(visible_trade_ts))
        visible_bbo_idx = (
            int(self.services.np.searchsorted(self.state.bbo_ts, visible_ts, side="right") - 1)
            if self.state.historical_bbo and self.state.bbo_ts is not None and len(self.state.bbo_ts) > 0
            else -1
        )
        visible_l2_idx = (
            int(self.services.np.searchsorted(self.state.l2_ts, visible_l2_ts, side="right") - 1)
            if self.state.historical_l2 and self.state.l2_ts is not None and len(self.state.l2_ts) > 0
            else -1
        )
        if message_clock_states:
            visible_bbo_idx = message_clock_states["bbo"][0]
            visible_l2_idx = message_clock_states["depth"][0]

        book_unavailable = self._bbo_query_unavailable(visible_bbo_idx, visible_l2_idx)
        bbo_usable = visible_bbo_idx >= 0 and bool(self.state.bbo_usable[visible_bbo_idx])
        visible_bid = 0.0 if message_clock_states or book_unavailable else self.state.inferred_best_bid
        visible_ask = 0.0 if message_clock_states or book_unavailable else self.state.inferred_best_ask
        visible_l2_bid_px = None
        visible_l2_bid_qty = None
        visible_l2_ask_px = None
        visible_l2_ask_qty = None
        visible_bid_qty = 0.0
        visible_ask_qty = 0.0
        if visible_l2_idx >= 0:
            visible_l2_bid_px = self.state.l2_bid_px[visible_l2_idx]
            visible_l2_bid_qty = self.state.l2_bid_qty[visible_l2_idx]
            visible_l2_ask_px = self.state.l2_ask_px[visible_l2_idx]
            visible_l2_ask_qty = self.state.l2_ask_qty[visible_l2_idx]
        if bbo_usable:
            visible_bid = float(self.state.bbo_best_bid[visible_bbo_idx])
            visible_ask = float(self.state.bbo_best_ask[visible_bbo_idx])
            if self.state.bbo_bid_qty is not None and self.state.bbo_ask_qty is not None:
                visible_bid_qty = max(
                    0.0,
                    float(self.state.bbo_bid_qty[visible_bbo_idx]),
                )
                visible_ask_qty = max(
                    0.0,
                    float(self.state.bbo_ask_qty[visible_bbo_idx]),
                )
            elif visible_l2_idx >= 0:
                visible_bid_qty = max(
                    0.0,
                    float(visible_l2_bid_qty[0]),
                )
                visible_ask_qty = max(
                    0.0,
                    float(visible_l2_ask_qty[0]),
                )
        elif visible_l2_idx >= 0 and not book_unavailable:
            visible_bid = float(visible_l2_bid_px[0])
            visible_ask = float(visible_l2_ask_px[0])
            visible_bid_qty = max(0.0, float(visible_l2_bid_qty[0]))
            visible_ask_qty = max(0.0, float(visible_l2_ask_qty[0]))

        guard_source = ""
        guard_fallback_reason = ""
        if message_clock_states:
            # Use the same two-clock selection as live: a stale/unavailable
            # bookTicker is optional, but cannot override the mandatory depth.
            snapshot = self.context.QuoteDecisionSnapshot(
                capture_ts_ns=int(now_ts) * 1_000_000,
                market_generation=0, depth_generation=visible_l2_idx + 1,
                book_ticker_generation=visible_bbo_idx + 1,
                depth_exchange_ts_ms=(int(self.state.l2_observation_us[visible_l2_idx]) // 1000 if visible_l2_idx >= 0 else 0),
                depth_receive_ts_ns=(int(self.state.exec_message_payload["depth"].get(
                    "source_receive_ts_ns", self.state.exec_message_schedules["depth"].receive_ns_for_channel())[visible_l2_idx])
                                     if visible_l2_idx >= 0 else 0),
                bids=(), asks=(),
                best_bid=float(visible_l2_bid_px[0]) if visible_l2_idx >= 0 else 0.0,
                best_ask=float(visible_l2_ask_px[0]) if visible_l2_idx >= 0 else 0.0,
                mid=0.0, bar_pricing_mid=0.0,
                book_ticker_bid=float(self.state.bbo_best_bid[visible_bbo_idx]) if bbo_usable else 0.0,
                book_ticker_ask=float(self.state.bbo_best_ask[visible_bbo_idx]) if bbo_usable else 0.0,
                book_ticker_exchange_ts_ms=(int(self.state.bbo_observation_us[visible_bbo_idx]) // 1000 if visible_bbo_idx >= 0 else 0),
                book_ticker_receive_ts_ns=(int(self.state.exec_message_payload["bbo"].get(
                    "source_receive_ts_ns", self.state.exec_message_schedules["bbo"].receive_ns_for_channel())[visible_bbo_idx])
                                          if visible_bbo_idx >= 0 else 0),
                book_ticker_sequence=None, depth_history=(), lock_wait_ns=0,
                lock_hold_ns=0, valid=visible_l2_idx >= 0,
            )
            guard = snapshot.post_only_guard(
                max_visible_age_s=self.state.max_exec_book_visible_age_ms / 1000.0,
                max_source_lag_s=self.state.max_exec_book_source_lag_ms / 1000.0,
            )
            guard_source, guard_fallback_reason = guard.source, guard.fallback_reason
            visible_bid, visible_ask = guard.best_bid, guard.best_ask
            if guard.source == "depth" and visible_l2_idx >= 0:
                visible_bid_qty = max(0.0, float(visible_l2_bid_qty[0]))
                visible_ask_qty = max(0.0, float(visible_l2_ask_qty[0]))

        visible_mid = float(fallback_mid)
        if visible_bid > 0.0 and visible_ask > visible_bid:
            visible_mid = 0.5 * (visible_bid + visible_ask)
        if message_clock_states and visible_l2_idx >= 0:
            # Live snapshots price from partial depth. bookTicker is a
            # separate post-only guard, not the quote/reference-price source.
            visible_mid = 0.5 * (
                float(visible_l2_bid_px[0]) + float(visible_l2_ask_px[0])
            )
        inventory_mark_price = 0.0
        inventory_mark_source = "unobserved"
        inventory_mark_ready_ns = 0
        inventory_mark_same_ready = False
        if message_clock_states:
            # InventoryManager is marked by *both* public callbacks, not by
            # depth. A late old trade may overwrite a newer exchange-time
            # bookTicker; select by callback-ready time, never source time.
            mark_candidates = []
            if bbo_usable:
                bid = float(self.state.bbo_best_bid[visible_bbo_idx])
                ask = float(self.state.bbo_best_ask[visible_bbo_idx])
                if bid > 0.0 and ask > 0.0:
                    mark_candidates.append((
                        message_clock_states["bbo"][2], 1,
                        "bookTicker", 0.5 * (bid + ask),
                    ))
            parent_idx = message_clock_states["trade"][0]
            if parent_idx >= 0:
                parent_prices = self.state.exec_message_payload["trade"].get("mark_price")
                child_idx = int(
                    self.state.exec_message_payload["trade"]["last_child_row_index"][parent_idx]
                )
                if parent_prices is not None or child_idx >= 0:
                    price = (
                        float(parent_prices[parent_idx]) if parent_prices is not None
                        else float(self.state.trade_price[self.state.exec_message_trade_event_rows[child_idx]])
                    )
                    if price > 0.0:
                        mark_candidates.append((
                            message_clock_states["trade"][2], 0, "trade", price,
                        ))
            if mark_candidates:
                inventory_mark_same_ready = (
                    len(mark_candidates) == 2
                    and mark_candidates[0][0] == mark_candidates[1][0]
                )
                # Separate historical feeds do not identify cross-feed order
                # at identical ready timestamps. Keep a deterministic BBO tie
                # break and expose that ambiguity instead of inventing lag.
                (inventory_mark_ready_ns, _, inventory_mark_source,
                 inventory_mark_price) = max(mark_candidates)
        observed_mid_override = bool(not book_unavailable and self.services.np.isfinite(paired_mid) and paired_mid > 0.0)
        if observed_mid_override:
            visible_mid = float(paired_mid)
            visible_bid = round(visible_mid - 0.5 * self.state.TICK, 10)
            visible_ask = round(visible_mid + 0.5 * self.state.TICK, 10)
        depth_receive_ns = (
            int(self.state.exec_message_payload["depth"].get(
                "source_receive_ts_ns", self.state.exec_message_schedules["depth"].receive_ns_for_channel(),
            )[visible_l2_idx])
            if message_clock_states and visible_l2_idx >= 0 else 0
        )
        depth_observation_ns = (
            int(self.state.l2_observation_us[visible_l2_idx]) * 1000
            if visible_l2_idx >= 0 else 0
        )
        if book_unavailable:
            # An explicit invalidation cannot be turned into an inferred
            # trade touch or overwritten by an older still-visible depth.
            visible_bid = visible_ask = visible_mid = 0.0
            visible_bid_qty = visible_ask_qty = 0.0
            guard_source, guard_fallback_reason = "unavailable", "invalid_book_observation"
        return {
            "book_usable": not book_unavailable,
            "delay_ms": int(delay_ms),
            "depth_delay_ms": int(depth_delay_ms),
            "trade_delay_ms": int(trade_delay_ms),
            "visible_ts": int(visible_ts),
            "visible_l2_ts": int(visible_l2_ts),
            "visible_trade_ts": int(visible_trade_ts),
            "paired_hit": bool(paired_state is not None),
            "observed_mid_override": observed_mid_override,
            "bbo_idx": int(visible_bbo_idx),
            "l2_idx": int(visible_l2_idx),
            "best_bid": float(visible_bid),
            "best_ask": float(visible_ask),
            "bid_qty": float(visible_bid_qty),
            "ask_qty": float(visible_ask_qty),
            "mid": float(visible_mid),
            "l2_bid_px": visible_l2_bid_px,
            "l2_bid_qty": visible_l2_bid_qty,
            "l2_ask_px": visible_l2_ask_px,
            "l2_ask_qty": visible_l2_ask_qty,
            **(
                {
                    "bbo_feature_ready_ts_ns": message_clock_states["bbo"][2],
                    "post_only_guard_source": guard_source,
                    "post_only_guard_fallback_reason": guard_fallback_reason,
                    "depth_feature_ready_ts_ns": message_clock_states["depth"][2],
                    "depth_receive_ts_ns": depth_receive_ns,
                    "depth_visible_age_ms": (
                        (int(now_ts) * 1_000_000 - depth_receive_ns) / 1_000_000.0
                        if visible_l2_idx >= 0 else self.services.math.inf
                    ),
                    "depth_source_lag_ms": (
                        (depth_receive_ns - depth_observation_ns) / 1_000_000.0
                        if visible_l2_idx >= 0 else self.services.math.inf
                    ),
                    "trade_feature_ready_ts_ns": message_clock_states["trade"][2],
                    "trade_message_index": message_clock_states["trade"][0],
                    "inventory_mark_price": inventory_mark_price,
                    "inventory_mark_source": inventory_mark_source,
                    "inventory_mark_ready_ts_ns": inventory_mark_ready_ns,
                    "inventory_mark_same_ready": inventory_mark_same_ready,
                }
                if message_clock_states else {}
            ),
        }

    def _markout_observation_mid(self, now_ts: int, fallback_mid: float) -> float:
        """Match live markout observations to the visible partial-depth top.

        ``SignalEngine.mid_price`` is sourced from ``_last_depth`` in live, not
        from bookTicker or the latest exchange-time BBO.  During a depth-stream
        stall the markout observer therefore keeps the last received depth mid
        even when trades and bookTicker continue moving.  A paired telemetry
        profile records that state as ``sample_ts - exec_depth_age``; carry the
        implied source boundary forward until the next telemetry sample.
        """
        # Capture provenance in this same lookup, without a second state read.
        # A replay row clock does not prove the underlying price's source age.
        self.state.mo_observation_price = {
            "price_selector": "current_replay_mid_fallback",
            "selected_l2_index": None, "price_source_ts_ms": None,
            "price_receive_ts_ms": None, "price_ready_ts_ms": None,
            "price_age_ms": None, "price_age_known": False,
        }
        if not self.state.historical_l2 or self.state.l2_ts is None or len(self.state.l2_ts) == 0:
            return float(fallback_mid)

        if self.state.exec_message_schedules:
            idx, _, _ = self._message_clock_state("depth", int(now_ts))
            self.state.mo_observation_price.update(
                price_selector="visible_message_depth_mid", selected_l2_index=(
                    idx + self.state.input_clock_offsets['l2'] if idx >= 0 else None))
            if idx < 0:
                return float("nan")
            bid = float(self.state.l2_bid_px[idx, 0])
            ask = float(self.state.l2_ask_px[idx, 0])
            return 0.5 * (bid + ask) if bid > 0.0 and ask > bid else float("nan")

        visible_l2_ts = (
            int(now_ts) + self.state.exec_depth_visibility_source_offset_ms
        )
        if (
            self.state.exec_book_visibility_mode == "paired"
            and self.state.exec_book_visibility_paired_ts_ms.size > 0
        ):
            paired_source_ts = self.services._paired_exec_source_visible_ts(
                int(now_ts),
                timestamps_ms=self.state.exec_book_visibility_paired_ts_ms,
                source_delays_ms=self.state.exec_depth_visibility_paired_delay_ms,
            )
            if paired_source_ts is not None:
                visible_l2_ts = (
                    int(paired_source_ts)
                    + self.state.exec_depth_visibility_source_offset_ms
                )
            elif self.state.exec_book_visibility_delay_enabled and not self.state.use_bar_pricing:
                visible_l2_ts -= self.services._exec_book_visibility_delay_ms(
                    int(now_ts),
                    mean_ms=self.state.exec_book_visibility_delay_mean_ms,
                    jitter_ms=self.state.exec_book_visibility_delay_jitter_ms,
                    seed=self.state.exec_book_visibility_delay_seed,
                    samples_ms=self.state.exec_book_visibility_delay_samples_ms,
                )
        elif (
            self.state.exec_book_visibility_mode == "sampled_joint"
            and self.state.exec_depth_visibility_paired_delay_ms.size > 0
            and not self.state.use_bar_pricing
        ):
            _, depth_delay_ms, _ = self.services._sampled_joint_exec_book_visibility_state(
                int(now_ts),
                seed=self.state.exec_book_visibility_delay_seed,
                book_delays_ms=self.state.exec_book_visibility_paired_delay_ms,
                depth_delays_ms=self.state.exec_depth_visibility_paired_delay_ms,
                trade_delays_ms=self.state.exec_trade_visibility_paired_delay_ms,
            )
            visible_l2_ts -= int(depth_delay_ms)
        elif (
            self.state.exec_book_visibility_mode == "profile_source_stratified"
            and self.state.exec_source_profile is not None
            and not self.state.use_bar_pricing
        ):
            book_delay_ms, _, _ = self.services._source_stratified_exec_visibility_state(
                int(now_ts),
                seed=self.state.exec_book_visibility_delay_seed,
                simulator=self.state.exec_source_profile,
                market_id=self.state.exec_source_profile_market_id,
                transport=self.state.exec_source_profile_transport,
            )
            visible_l2_ts -= int(book_delay_ms)
        elif self.state.exec_book_visibility_delay_enabled and not self.state.use_bar_pricing:
            visible_l2_ts -= self.services._exec_book_visibility_delay_ms(
                int(now_ts),
                mean_ms=self.state.exec_book_visibility_delay_mean_ms,
                jitter_ms=self.state.exec_book_visibility_delay_jitter_ms,
                seed=self.state.exec_book_visibility_delay_seed,
                samples_ms=self.state.exec_book_visibility_delay_samples_ms,
            )

        if self.state.monotonic_exec_visibility_enabled:
            visible_l2_ts = self.services._advance_monotonic_visibility_cutoff(
                self.state.monotonic_exec_visibility_cutoffs_ms,
                feed="l2",
                candidate_ts_ms=visible_l2_ts,
            )

        visible_l2_idx = int(
            self.services.np.searchsorted(self.state.l2_ts, int(visible_l2_ts), side="right") - 1
        )
        if visible_l2_idx < 0:
            return float(fallback_mid)
        bid = float(self.state.l2_bid_px[visible_l2_idx, 0])
        ask = float(self.state.l2_ask_px[visible_l2_idx, 0])
        if bid > 0.0 and ask > bid:
            self.state.mo_observation_price.update(
                price_selector="visibility_selected_depth_mid",
                selected_l2_index=visible_l2_idx + self.state.input_clock_offsets['l2'])
            return 0.5 * (bid + ask)
        return float(fallback_mid)

    def _advance_event_market(self):
        """Advance causal market views, order boundaries and visible markout state."""
        if self.state.is_market_array_event and self.state.replay_progress_callback is not None and (
            self.state.i == 0 or self.state.i % self.state.replay_progress_interval_events == 0
        ):
            self.state.replay_progress_callback(
                {
                    "event_index": int(self.state.i),
                    "event_count": int(self.state.n_trades),
                    "event_ts_ms": int(self.state.trade_ts[self.state.i]),
                }
            )
        self.state.last_processed_event_idx = self.state.i
        self.state.active_decision_bbo_idx = None
        self.state.t = self.state.event_ts_ms
        if self.state.quote_schedule is not None:
            self._quote_schedule_publish(int(self.state.t))
            # Own and acknowledge even a quote opportunity rejected by an
            # earlier safety/maintenance return. KEEP cannot self-excite.
            if self.state.is_main_loop_wake:
                self.state.quote_schedule.claim(int(self.state.t) * 1_000_000)
        if self.state.utc_accounting_marks is not None and self.state.is_market_array_event:
            self.state.utc_accounting_marks.observe(
                int(self.state.t), float(self.state.trade_price[self.state.i]),
            )
        self.state.hard_risk_clock_ts_ms = int(self.state.t)
        self.state.p = self.state.trade_price[self.state.i]
        self.state.qty = self.state.trade_qty[self.state.i] if self.state.is_market_array_event else 0.0
        self.state.is_sell = self.state.is_seller[self.state.i]
        self.state.is_execution_event = bool(self.state.is_market_array_event and self.state.is_execution_trade[self.state.i])
        self.state.has_executable_trade = self.state.is_execution_event and float(self.state.qty) > 0.0
        self.state.trade_price_tick = self.services._price_to_tick(float(self.state.p), self.state.TICK) if self.state.has_executable_trade else 0
        self.state.dynamic_fill_hazard_last_event_at_timestamp = bool(
            self.state.i + 1 >= self.state.n_trades or int(self.state.trade_ts[self.state.i + 1]) != int(self.state.t)
        )
        self.state.system_event_code = (
            int(self.state.maker_event_code[self.state.i]) if self.state.is_market_array_event and not self.state.is_execution_event else 0
        )
        self.state.empirical_clock_action = (
            int(self.state.maker_event_code[self.state.i])
            if (
                self.state.replay_event_clock == "empirical"
                and not self.state.is_execution_event
                and int(self.state.maker_event_code[self.state.i]) in (2, 3)
            )
            else 0
        )
        if (
            self.state.exchange_book_scheduler is not None
            or self.state.local_lifecycle_boundary_scheduler is not None
        ):
            self.state.lifecycle_fallback_mid = (
                float(self.state.prev_mark_px)
                if self.services.np.isfinite(self.state.prev_mark_px) and self.state.prev_mark_px > 0.0
                else float(self.state.p)
            )
            self._advance_exchange_book_through_order_lifecycle(
                int(self.state.t),
                self.state.lifecycle_fallback_mid,
            )
        self.state.continuation_pending_bid = self._replacement_continuation_pending("BUY")
        self.state.continuation_pending_ask = self._replacement_continuation_pending("SELL")
        # The merged replay clock contains 1ms market-data boundaries as well
        # as the fixed 100ms policy timer. A terminal callback may become
        # visible at either kind of boundary, but live continuation is a main
        # loop action: keep it pending until the next actual timer wake.
        self.state.continuation_timer_wake = bool(
            self.state.replace_terminal_continuation
            and self.state.replay_event_clock == "merged"
            and (
                (self.state.main_loop_enabled and self.state.is_main_loop_wake)
                or (
                    not self.state.main_loop_enabled
                    and int(self.state.t) % self.state.replay_clock_interval_ms == 0
                )
            )
        )
        self.state.continuation_force_bid_due = bool(
            self.state.continuation_pending_bid and self.state.continuation_timer_wake
        )
        self.state.continuation_force_ask_due = bool(
            self.state.continuation_pending_ask and self.state.continuation_timer_wake
        )
        self.state.continuation_bid_terminal_ts = self.state.replacement_terminal_due_ts["BUY"]
        self.state.continuation_ask_terminal_ts = self.state.replacement_terminal_due_ts["SELL"]
        if self.state.continuation_force_bid_due:
            self.state.replacement_terminal_due_ts["BUY"] = -1
        if self.state.continuation_force_ask_due:
            self.state.replacement_terminal_due_ts["SELL"] = -1
        self.state.exchange_book_pre_advance = (
            self.state.exchange_book_scheduler.advance_to(
                int(self.state.t) * 1_000_000,
                inclusive=False,
                emitted_levels=self._exchange_book_emitted_levels(),
            )
            if self.state.exchange_book_scheduler is not None
            else None
        )
        self.state.l2_idx_before_event = self.state.l2_idx
        if self.state.reference_scheduler is not None:
            self.state.reference_scheduler.advance_to(int(self.state.t) * 1_000_000)
        if not self.state.exec_message_schedules or not self.state.main_loop_enabled or self.state.is_main_loop_wake:
            # Live resolves/decays markouts on the main loop. Public callbacks
            # that arrive while quoting is blocked do not advance this state.
            self._decay_markout_ema(int(self.state.t))

        # inventory_pnl 是持仓期间价格路径漂移分解项，不是库存风险惩罚。
        # The shared clock also lets a future local lifecycle callback accrue
        # the old inventory exactly through its visibility boundary.
        self._accrue_local_path_to(int(self.state.t), float(self.state.p))

        # Infer the opposite side of the touch from the latest aggressing trade.
        # aggTrade prints at bid on seller aggression and at ask on buyer aggression.
        if self.state.is_execution_event:
            if self.state.is_sell:
                self.state.inferred_best_bid = self.state.p
                if self.state.inferred_best_ask <= self.state.inferred_best_bid:
                    self.state.inferred_best_ask = self.state.inferred_best_bid + self.state.TICK
            else:
                self.state.inferred_best_ask = self.state.p
                if self.state.inferred_best_bid >= self.state.inferred_best_ask:
                    self.state.inferred_best_bid = self.state.inferred_best_ask - self.state.TICK

        if self.state.historical_l2 and self.state.l2_ts is not None and len(self.state.l2_ts) > 0:
            while self.state.l2_idx < len(self.state.l2_ts) - 1 and self.state.l2_ts[self.state.l2_idx + 1] <= self.state.t:
                self.state.l2_idx += 1
            if self.state.l2_ts[self.state.l2_idx] <= self.state.t:
                self.state.cur_l2_bid_px_row = self.state.l2_bid_px[self.state.l2_idx]
                self.state.cur_l2_bid_qty_row = self.state.l2_bid_qty[self.state.l2_idx]
                self.state.cur_l2_ask_px_row = self.state.l2_ask_px[self.state.l2_idx]
                self.state.cur_l2_ask_qty_row = self.state.l2_ask_qty[self.state.l2_idx]

        if self.state.historical_bbo and self.state.bbo_ts is not None and len(self.state.bbo_ts) > 0:
            while self.state.bbo_idx < len(self.state.bbo_ts) - 1 and self.state.bbo_ts[self.state.bbo_idx + 1] <= self.state.t:
                self.state.bbo_idx += 1
            if self.state.bbo_ts[self.state.bbo_idx] <= self.state.t:
                if self.state.bbo_usable[self.state.bbo_idx]:
                    self.state.cur_best_bid = self.state.bbo_best_bid[self.state.bbo_idx]
                    self.state.cur_best_ask = self.state.bbo_best_ask[self.state.bbo_idx]
                elif not self._bbo_query_unavailable(self.state.bbo_idx, self.state.l2_idx):
                    self.state.cur_best_bid = self.state.l2_bid_px[self.state.l2_idx, 0]
                    self.state.cur_best_ask = self.state.l2_ask_px[self.state.l2_idx, 0]
                else:
                    self.state.cur_best_bid = self.state.cur_best_ask = 0.0
        elif self.state.historical_l2 and self.state.cur_l2_bid_px_row is not None and self.state.cur_l2_ask_px_row is not None:
            self.state.cur_best_bid = self.state.cur_l2_bid_px_row[0]
            self.state.cur_best_ask = self.state.cur_l2_ask_px_row[0]
        else:
            self.state.cur_best_bid = self.state.inferred_best_bid
            self.state.cur_best_ask = self.state.inferred_best_ask
        if self.state.cur_best_bid > 0.0 and self.state.cur_best_ask > self.state.cur_best_bid:
            # Live maker_engine quotes off the latest bookTicker mark price.
            # Keep replay quote-core mid on the same BBO/L2 snapshot as best_bid/ask;
            # otherwise fast moves mix a stale trade-price mid with fresh BBO guards.
            self.state.mid = 0.5 * (self.state.cur_best_bid + self.state.cur_best_ask)
        elif self._bbo_query_unavailable(self.state.bbo_idx, self.state.l2_idx):
            self.state.mid = 0.0
        else:
            self.state.mid = self.state.p

        self.state.event_external_generation: dict[str, int] | None = None
        self.state.event_external_ready = False
        if self.state.pending_external_market_release_rows or (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and not self.state.ema_add_wait_fork_external_epoch_observed
            and self.state.ema_add_wait_fork_release_clock
            == "next_ready_market_generation_v1"
        ):
            (
                self.state.event_external_generation,
                self.state.event_external_ready,
            ) = self._external_market_generation_at_event(int(self.state.t), int(self.state.i), float(self.state.p))

        if self.state.pending_external_market_release_rows and self.state.event_external_ready:
            self.state.unresolved_release_rows = []
            for loop_item in self.state.pending_external_market_release_rows:
                self.state.trace_row, self.state.target_generation = loop_item
                if not self._market_generation_strictly_after(
                    self.state.event_external_generation,
                    self.state.target_generation,
                ):
                    self.state.unresolved_release_rows.append((self.state.trace_row, self.state.target_generation))
                    continue
                self.state.trace_row["external_epoch_support_valid"] = 1
                self.state.trace_row["external_epoch_support_reason"] = "supported"
                self.state.trace_row["release_ts_ms"] = int(self.state.t)
                self.state.trace_row["release_market_event_generation"] = int(self.state.i)
                self.state.trace_row["release_market_generation_identity"] = (
                    self._market_generation_identity(self.state.event_external_generation)
                )
                for loop_item in self.state.event_external_generation.items():
                    self.state.field, self.state.value = loop_item
                    self.state.trace_row[f"release_{self.state.field}"] = int(self.state.value)
            self.state.pending_external_market_release_rows[:] = self.state.unresolved_release_rows

        # The successor clock releases at the first ready market-content
        # generation after assignment. The expected locator is frozen by the
        # untreated census and checked here, before any arm-dependent blocker.
        if (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and not self.state.ema_add_wait_fork_external_epoch_observed
            and self.state.ema_add_wait_fork_release_clock
            == "next_ready_market_generation_v1"
        ):
            if (
                int(self.state.i) > self.state.ema_add_wait_fork_frozen_release_event_index
                or int(self.state.t) > self.state.ema_add_wait_fork_frozen_release_ts_ms
            ):
                raise RuntimeError(
                    "EMA ADD/WAIT fork skipped its frozen external release event"
                )
            if self.state.event_external_ready and self._market_generation_strictly_after(
                self.state.event_external_generation,
                self.state.ema_add_wait_fork_target_generation,
            ):
                self.state.expected_release_generation = {
                    str(key): int(value)
                    for key, value in self.state.ema_add_wait_fork_frozen_release_generation.items()
                }
                if (
                    int(self.state.i) != self.state.ema_add_wait_fork_frozen_release_event_index
                    or int(self.state.t) != self.state.ema_add_wait_fork_frozen_release_ts_ms
                    or self.state.event_external_generation != self.state.expected_release_generation
                ):
                    raise RuntimeError(
                        "EMA ADD/WAIT first ready market generation drifted"
                    )
                self.state.ema_add_wait_fork_external_epoch_observed = True
                self.state.ema_add_wait_fork_release_ts_ms = int(self.state.t)
                self.state.ema_add_wait_fork_release_generation = dict(
                    self.state.event_external_generation
                )
                if self.state.ema_add_wait_fork_wait_active:
                    self.state.ema_add_wait_fork_wait_active = False
                    self.state.ema_add_wait_fork_released = True
            elif (
                int(self.state.i) == self.state.ema_add_wait_fork_frozen_release_event_index
                or int(self.state.t) == self.state.ema_add_wait_fork_frozen_release_ts_ms
            ):
                raise RuntimeError(
                    "EMA ADD/WAIT frozen release is not a ready successor generation"
                )
        elif (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and not self.state.ema_add_wait_fork_external_epoch_observed
            and self.state.ema_add_wait_fork_release_clock
            == "frozen_scheduled_requote_v1"
            and int(self.state.i) == self.state.ema_add_wait_fork_frozen_release_event_index
            and int(self.state.t) == self.state.ema_add_wait_fork_frozen_release_ts_ms
        ):
            self.state.ema_add_wait_fork_external_epoch_observed = True
            self.state.ema_add_wait_fork_release_ts_ms = int(self.state.t)
            self.state.ema_add_wait_fork_release_generation = {
                str(key): int(value)
                for key, value in self.state.ema_add_wait_fork_frozen_release_generation.items()
            }
            if self.state.ema_add_wait_fork_wait_active:
                self.state.ema_add_wait_fork_wait_active = False
                self.state.ema_add_wait_fork_released = True
        elif (
            self.state.ema_add_wait_fork_enabled
            and self.state.ema_add_wait_fork_assigned
            and not self.state.ema_add_wait_fork_external_epoch_observed
            and self.state.ema_add_wait_fork_release_clock
            == "frozen_scheduled_requote_v1"
            and (
                int(self.state.i) > self.state.ema_add_wait_fork_frozen_release_event_index
                or int(self.state.t) > self.state.ema_add_wait_fork_frozen_release_ts_ms
            )
        ):
            raise RuntimeError(
                "EMA ADD/WAIT fork skipped its frozen external release event"
            )

        if self.state.exchange_book_scheduler is not None:
            if self.state.exchange_book_pre_advance is not None:
                self._apply_exchange_book_advance(
                    self.state.exchange_book_pre_advance,
                    now_ts_ms=int(self.state.t),
                    execution_trade_same_ms=False,
                )
            self._advance_dynamic_fill_hazard_visibility(
                int(self.state.t) * 1_000_000,
                inclusive=False,
            )
            self._mark_cancel_ack_exchange_book_ambiguity(
                self.state.bid_orders,
                "BUY",
                int(self.state.t),
            )
            self._mark_cancel_ack_exchange_book_ambiguity(
                self.state.ask_orders,
                "SELL",
                int(self.state.t),
            )
            # Activate every order against the native state strictly before
            # this timestamp. The snapshot/delta at t is applied only after
            # the order has joined the queue, so same-ms uncertainty is
            # explicit rather than silently ordered in our favor.
            if self.state.has_executable_trade:
                self._mark_cancel_ack_trade_ambiguity(
                    self.state.bid_orders,
                    "BUY",
                    int(self.state.t),
                )
                self._mark_cancel_ack_trade_ambiguity(
                    self.state.ask_orders,
                    "SELL",
                    int(self.state.t),
                )
            self._process_order_transitions(
                self.state.bid_orders,
                "BUY",
                self.state.t,
                self.state.p,
                defer_cancel_ack_at_ts=bool(self.state.has_executable_trade),
            )
            self._process_order_transitions(
                self.state.ask_orders,
                "SELL",
                self.state.t,
                self.state.p,
                defer_cancel_ack_at_ts=bool(self.state.has_executable_trade),
            )
            self._mark_cancel_ack_exchange_book_ambiguity(
                self.state.bid_orders,
                "BUY",
                int(self.state.t),
            )
            self._mark_cancel_ack_exchange_book_ambiguity(
                self.state.ask_orders,
                "SELL",
                int(self.state.t),
            )
            if self.state.has_executable_trade:
                # TTL or another lifecycle rule may have created a zero-
                # latency cancel at this boundary while transitions ran.
                self._mark_cancel_ack_trade_ambiguity(
                    self.state.bid_orders,
                    "BUY",
                    int(self.state.t),
                )
                self._mark_cancel_ack_trade_ambiguity(
                    self.state.ask_orders,
                    "SELL",
                    int(self.state.t),
                )
            self.state.exchange_book_advance = self.state.exchange_book_scheduler.advance_to(
                int(self.state.t) * 1_000_000,
                inclusive=True,
                emitted_levels=self._exchange_book_emitted_levels(),
            )
            self._apply_exchange_book_advance(
                self.state.exchange_book_advance,
                now_ts_ms=int(self.state.t),
                execution_trade_same_ms=bool(self.state.has_executable_trade),
            )

        self.state.bid_cancel_qty = 0.0
        self.state.bid_refill_qty = 0.0
        self.state.ask_cancel_qty = 0.0
        self.state.ask_refill_qty = 0.0
        self.state.policy_visible_book: dict[str, self.services.Any] | None = None
        if (
            self.state.queue_value_keep_cancel_enabled
            or self.state.trace_local_order_value is not None
            or self.state.local_order_lifecycle.enabled
        ):
            self.state.policy_visible_book = self._decision_visible_book(
                int(self.state.t),
                float(self.state.p),
                advance_visibility_state=self.state.queue_value_keep_cancel_enabled,
            )
            self.state.current_bid_qty = float(self.state.policy_visible_book["bid_qty"])
            self.state.current_ask_qty = float(self.state.policy_visible_book["ask_qty"])
            if (
                not self.state.is_execution_event
                and self.state.queue_value_prev_bid_qty is not None
            ):
                self.state.bid_cancel_qty = max(
                    0.0,
                    float(self.state.queue_value_prev_bid_qty - self.state.current_bid_qty),
                )
                self.state.bid_refill_qty = max(
                    0.0,
                    float(self.state.current_bid_qty - self.state.queue_value_prev_bid_qty),
                )
            if (
                not self.state.is_execution_event
                and self.state.queue_value_prev_ask_qty is not None
            ):
                self.state.ask_cancel_qty = max(
                    0.0,
                    float(self.state.queue_value_prev_ask_qty - self.state.current_ask_qty),
                )
                self.state.ask_refill_qty = max(
                    0.0,
                    float(self.state.current_ask_qty - self.state.queue_value_prev_ask_qty),
                )
            if self.state.queue_value_keep_cancel_enabled and self.state.is_execution_event:
                self.state.buy_adverse = int(bool(self.state.is_sell))
                self.state.sell_adverse = int(not bool(self.state.is_sell))
                self.state.queue_value_runtime["BUY"].observe(
                    int(self.state.t) * 1_000_000,
                    adverse_market_order=float(self.state.buy_adverse),
                )
                self.state.queue_value_runtime["SELL"].observe(
                    int(self.state.t) * 1_000_000,
                    adverse_market_order=float(self.state.sell_adverse),
                )
                self.state.queue_value_runtime_event_counts["BUY"][
                    "adverse_market_order"
                ] += self.state.buy_adverse
                self.state.queue_value_runtime_event_counts["SELL"][
                    "adverse_market_order"
                ] += self.state.sell_adverse
            elif self.state.queue_value_keep_cancel_enabled:
                if (
                    self.state.queue_value_prev_bid_qty is not None
                    and self.state.queue_value_runtime_event_source.get("BUY")
                    == "policy_visible_top_book"
                ):
                    self.state.queue_value_runtime["BUY"].observe(
                        int(self.state.t) * 1_000_000,
                        cancel=float(self.state.bid_cancel_qty > 1e-12),
                        refill=float(self.state.bid_refill_qty > 1e-12),
                    )
                    self.state.queue_value_runtime_event_counts["BUY"][
                        "cancel"
                    ] += int(self.state.bid_cancel_qty > 1e-12)
                    self.state.queue_value_runtime_event_counts["BUY"][
                        "refill"
                    ] += int(self.state.bid_refill_qty > 1e-12)
                if (
                    self.state.queue_value_prev_ask_qty is not None
                    and self.state.queue_value_runtime_event_source.get("SELL")
                    == "policy_visible_top_book"
                ):
                    self.state.queue_value_runtime["SELL"].observe(
                        int(self.state.t) * 1_000_000,
                        cancel=float(self.state.ask_cancel_qty > 1e-12),
                        refill=float(self.state.ask_refill_qty > 1e-12),
                    )
                    self.state.queue_value_runtime_event_counts["SELL"][
                        "cancel"
                    ] += int(self.state.ask_cancel_qty > 1e-12)
                    self.state.queue_value_runtime_event_counts["SELL"][
                        "refill"
                    ] += int(self.state.ask_refill_qty > 1e-12)
            self.state.queue_value_prev_bid_qty = self.state.current_bid_qty
            self.state.queue_value_prev_ask_qty = self.state.current_ask_qty

        self._process_order_transitions(
            self.state.bid_orders,
            "BUY",
            self.state.t,
            self.state.p,
            defer_cancel_ack_at_ts=bool(
                self.state.exchange_book_scheduler is not None
                and self.state.has_executable_trade
            ),
        )
        self._process_order_transitions(
            self.state.ask_orders,
            "SELL",
            self.state.t,
            self.state.p,
            defer_cancel_ack_at_ts=bool(
                self.state.exchange_book_scheduler is not None
                and self.state.has_executable_trade
            ),
        )
        self._update_local_order_value_path_counts(
            is_execution_event=bool(self.state.has_executable_trade),
            is_sell_trade=bool(self.state.is_sell),
            trade_price_now=float(self.state.p),
            trade_quantity=float(self.state.qty),
            bid_cancel_qty=float(self.state.bid_cancel_qty),
            bid_refill_qty=float(self.state.bid_refill_qty),
            ask_cancel_qty=float(self.state.ask_cancel_qty),
            ask_refill_qty=float(self.state.ask_refill_qty),
        )
        if (
            self.state.local_order_lifecycle.enabled
            and not self.state.is_execution_event
            and self.state.policy_visible_book is not None
        ):
            self._record_local_order_lifecycle_risk_snapshots(
                int(self.state.t),
                self.state.policy_visible_book,
            )
        if self.state.historical_l2 and self.state.l2_idx != self.state.l2_idx_before_event:
            self._apply_l2_cancel_ahead(self.state.bid_orders, "BUY", int(self.state.l2_idx))
            self._apply_l2_cancel_ahead(self.state.ask_orders, "SELL", int(self.state.l2_idx))
        # Independent counterfactual orders see the same public trade/L2 event,
        # but never consume baseline liquidity, inventory, or RNG state.
        if self.state.has_executable_trade:
            self._process_safe_add_rearm_orders(int(self.state.t), float(self.state.p), float(self.state.qty), bool(self.state.is_sell))
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

        # Resolve once the minimum fill age is reached at an original processing
        # opportunity; use its visible price, not an exact target-time query.
        if (
            self.state.markout_ema_span_fills > 0 and self.state.mo_pending
            and (not self.state.main_loop_enabled or self.state.is_main_loop_wake)
        ):
            self.state.markout_mid = self._markout_observation_mid(int(self.state.t), float(self.state.mid))
            self.state.resolved = []
            for loop_item in enumerate(self.state.mo_pending):
                self.state.j, self.state.item = loop_item
                self.state.ft, self.state.fpx, self.state.is_bid = self.state.item[:3]
                self.state.quote_flags = self.state.item[3] if len(self.state.item) > 3 else {}
                self.state.fill_qty_btc = float(self.state.item[4]) if len(self.state.item) > 4 else 1.0
                self.state.age_ms = int(self.state.t - self.state.ft)
                if self.state.age_ms >= self.state.markout_horizon_ms:
                    if (not self.state.exec_message_schedules
                            and self.state.age_ms > self.state.adverse_markout_max_resolve_gap_ms):
                        self.state.mo_stale_drop_count += 1
                        self._observe_markout_item(self.state.item, self.state.t, "STALE_DROP")
                        self.state.resolved.append(self.state.j)
                        continue
                    if self.state.exec_message_schedules and not self.services.math.isfinite(self.state.markout_mid):
                        # An inherited fill may precede the first received
                        # depth message. Wait for visibility; exchange truth
                        # is not a substitute for an unobserved local markout.
                        if not self.state.quote_flags.get("markout_missing_price_observed", False):
                            self._observe_markout_item(self.state.item, self.state.t, "WAITING_FOR_PRICE")
                            self.state.quote_flags["markout_missing_price_observed"] = True
                        continue
                    self.state.mo_observation_ema_before = {
                        "side": self.state.mo_ema_bid if self.state.is_bid else self.state.mo_ema_ask,
                        "all": self.state.mo_ema_all,
                    }
                    if self.state.is_bid:
                        self.state.mo_val = self.state.markout_mid - self.state.fpx
                        self.state.mo_ema_bid = self.state.mo_alpha * self.state.mo_val + (1.0 - self.state.mo_alpha) * self.state.mo_ema_bid
                        self.state.mo_sum_bid += self.state.mo_val * self.state.fill_qty_btc
                        self.state.mo_count_bid += 1
                        self.state.mo_qty_bid += self.state.fill_qty_btc
                        self._extend_markout_pause_until(True, int(self.state.t))
                    else:
                        self.state.mo_val = self.state.fpx - self.state.markout_mid
                        self.state.mo_ema_ask = self.state.mo_alpha * self.state.mo_val + (1.0 - self.state.mo_alpha) * self.state.mo_ema_ask
                        self.state.mo_sum_ask += self.state.mo_val * self.state.fill_qty_btc
                        self.state.mo_count_ask += 1
                        self.state.mo_qty_ask += self.state.fill_qty_btc
                        self._extend_markout_pause_until(False, int(self.state.t))
                    self.state.mo_ema_all = self.state.mo_alpha * self.state.mo_val + (1.0 - self.state.mo_alpha) * self.state.mo_ema_all
                    self.state.mo_sum_all += self.state.mo_val * self.state.fill_qty_btc
                    self.state.mo_count_all += 1
                    self.state.mo_qty_all += self.state.fill_qty_btc
                    self._observe_markout_item(self.state.item, self.state.t, "RESOLVED",
                        mid=float(self.state.markout_mid), ema_before=self.state.mo_observation_ema_before)
                    self._observe_markout_pause(self.state.t, "resolved_fill")
                    if self.state.quote_flags.get("final_compressed", False):
                        self.state.mo_sum_final_compressed += self.state.mo_val * self.state.fill_qty_btc
                        self.state.mo_count_final_compressed += 1
                        self.state.mo_qty_final_compressed += self.state.fill_qty_btc
                    else:
                        self.state.mo_sum_not_final_compressed += self.state.mo_val * self.state.fill_qty_btc
                        self.state.mo_count_not_final_compressed += 1
                        self.state.mo_qty_not_final_compressed += self.state.fill_qty_btc
                    self.state.resolved.append(self.state.j)
            for loop_item in reversed(self.state.resolved):
                self.state.j = loop_item
                self.state.mo_pending.pop(self.state.j)
