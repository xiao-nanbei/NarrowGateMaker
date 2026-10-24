"""Replay hazard owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class HazardDiagnostics:
    def _dynamic_fill_hazard_feature_ready_ts_ns(self,
        *,
        exchange_ts_ns: int,
        provider_receive_ts_ns: int,
        event_type: str,
        source_ordinal: int,
    ) -> int:
        exchange_ns = int(exchange_ts_ns)
        if self.state.dynamic_fill_hazard_visibility_clock_mode == "legacy_mixed":
            return max(exchange_ns, int(provider_receive_ts_ns or 0))
        if (
            self.state.dynamic_fill_hazard_visibility_clock_mode
            == "provider_receive_sensitivity"
        ):
            if str(event_type) == "book":
                receive_ns = int(provider_receive_ts_ns or 0)
                return (
                    max(exchange_ns, receive_ns if receive_ns > 0 else exchange_ns)
                    + self.state.dynamic_fill_hazard_provider_feature_latency_ns
                )
            return exchange_ns + self.state.dynamic_fill_hazard_provider_trade_delay_ns
        if self.state.dynamic_fill_hazard_visibility_profile is None:
            raise RuntimeError("q90 AWS visibility profile is not initialized")
        source_stratified = (
            self.state.dynamic_fill_hazard_profile_mode == "profile_source_stratified"
        )
        event_salt = (
            0
            if source_stratified
            else (0xB00C if str(event_type) == "book" else 0x7ADE)
        )
        seed_clock = (
            exchange_ns
            // (
                self.state.dynamic_fill_hazard_visibility_profile.source_stratified_bucket_ms
                * 1_000_000
            )
            if source_stratified
            else exchange_ns
        )
        keyed_seed = self.services._splitmix64(
            int(self.state.dynamic_fill_hazard_visibility_seed)
            ^ int(seed_clock)
            ^ (0 if source_stratified else (int(source_ordinal) << 1))
            ^ event_salt
        )
        delay_ms = self.state.dynamic_fill_hazard_visibility_profile.delay_ms(
            {
                "market_id": (
                    self.state.dynamic_fill_hazard_profile_market_id
                    if str(event_type) == "book"
                    else self.state.dynamic_fill_hazard_profile_trade_market_id
                ),
                "event_type": str(event_type),
                "transport": self.state.dynamic_fill_hazard_profile_transport,
            },
            mode=self.state.dynamic_fill_hazard_profile_mode,
            rng=self.services.random.Random(int(keyed_seed)),
        )
        return exchange_ns + int(round(float(delay_ms) * 1_000_000.0))

    def _dynamic_fill_hazard_policy_source_ts_ns(self,
        *,
        exchange_ts_ns: int,
        provider_receive_ts_ns: int,
    ) -> int:
        if self.state.dynamic_fill_hazard_visibility_clock_mode == "aws_profile":
            return int(exchange_ts_ns)
        receive_ns = int(provider_receive_ts_ns or 0)
        return receive_ns if receive_ns > 0 else int(exchange_ts_ns)

    def _append_dynamic_fill_hazard_lifecycle_journal(self,
        order: Mapping[str, Any],
        source_event_type: str,
    ) -> None:
        hazard_id = str(order.get("trace_id", ""))
        lifecycle = self.state.dynamic_fill_hazard_lifecycles.get(hazard_id)
        if lifecycle is None:
            return
        rows = self.services.emit_diagnostic_lifecycle_rows(
            emitters=self.state.dynamic_fill_hazard_lifecycle_emitters,
            lifecycle=lifecycle,
            runtime_source="replay",
            source_event_type=str(source_event_type),
            client_order_id=hazard_id,
            exchange_order_id=int(order.get("trace_id", 0) or 0),
            symbol=str(self.context.params.get("symbol", "BTCUSDC") or "BTCUSDC"),
            side=str(order.get("side", "")),
        )
        self.state.dynamic_fill_hazard_lifecycle_journal.extend(self.services.asdict(row) for row in rows)

    def _dynamic_fill_hazard_cpp_mismatch(self,
        stage: str,
        details: Mapping[str, Any],
    ) -> None:
        self.state.dynamic_fill_hazard_cpp_mismatch_count += 1
        normalized_stage = str(stage)
        self.state.dynamic_fill_hazard_cpp_mismatch_by_stage[normalized_stage] = int(
            self.state.dynamic_fill_hazard_cpp_mismatch_by_stage.get(normalized_stage, 0)
            + 1
        )
        payload = {
            "stage": normalized_stage,
            **{str(key): value for key, value in details.items()},
        }
        if (
            len(self.state.dynamic_fill_hazard_cpp_mismatch_trace)
            < self.state.dynamic_fill_hazard_cpp_mismatch_trace_max
        ):
            self.state.dynamic_fill_hazard_cpp_mismatch_trace.append(payload)
        if self.state.dynamic_fill_hazard_cpp_parity_strict:
            raise RuntimeError(
                "dynamic fill-hazard Python/C++ parity mismatch: "
                + self.services.json.dumps(payload, sort_keys=True, default=str)
            )

    def _dynamic_fill_hazard_cpp_check(self,
        condition: bool,
        stage: str,
        **details: Any,
    ) -> None:
        if not condition:
            self._dynamic_fill_hazard_cpp_mismatch(stage, details)

    def _dynamic_fill_hazard_float_equal(self,
        left: Any,
        right: Any,
        *,
        rtol: float = 2e-11,
        atol: float = 1e-12,
    ) -> bool:
        try:
            lhs = float(left)
            rhs = float(right)
        except (TypeError, ValueError):
            return False
        if self.services.math.isnan(lhs) or self.services.math.isnan(rhs):
            return self.services.math.isnan(lhs) and self.services.math.isnan(rhs)
        return bool(self.services.math.isclose(lhs, rhs, rel_tol=rtol, abs_tol=atol))

    def _exchange_book_strict_counter_snapshot(self) -> dict[str, int]:
        scheduler_stats = (
            self.state.exchange_book_scheduler.stats()
            if self.state.exchange_book_scheduler is not None
            else None
        )
        return {
            "exchange_book_queue_lookup_count": int(
                self.state.exchange_book_queue_lookup_count
            ),
            "exchange_book_queue_exact_count": int(
                self.state.exchange_book_queue_exact_count
            ),
            "exchange_book_queue_known_zero_count": int(
                self.state.exchange_book_queue_known_zero_count
            ),
            "exchange_book_queue_missing_count": int(
                self.state.exchange_book_queue_missing_count
            ),
            "exchange_book_queue_invalidated_order_count": int(
                self.state.exchange_book_queue_invalidated_order_count
            ),
            "exchange_book_queue_ambiguous_event_count": int(
                self.state.exchange_book_queue_ambiguous_event_count
            ),
            "exchange_book_cancel_trade_ambiguous_order_count": int(
                self.state.exchange_book_cancel_trade_ambiguous_order_count
            ),
            "exchange_book_cancel_book_ambiguous_order_count": int(
                self.state.exchange_book_cancel_book_ambiguous_order_count
            ),
            "exchange_book_events_consumed": int(
                scheduler_stats.consumed_events if scheduler_stats else 0
            ),
            "exchange_book_events_accepted": int(
                scheduler_stats.accepted_events if scheduler_stats else 0
            ),
            "exchange_book_events_rejected": int(
                scheduler_stats.rejected_events if scheduler_stats else 0
            ),
            "exchange_book_source_gap_events": int(
                scheduler_stats.source_gap_events if scheduler_stats else 0
            ),
            "exchange_book_invalid_sequence_messages": int(
                scheduler_stats.invalid_sequence_messages
                if scheduler_stats
                else 0
            ),
            "exchange_book_sequence_gaps": int(
                scheduler_stats.sequence_gaps if scheduler_stats else 0
            ),
            "exchange_book_message_time_reversals": int(
                scheduler_stats.message_time_reversals
                if scheduler_stats
                else 0
            ),
            "exchange_book_event_timestamp_fallback_events": int(
                scheduler_stats.event_timestamp_fallback_events
                if scheduler_stats
                else 0
            ),
            "exchange_book_receive_timestamp_fallback_events": int(
                scheduler_stats.receive_timestamp_fallback_events
                if scheduler_stats
                else 0
            ),
            "exchange_book_unknown_timestamp_source_events": int(
                scheduler_stats.unknown_timestamp_source_events
                if scheduler_stats
                else 0
            ),
        }
