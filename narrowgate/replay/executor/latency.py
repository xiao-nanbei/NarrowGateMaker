"""Replay latency owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import numpy as np


class LatencyAdapter:
    def _sample_latency_ms(self,
        base_ms: int,
        samples_ms: Optional[np.ndarray] = None,
        *,
        event_ts: int,
        side: str,
        operation: int,
        order_ts: int = 0,
    ) -> int:
        return self.services.deterministic_latency_ms(
            base_ms=base_ms,
            jitter_ms=self.state.latency_jitter_ms,
            samples_ms=(
                samples_ms
                if samples_ms is not None
                else self.services.np.empty(0, dtype=self.services.np.float64)
            ),
            seed=self.state.latency_seed,
            event_ts_ms=int(event_ts),
            side=side,
            operation=int(operation),
            order_ts_ms=int(order_ts),
            stress_enabled=self.state.latency_stress_enabled,
            stress_spike_probability=self.state.latency_stress_spike_probability,
            stress_spike_multiplier=self.state.latency_stress_spike_multiplier,
            samples_validated=True,
        )

    def _decision_gateway_request_ts(self,
        decision_ts: int, *, apply_decision_compute: bool = True
    ) -> int:
        if not self.state.decision_to_gateway_latency_enabled or not apply_decision_compute:
            return int(decision_ts)
        delay_ms = self.services._deterministic_decision_to_gateway_latency_ms(
            self.state.decision_to_gateway_latency_samples_ms,
            seed=int(self.state.decision_to_gateway_latency_seed),
            decision_ts_ms=int(decision_ts),
        )
        return int(decision_ts) + int(delay_ms)

    def _prepare_rest_gateway_timing(self,
        *,
        decision_ts: int,
        requested_slots: tuple[str, ...],
        apply_decision_compute: bool = False,
    ) -> None:
        if not self.state.rest_gateway_timing_enabled:
            return
        if self.state.sampled_serial_gateway:
            origin = self._decision_gateway_request_ts(
                decision_ts, apply_decision_compute=apply_decision_compute
            )
            self.state.active_rest_gateway_timing = {
                slot: {"request_origin_ts_ms": origin} for slot in requested_slots
            }
            self.state.rest_gateway_plan_active = True
            self.state.rest_gateway_last_slot_index = -1
            return
        if self.state.rest_gateway_timing_profile is None:
            raise RuntimeError("serial REST gateway timing profile is unavailable")
        self.state.active_rest_gateway_timing = self.services._sample_serial_rest_gateway_timing_row(
            self.state.rest_gateway_timing_profile,
            decision_ts_ms=int(decision_ts),
            request_origin_ts_ms=self._decision_gateway_request_ts(
                decision_ts, apply_decision_compute=apply_decision_compute
            ),
            requested_slots=tuple(requested_slots),
            seed=int(self.state.rest_gateway_timing_seed),
        )
        self.state.rest_gateway_plan_active = True
        if self.state.active_rest_gateway_timing:
            self.state.rest_gateway_sampled_row_count += 1

    def _clear_rest_gateway_timing(self) -> None:
        self.state.active_rest_gateway_timing = {}
        self.state.rest_gateway_plan_active = False
        self.state.rest_gateway_last_slot_index = -1

    def _rest_gateway_slot_timing(self,
        slot: str,
        *,
        request_ts: int,
        side: str,
        operation: int,
        order_ts: int,
        apply_decision_compute: bool = False,
    ) -> dict[str, Any]:
        normalized_slot = str(slot)
        if not self.state.rest_gateway_timing_enabled:
            raise RuntimeError("serial REST gateway timing is disabled")
        if self.state.sampled_serial_gateway:
            if self.state.rest_gateway_plan_active:
                if normalized_slot not in self.state.active_rest_gateway_timing:
                    raise RuntimeError("sampled_serial request is outside the decision plan")
                index = self.services._REST_GATEWAY_SLOT_NAMES.index(normalized_slot)
                if index < self.state.rest_gateway_last_slot_index:
                    raise RuntimeError("sampled_serial request order regressed")
                self.state.rest_gateway_last_slot_index = index
                origin = self.state.active_rest_gateway_timing[normalized_slot]["request_origin_ts_ms"]
            else:
                origin = self._decision_gateway_request_ts(
                    request_ts, apply_decision_compute=apply_decision_compute
                )
            is_new = normalized_slot.startswith("new_")
            effective_samples = (
                self.state.new_exchange_effective_latency_samples_ms if is_new
                else self.state.cancel_exchange_effective_latency_samples_ms
            )
            ack_samples = (
                self.state.new_latency_samples_ms if is_new
                else self.state.cancel_ack_visibility_latency_samples_ms
            )
            response_samples = None
            if self.state.serial_rest_return_enabled:
                rows = self.state.rest_return_samples[normalized_slot]
                effective_samples, ack_samples, response_samples = rows.T
            effective_ms = self._sample_latency_ms(
                self.state.new_order_latency_ms if is_new else self.state.cancel_order_latency_ms,
                effective_samples, event_ts=request_ts, side=side,
                operation=operation, order_ts=order_ts,
            )
            # Reuse each request's original keyed draw, with row-aligned ACK.
            # Different requests are independent service-time draws, NOT an
            # observed joint burst or a synthesized missing request mask.
            ack_ms = self.services.deterministic_latency_ms(
                base_ms=self.state.new_order_latency_ms if is_new else 0,
                jitter_ms=self.state.latency_jitter_ms if is_new else 0,
                samples_ms=ack_samples, seed=self.state.latency_seed,
                event_ts_ms=request_ts, side=side, operation=operation,
                order_ts_ms=order_ts, stress_enabled=self.state.latency_stress_enabled,
                stress_spike_probability=self.state.latency_stress_spike_probability,
                stress_spike_multiplier=self.state.latency_stress_spike_multiplier,
                samples_validated=True,
            )
            start = max(int(origin), self.state.rest_gateway_busy_until_ms)
            service_ms = (
                self._sample_latency_ms(
                    0, response_samples, event_ts=request_ts, side=side,
                    operation=operation, order_ts=order_ts,
                )
                if response_samples is not None else max(effective_ms, ack_ms)
            )
            if self.state.async_rest_gateway:
                if self.state.async_bulk_request is not None:
                    raise NotImplementedError(
                        "new callback write admission during a bulk barrier is not modeled"
                    )
                if len(self.state.async_rest_queue) + int(self.state.async_rest_active is not None) >= (
                    self.state.async_lane_capacity + 1
                ):
                    raise RuntimeError("GLOBAL asynchronous order lane is full")
                self.state.async_rest_sequence += 1
                self.state.rest_gateway_last_timing = {
                    "request_ts_ms": self.state.async_rest_unset_ms,
                    "exchange_effective_ts_ms": self.state.async_rest_unset_ms,
                    "local_visibility_ts_ms": self.state.async_rest_unset_ms,
                    "rest_return_ts_ms": self.state.async_rest_unset_ms,
                    "origin_ts_ms": int(origin), "is_new": is_new,
                    "effective_ms": effective_ms, "ack_ms": max(effective_ms, ack_ms),
                    "service_ms": service_ms, "sequence": self.state.async_rest_sequence,
                }
                self.state.async_rest_queue.append(self.state.rest_gateway_last_timing)
                self.state.async_rest_queue_high_watermark = max(
                    self.state.async_rest_queue_high_watermark, len(self.state.async_rest_queue),
                )
                self.state.local_lifecycle_boundary_scheduler.schedule(
                    ts_ms=max(int(origin), self.state.rest_gateway_busy_until_ms), phase="rest_return",
                    event_id=f"async-dispatch:{self.state.async_rest_sequence}",
                    payload={"async_rest_worker": True},
                )
                return self.state.rest_gateway_last_timing
            self.state.rest_gateway_request_count += 1
            self.state.rest_gateway_request_wait_ms += start - int(origin)
            self.state.rest_gateway_busy_ms += service_ms
            self.state.rest_gateway_busy_until_ms = start + service_ms
            self.state.rest_gateway_last_timing = {
                "request_ts_ms": start,
                "exchange_effective_ts_ms": start + effective_ms,
                "local_visibility_ts_ms": start + max(effective_ms, ack_ms),
                "rest_return_ts_ms": self.state.rest_gateway_busy_until_ms,
            }
            return self.state.rest_gateway_last_timing
        if normalized_slot in self.state.active_rest_gateway_timing:
            return self.state.active_rest_gateway_timing[normalized_slot]
        if self.state.rest_gateway_plan_active:
            raise RuntimeError(
                "serial REST gateway action diverged from its sampled request mask: "
                f"missing {normalized_slot}"
            )
        if self.state.rest_gateway_timing_profile is None:
            raise RuntimeError("serial REST gateway timing profile is unavailable")
        sampled = self.services._sample_serial_rest_gateway_timing_row(
            self.state.rest_gateway_timing_profile,
            decision_ts_ms=int(request_ts),
            request_origin_ts_ms=self._decision_gateway_request_ts(
                request_ts, apply_decision_compute=False
            ),
            requested_slots=(normalized_slot,),
            seed=int(self.state.rest_gateway_timing_seed),
        )
        self.state.rest_gateway_sampled_row_count += 1
        return sampled[normalized_slot]

    def _sample_cancel_deadlines_ms(self,
        *,
        request_ts: int,
        side: str,
        operation: int,
        order_ts: int,
        apply_decision_compute: bool = False,
    ) -> tuple[int, int, int]:
        """Return request, exchange-effective, and local cancel deadlines.

        Legacy latency has one boundary, so both timestamps remain identical.
        The optional split profile models exchange fillability separately from
        the later local ACK without changing any existing replay identity.
        """

        if self.state.rest_gateway_timing_enabled:
            slot = "cancel_buy" if str(side).upper() == "BUY" else "cancel_sell"
            timing = self._rest_gateway_slot_timing(
                slot,
                request_ts=int(request_ts),
                side=side, operation=operation, order_ts=order_ts,
                apply_decision_compute=apply_decision_compute,
            )
            return (
                int(timing["request_ts_ms"]),
                int(timing["exchange_effective_ts_ms"]),
                int(timing["local_visibility_ts_ms"]),
            )

        if not self.state.cancel_latency_split_enabled:
            latency_ms = self._sample_latency_ms(
                self.state.cancel_order_latency_ms,
                self.state.cancel_latency_samples_ms,
                event_ts=request_ts,
                side=side,
                operation=operation,
                order_ts=order_ts,
            )
            gateway_request_ts = self._decision_gateway_request_ts(
                request_ts, apply_decision_compute=apply_decision_compute
            )
            deadline = int(gateway_request_ts) + int(latency_ms)
            return int(gateway_request_ts), deadline, deadline

        gateway_request_ts = self._decision_gateway_request_ts(
            request_ts, apply_decision_compute=apply_decision_compute
        )
        effective_samples = (
            self.state.cancel_exchange_effective_latency_samples_ms
            if self.state.cancel_exchange_effective_latency_samples_ms.size > 0
            else self.state.cancel_latency_samples_ms
        )
        effective_latency_ms = self._sample_latency_ms(
            self.state.cancel_order_latency_ms,
            effective_samples,
            event_ts=request_ts,
            side=side,
            operation=operation,
            order_ts=order_ts,
        )
        effective_ts = int(gateway_request_ts) + int(effective_latency_ms)
        if self.state.cancel_ack_visibility_latency_samples_ms.size == 0:
            return int(gateway_request_ts), effective_ts, effective_ts
        # Both arrays are row-aligned measurements from the same request.  Use
        # the identical keyed sample index; an independent draw destroys the
        # observed lifecycle correlation.  ACK samples are request-to-local
        # visibility, not an increment after exchange effectiveness.
        ack_latency_ms = self.services.deterministic_latency_ms(
            base_ms=0,
            jitter_ms=0,
            samples_ms=self.state.cancel_ack_visibility_latency_samples_ms,
            seed=self.state.latency_seed,
            event_ts_ms=int(request_ts),
            side=side,
            operation=int(operation),
            order_ts_ms=int(order_ts),
            stress_enabled=self.state.latency_stress_enabled,
            stress_spike_probability=self.state.latency_stress_spike_probability,
            stress_spike_multiplier=self.state.latency_stress_spike_multiplier,
            samples_validated=True,
        )
        return (
            int(gateway_request_ts),
            effective_ts,
            max(effective_ts, int(gateway_request_ts) + int(ack_latency_ms)),
        )

    def _sample_new_order_deadlines_ms(self,
        *,
        request_ts: int,
        side: str,
        order_ts: int,
        apply_decision_compute: bool = True,
    ) -> tuple[int, int, int]:
        if self.state.rest_gateway_timing_enabled:
            slot = "new_buy" if str(side).upper() == "BUY" else "new_sell"
            timing = self._rest_gateway_slot_timing(
                slot,
                request_ts=int(request_ts),
                side=side, operation=self.services._LATENCY_NEW, order_ts=order_ts,
                apply_decision_compute=apply_decision_compute,
            )
            return (
                int(timing["request_ts_ms"]),
                int(timing["exchange_effective_ts_ms"]),
                int(timing["local_visibility_ts_ms"]),
            )

        if not self.state.new_order_latency_split_enabled:
            latency_ms = self._sample_latency_ms(
                self.state.new_order_latency_ms,
                self.state.new_latency_samples_ms,
                event_ts=request_ts,
                side=side,
                operation=self.services._LATENCY_NEW,
                order_ts=order_ts,
            )
            gateway_request_ts = self._decision_gateway_request_ts(
                request_ts, apply_decision_compute=apply_decision_compute
            )
            deadline = int(gateway_request_ts) + int(latency_ms)
            return int(gateway_request_ts), deadline, deadline
        gateway_request_ts = self._decision_gateway_request_ts(
            request_ts, apply_decision_compute=apply_decision_compute
        )
        effective_samples = (
            self.state.new_exchange_effective_latency_samples_ms
            if self.state.new_exchange_effective_latency_samples_ms.size > 0
            else self.state.new_latency_samples_ms
        )
        effective_latency_ms = self._sample_latency_ms(
            self.state.new_order_latency_ms,
            effective_samples,
            event_ts=request_ts,
            side=side,
            operation=self.services._LATENCY_NEW,
            order_ts=order_ts,
        )
        effective_ts = int(gateway_request_ts) + int(effective_latency_ms)
        if self.state.new_latency_samples_ms.size == 0:
            return int(gateway_request_ts), effective_ts, effective_ts
        ack_latency_ms = self._sample_latency_ms(
            self.state.new_order_latency_ms,
            self.state.new_latency_samples_ms,
            event_ts=request_ts,
            side=side,
            operation=self.services._LATENCY_NEW,
            order_ts=order_ts,
        )
        return (
            int(gateway_request_ts),
            effective_ts,
            max(
                effective_ts,
                int(gateway_request_ts) + int(ack_latency_ms),
            ),
        )

    def _sample_private_fill_visible_ts(self,
        *,
        exchange_ts: int,
        side: str,
        order_ts: int,
    ) -> int:
        if not self.state.private_fill_visibility_enabled:
            return int(exchange_ts)
        latency_ms = self._sample_latency_ms(
            0,
            self.state.private_fill_visibility_latency_samples_ms,
            event_ts=int(exchange_ts),
            side=str(side),
            operation=self.services._LATENCY_PRIVATE_FILL,
            order_ts=int(order_ts),
        )
        return int(exchange_ts) + int(latency_ms)

    def _order_cancel_effective_ts(self, order: Mapping[str, Any]) -> int:
        effective_ts = int(order.get("cancel_effective_ts", -1) or -1)
        if effective_ts > 0:
            return effective_ts
        return int(order.get("cancel_ts", -1) or -1)

    def _order_exchange_fillable(self, order: Mapping[str, Any], ts_ms: int) -> bool:
        effective_ts = self._order_cancel_effective_ts(order)
        if effective_ts <= 0 or int(ts_ms) < effective_ts:
            return True
        # Preserve the legacy same-millisecond trade/cancel ambiguity order.
        # The native exchange-book path lets the crossing trade run before a
        # single-boundary ACK at the same millisecond, then censors that path
        # from formal evidence. Split profiles have a real exchange-effective
        # boundary and therefore stop matching at that boundary.
        return bool(
            not self.state.cancel_latency_split_enabled
            and int(ts_ms) == effective_ts
            and int(order.get("cancel_ts", -1) or -1) == effective_ts
        )
