"""Replay policy setup owner; venue/input adaptation stays explicit."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from research.families.f07_active_order_continuation.audit.queue_value_models import QueueReactiveHawkesArtifact


class PolicySetup:
    def _queue_runtime_event_source(self,
        artifact: QueueReactiveHawkesArtifact,
    ) -> str:
        event_columns = dict(artifact.event_columns)
        if event_columns == self.services.NATIVE_EXCHANGE_EVENT_COLUMNS:
            if self.state.exchange_book_scheduler is None:
                raise ValueError(
                    "native queue-value artifact requires the "
                    "strategy-independent exchange-book scheduler"
                )
            return "native_exchange_exact_level"
        if event_columns == self.services.EVENT_COLUMNS:
            return "policy_visible_top_book"
        raise ValueError(
            "queue-value artifact uses an unsupported event-source mapping: "
            f"{event_columns}"
        )

    def _post_cooldown_budget_enabled_for_side(self, side: str) -> bool:
        return bool(
            self.state.post_cooldown_inventory_budget_enabled
            and self.state.post_cooldown_inventory_budget_target_side
            in {"BOTH", str(side).upper()}
        )

    def _conditional_p3_status_for(self,
        *,
        side: str,
        opener: bool,
        distance_ticks: int,
        prediction_index: int,
        prediction_ready: bool,
    ) -> int:
        if not self.state.conditional_p3_reach_gate_enabled or not prediction_ready:
            return 0
        offset = int(distance_ticks) - self.state.conditional_p3_reach_gate_grid_min_ticks
        if offset < 0 or offset >= self.state.conditional_p3_reach_gate_grid_size:
            return 0
        block = {
            ("BUY", True): 0,
            ("BUY", False): 1,
            ("SELL", True): 2,
            ("SELL", False): 3,
        }[(str(side).upper(), bool(opener))]
        return int(
            self.state.conditional_p3_reach_gate_status[
                int(prediction_index),
                block * self.state.conditional_p3_reach_gate_grid_size + offset,
            ]
        )

    def _loss_cooldown_on_fill(self,
        side: str,
        fill_qty: float,
        fill_price: float,
        fee_rate: float,
    ) -> None:
        self.state.consecutive_loss_cooldown.on_fill(
            side=side,
            quantity=float(fill_qty),
            price=float(fill_price),
            commission=float(fill_price) * float(fill_qty) * float(fee_rate),
        )
        if self.state.hard_risk_enabled:
            self.state.risk_state.observe(self.state.hard_risk_clock_ts_ms, self.state.cash, self.state.q, self.state.hard_risk_mark_price)
        if abs(float(self.state.consecutive_loss_cooldown.inventory) - float(self.state.q)) > max(
            1e-10,
            float(self.state.LOT_SIZE) * 1e-7,
        ):
            raise RuntimeError(
                "consecutive-loss replay inventory diverged from execution state"
            )
