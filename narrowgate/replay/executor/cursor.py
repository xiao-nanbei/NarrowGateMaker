"""Replay cursor owner; venue/input adaptation stays explicit."""

from __future__ import annotations


class ReplayCursor:
    def _next_replay_event(self):
        """Select one event using an explicit, checkpointable cursor.

        A wake uses only the last consumed market row. Equal-ms market rows
        precede the wake; source-message readiness still uses strict < now.
        Local callbacks can finish HTTP calls between market rows and schedule
        the next sleep boundary without waiting for a trade or fixed grid.
        Post-event work is explicit because an input cutoff can occur before
        the next call. A suspended generator used to hide this phase and the
        source index outside the runtime state that a checkpoint must retain.
        """
        after_event = self.state.replay_event_cursor["after_event"]
        self.state.replay_event_cursor["after_event"] = None
        if after_event == "resume":
            if self.state.serial_rest_decision is None and self.state.main_loop_next_wake_ms is None:
                self._finish_main_loop_tick(self.state.main_loop_tick_complete_ms)
        elif after_event == "wake":
            if (self.state.main_loop_next_wake_ms is None and self.state.serial_rest_decision is None
                    and self.state.pending_quote_compute is None):
                self._finish_main_loop_tick(self.state.main_loop_tick_complete_ms)
        index = self.state.replay_event_cursor["index"]
        if not self.state.main_loop_enabled:
            if index >= self.state.n_trades:
                return None
            self.state.replay_event_cursor["index"] = index + 1
            return index, int(self.state.trade_ts[index]), True, False, False
        if not self.state.replay_event_cursor["started"]:
            self._schedule_main_loop_tick(int(self.state.trade_ts[0]))
            self.state.replay_event_cursor["started"] = True
            if self.state.quote_schedule is not None:
                if not self.state.exec_message_schedules and (
                    self.state.exec_book_visibility_delay_enabled
                    or self.state.exec_depth_visibility_source_offset_ms
                    or self.state.use_bar_pricing
                ):
                    raise ValueError("state_event requires a supported committed-input clock; sampled visibility is not a publication stream")
                self.state.quote_schedule.publish("BOOTSTRAP", 0)
                self.state.quote_schedule_visible_ms = int(self.state.trade_ts[0]) - 1
        end_ms = int(self.state.trade_ts[-1])
        while True:
            market_ms = int(self.state.trade_ts[index]) if index < self.state.n_trades else end_ms + 1
            wake_ms = self.state.main_loop_next_wake_ms
            resume_ms = (
                int(self.state.pending_quote_compute["capture_ts"])
                if self.state.pending_quote_compute is not None else None
            )
            next_main_ms = resume_ms if resume_ms is not None else wake_ms
            boundary_ms = self.state.local_lifecycle_boundary_scheduler.next_timestamp()
            visible_ms = self._quote_schedule_next_visible(
                int(self.state.quote_schedule_visible_ms)
            ) if self.state.quote_schedule is not None else None
            if (visible_ms is not None and visible_ms <= end_ms
                    and visible_ms < market_ms
                    and (boundary_ms is None or visible_ms < boundary_ms)
                    and (next_main_ms is None or visible_ms < next_main_ms)):
                return max(0, index - 1), visible_ms, False, False, False
            if (
                boundary_ms is not None
                and boundary_ms <= end_ms
                and boundary_ms < market_ms
                and (next_main_ms is None or boundary_ms <= next_main_ms)
            ):
                return max(0, index - 1), int(boundary_ms), False, False, False
            if market_ms <= end_ms and (next_main_ms is None or market_ms <= next_main_ms):
                self.state.replay_event_cursor["index"] = index + 1
                return index, market_ms, True, False, False
            if resume_ms is not None:
                if resume_ms > end_ms:
                    return None
                self.state.replay_event_cursor["after_event"] = "resume"
                return max(0, index - 1), resume_ms, False, False, True
            if wake_ms is None or wake_ms > end_ms:
                return None
            if (self.state.quote_schedule is not None
                    and getattr(self.state, "main_loop_pre_pending", False)):
                self.state.main_loop_pre_pending = False
                self._begin_main_loop_work(int(wake_ms))
                # Selection still orders market/private events before a
                # same-ms wake, including a zero-duration measured pre-work.
                continue
            if self.state.serial_rest_decision is not None:
                raise RuntimeError("main-loop wake overlaps an in-flight quote call")
            if not self.state.async_rest_gateway and wake_ms < self.state.rest_gateway_busy_until_ms:
                self.state.main_loop_next_wake_ms = self.state.rest_gateway_busy_until_ms + self.state.main_loop_sleep_ms
                continue
            self.state.main_loop_next_wake_ms = None
            self.state.main_loop_tick_complete_ms = int(wake_ms)
            self.state.main_loop_tick_count += 1
            self.state.replay_event_cursor["after_event"] = "wake"
            return max(0, index - 1), int(wake_ms), False, True, False
