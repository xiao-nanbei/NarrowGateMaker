"""Rotate overlapping input windows without restarting the tick-loop state.

This module owns array/cursor translation only, never economic initialization.
Callers retain enough actual pre-roll for every input consumer. Emitted decision
trace generations remain global; internal array cursors are window-local.
"""

from __future__ import annotations

import numpy as np


_CLOCK_FIELDS = {
    "trade": ("trade_ts", ("trade_price", "trade_qty", "maker_event_code", "is_execution_trade")),
    "seed": ("trade_ts_seed", ("queue_base_arr", "queue_decay_arr", "buy_fill_prob_arr", "sell_fill_prob_arr")),
    "bbo": ("bbo_ts", ("bbo_best_bid", "bbo_best_ask", "bbo_bid_qty", "bbo_ask_qty", "bbo_observation_us", "bbo_usable")),
    "l2": ("l2_ts", ("l2_bid_px", "l2_bid_qty", "l2_ask_px", "l2_ask_qty", "l2_observation_us")),
    "variance": ("loaded_variance_ts_ms", ()),
    "prediction": ("ml_ts", ("ml_dir", "ml_vol", "ml_ret", "ml_tox_bid", "ml_tox_ask")),
}

_CURSORS = {
    "trade": ("i", "last_processed_event_idx"),
    "bbo": ("bbo_idx", "decision_bbo_idx", "active_decision_bbo_idx"),
    "l2": ("l2_idx", "decision_l2_idx", "l2_idx_before_event"),
    "variance": ("var_idx", "main_loop_rq_var_idx"),
    "prediction": ("ml_idx", "ranked_toxicity_guard_last_prediction_idx"),
}

_DERIVED_INPUTS = (
    "is_seller", "policy_visible_execution", "exec_message_trade_event_rows",
    "execution_qty", "taker_buy_qty_cumulative", "taker_sell_qty_cumulative",
    "last_execution_event_index", "exec_message_schedules", "exec_message_payload",
    "raw_delivery", "aligned_sources", "child_rows", "visible_children",
    "parents_with_children", "quote_ti_source_indices", "ml_xmarket",
    "ml_prediction_features", "conditional_p3_reach_gate_ts", "conditional_p3_reach_gate_status",
    "n_trades", "book_state_resolution_ms", "quote_core_params",
)

# These are initialization-validation temporaries, not input consumers. In
# particular a saved ``schedule``/``rows`` alias kept the first input window
# alive even after the authoritative input fields had been rotated.
_INPUT_VALIDATION_SCRATCH = (
    "source_ts", "positive_deltas", "aligned_sources", "rows", "schedule",
    "expected", "child_rows", "visible_children", "parents_with_children",
    "mark_prices", "child_exchange_ns",
)


def release_runtime_input_scratch(runtime) -> None:
    """Release initialization-only arrays, including those in v1 checkpoints."""
    for name in _INPUT_VALIDATION_SCRATCH:
        vars(runtime).pop(name, None)


def ensure_runtime_sample_capacity(runtime) -> None:
    """Grow actual path output in small blocks, never by elapsed calendar time.

    All recorded observations remain available to the unchanged final path
    statistics/output. Only speculative unused capacity is bounded here; this
    does not discard history or claim a bounded size for complete result data.
    Old checkpoints with larger preallocated arrays remain accepted.
    """
    if int(runtime.si) < int(runtime.max_samples):
        return
    capacity = int(runtime.si) + 4096
    for name in ("pnl_arr", "inv_arr", "ts_arr"):
        original = getattr(runtime, name)
        expanded = np.empty(capacity, dtype=original.dtype)
        expanded[:runtime.si] = original[:runtime.si]
        setattr(runtime, name, expanded)
    runtime.max_samples = capacity


def compact_runtime_checkpoint(runtime):
    """Shallow copy live state and copy only the written sampling prefix.

    The order/FIFO/RNG/campaign graph is unchanged. The enclosing checkpoint
    pickler still preserves its ownership aliases. Copying array slices here
    also releases their unused backing allocation, unlike a NumPy view.
    """
    from copy import copy

    captured = copy(runtime)
    release_runtime_input_scratch(captured)
    for name in ("pnl_arr", "inv_arr", "ts_arr"):
        setattr(captured, name, getattr(runtime, name)[:runtime.si].copy())
    captured.max_samples = int(runtime.si)
    return captured


def runtime_input_context_start(runtime, default_start: int, context_ms: int) -> int:
    """Retain actual rows still needed after a long pause in quote evaluation.

    The configured pre-roll is a minimum, not permission to discard a dormant
    consumer's cursor. In particular variance/BER catch-up must see every bar
    since its last evaluation. This only widens the loaded input, never advances
    a cursor, emits an observation, or changes the next checkpoint time.
    """
    start = int(default_start)

    def retain(value, clock):
        nonlocal start
        rows = getattr(runtime, _CLOCK_FIELDS[clock][0], None)
        if rows is None or not len(rows) or value is None:
            return
        index = int(value)
        if index < 0:
            return
        if index >= len(rows):
            raise ValueError(f"{clock} saved cursor lies beyond its input window")
        start = min(start, int(rows[index]) - context_ms)

    for clock, names in _CURSORS.items():
        for name in names:
            if name == "main_loop_rq_var_idx" and not (runtime.main_loop_enabled and runtime.dynamic_rq):
                continue
            value = getattr(runtime, name, None)
            # These incremental consumers have not consumed their first row.
            if value == -1 and (name == "var_idx" or (
                name == "ranked_toxicity_guard_last_prediction_idx"
                and runtime.ranked_toxicity_guard_binding_active
            )):
                value = 0
            retain(value, clock)

    seen = set()

    def visit(value):
        if value is None or id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, dict):
            retain(value.get("queue_l2_seen_idx"), "l2")
            retain(value.get("trade_idx"), "trade")
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            for child in value:
                visit(child)
        elif value is runtime.local_lifecycle_boundary_scheduler:
            visit(vars(value))

    for name in ("bid_orders", "ask_orders", "local_lifecycle_boundary_scheduler",
                 "serial_rest_decision", "pending_quote_compute"):
        visit(getattr(runtime, name))
    # Never reintroduce a prefix discarded by an earlier successful rotation.
    # Its consumers have already been retained/advanced in the saved state.
    return max(start, int(runtime.trade_ts[0]))


def _overlap_offset(old, new, name, through_ts):
    if old is None or new is None:
        if old is not None or new is not None:
            raise ValueError(f"input rotation cannot change the presence of {name}")
        return 0, 0
    if not len(old) or not len(new):
        if len(old) != len(new):
            raise ValueError(f"input rotation cannot drop/add an empty {name} clock")
        return 0, 0
    if new[0] < old[0]:
        raise ValueError(f"{name} input window moved backwards")
    offset = int(np.searchsorted(old, new[0], side="left"))
    # A bounded batch may add an unused terminal timer to its lookahead. It is
    # not part of the consumed prefix and must not become a real lifecycle event
    # when the next batch provides the continuation. Verify the prefix through
    # the selected next event, which is the exact saved execution boundary.
    count = min(int(np.searchsorted(old, through_ts, side="right")) - offset, len(new))
    if count <= 0 or not np.array_equal(old[offset:offset + count], new[:count]):
        raise ValueError(f"{name} input windows need an unchanged overlapping clock")
    return offset, count


def rotate_runtime_inputs(saved, fresh, next_event, *, exchange_book_event_tape=None):
    """Replace input arrays; keep orders, accounting, RNG and scheduling state.

    The incoming window includes the cutoff and real overlapping context.
    Native sources resume at their file cursor, without re-applying snapshots or
    increments to the saved book or restarting queue ownership.
    """
    if (saved.exchange_book_scheduler is None) != (fresh.exchange_book_scheduler is None):
        raise ValueError("input rotation cannot change native book availability")
    mappings = {}
    for name, (clock, values) in _CLOCK_FIELDS.items():
        old, new = getattr(saved, clock, None), getattr(fresh, clock, None)
        offset, count = _overlap_offset(old, new, name, int(next_event[1]))
        mappings[name] = offset
        for field in values:
            a, b = getattr(saved, field, None), getattr(fresh, field, None)
            if a is None or b is None:
                if a is not None or b is not None:
                    raise ValueError(f"input rotation cannot change {field} availability")
            elif count and not np.array_equal(a[offset:offset + count], b[:count], equal_nan=True):
                raise ValueError(f"input rotation changed overlapping {field} values")

    def translate(value, clock, *, boundary=False):
        if value is None or int(value) < 0:
            return value
        translated = int(value) - mappings[clock]
        if translated < 0:
            raise ValueError(f"{clock} window discarded context still referenced by runtime")
        new_clock = getattr(fresh, _CLOCK_FIELDS[clock][0])
        if translated >= len(new_clock) + int(boundary):
            raise ValueError(f"{clock} cursor lies beyond the new input window")
        return translated

    # Sampling capacity grows only when a real observation is emitted. A long
    # source gap/new end time must not allocate a speculative calendar prefix.
    release_runtime_input_scratch(saved)

    previous_end = int(saved.trade_ts[-1])
    added_execution = int(np.count_nonzero(
        fresh.is_execution_trade & (fresh.trade_ts > previous_end)
    ))
    saved.n_execution_trades += added_execution
    saved.input_window_count += 1
    saved.input_event_offset += mappings["trade"]
    for name, offset in mappings.items():
        saved.input_clock_offsets[name] += offset
    for clock, names in _CURSORS.items():
        for name in names:
            if hasattr(saved, name):
                if name == "main_loop_rq_var_idx" and not (saved.main_loop_enabled and saved.dynamic_rq):
                    continue
                setattr(saved, name, translate(getattr(saved, name), clock))
    saved.replay_event_cursor["index"] = translate(saved.replay_event_cursor["index"], "trade", boundary=True)
    next_event = (translate(next_event[0], "trade"), *next_event[1:])

    # Mutable pending payloads share actual order objects. Traverse only live
    # scheduler/ownership graphs, not historical trace dictionaries.
    seen = set()

    def visit(value):
        if value is None:
            return
        if id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, dict):
            for key in ("queue_l2_seen_idx",):
                if key in value:
                    value[key] = translate(value[key], "l2")
            if "trade_idx" in value:
                value["trade_idx"] = translate(value["trade_idx"], "trade")
            for child in value.values():
                visit(child)
        elif isinstance(value, (tuple, list)):
            for child in value:
                visit(child)
        elif value is saved.local_lifecycle_boundary_scheduler:
            visit(vars(value))

    for name in ("bid_orders", "ask_orders", "local_lifecycle_boundary_scheduler",
                 "serial_rest_decision", "pending_quote_compute"):
        visit(getattr(saved, name))

    if saved.quote_ti_cursor:
        completed_index = saved.quote_ti_source_indices[saved.quote_ti_cursor - 1]
        completed_time = saved.loaded_variance_ts_ms[completed_index]
        saved.quote_ti_cursor = int(np.searchsorted(
            fresh.loaded_variance_ts_ms[fresh.quote_ti_source_indices], completed_time, side="right",
        ))
    for clock, values in _CLOCK_FIELDS.values():
        for field in (clock, *values):
            if hasattr(fresh, field):
                setattr(saved, field, getattr(fresh, field))
    for name in _DERIVED_INPUTS:
        if hasattr(fresh, name):
            setattr(saved, name, getattr(fresh, name))
    if saved.exchange_book_scheduler is not None:
        saved.exchange_book_scheduler.resume_input_source(exchange_book_event_tape)
    if saved.cooldown_duration_policy_evaluator is not None:
        # The emitter and evaluator are aliases to one stateful live adapter.
        saved.cooldown_duration_policy_evaluator.resume_input_window(
            fresh.cooldown_duration_policy_evaluator,
            before_ts_ns=int(next_event[1]) * 1_000_000,
        )
        for name in ("cooldown_duration_policy_evaluator", "cooldown_v2_snapshot_emitter"):
            if name in saved.quote_core_params:
                saved.quote_core_params[name] = getattr(saved, name)
    return next_event
