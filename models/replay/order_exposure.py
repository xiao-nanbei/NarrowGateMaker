"""Observed/simulated order exposure, independent of market-file boundaries."""

import math


def order_exposure(events, *, order_id, observation_end_ns, initial_quantity):
    """Summarize an explicit order lifecycle without reconstructing missing ACKs.

    Events use effective exchange-side times. A cancel request is not a
    terminal event. A source rebase invalidates queue knowledge, not the order.
    A fill after cancellation or before activation is an input error.
    """
    if type(observation_end_ns) is not int or observation_end_ns < 0:
        raise ValueError("integer observation endpoint required")
    if not math.isfinite(initial_quantity) or initial_quantity <= 0:
        raise ValueError("positive initial order quantity required")
    previous = -1
    active = first_fill = terminal = None
    filled = 0.0
    pending_cancel = False
    queue_unknown = True  # L2 is not an observed queue position.
    reason = "right_censored"
    for event in events:
        if event["order_id"] != order_id:
            raise ValueError("mixed order identities")
        clock, kind = event["effective_ns"], event["kind"]
        if type(clock) is not int or clock < previous:
            raise ValueError("order event clocks regress or are not integer nanoseconds")
        previous = clock
        if clock >= observation_end_ns:
            continue  # half-open exposure; next interval owns boundary events
        if terminal is not None:
            raise ValueError("event after terminal order state")
        if kind == "active":
            if active is not None:
                raise ValueError("duplicate order activation")
            active = clock
        elif kind == "cancel_requested":
            pending_cancel = True
        elif kind == "cancel_active":
            if active is None or not pending_cancel:
                raise ValueError("cancel effect lacks active order/request")
            terminal, reason, pending_cancel = clock, "cancel", False
        elif kind == "fill":
            if active is None:
                raise ValueError("fill before order activation")
            quantity = float(event["quantity"])
            if not math.isfinite(quantity) or quantity <= 0 or filled + quantity > initial_quantity + 1e-12:
                raise ValueError("invalid fill quantity")
            filled += quantity
            first_fill = clock if first_fill is None else first_fill
            if math.isclose(filled, initial_quantity, rel_tol=0, abs_tol=1e-12):
                terminal, reason, pending_cancel = clock, "filled", False
        elif kind == "snapshot_rebase":
            queue_unknown = True
        else:
            raise ValueError(f"unsupported lifecycle event: {kind}")
    if active is None or active >= observation_end_ns:
        raise ValueError("no observed activation in exposure interval")
    return dict(order_id=order_id, active_ns=active, first_fill_ns=first_fill,
                exposure_end_ns=terminal if terminal is not None else observation_end_ns,
                terminal_reason=reason, filled_quantity=filled,
                right_censored=terminal is None, pending_cancel=pending_cancel,
                queue_position_known=not queue_unknown)
