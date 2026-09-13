"""Continuation diagnostics on new-source explicitly recorded order events."""

from models.replay.order_exposure import order_exposure
from data.feature_cursor import FeatureCursor


def replay_continuation_strategy(root, *, contract, **kwargs):
    """Explicit feature-driven keep/cancel rules through the shared executor."""
    from models.replay.public_strategy import replay_configured_strategy

    if contract.get("family") != "F07":
        raise ValueError("F07 action contract required")
    return replay_configured_strategy(root, contract=contract, **kwargs)


def continuation_report(root, events, *, order_id, initial_quantity, observation_end_ns):
    cursor = FeatureCursor(root)
    events = list(events)
    cursor.require_binding(events)
    exposure = order_exposure(events, order_id=order_id, initial_quantity=initial_quantity,
                              observation_end_ns=observation_end_ns)
    return {"schema": "active_order_continuation.observed_lifecycle.v1", **exposure,
            "input_manifest_id": cursor.input_manifest_id,
            "queue_model": "unobserved_requires_explicit_model",
            "native_queue_closed": False, "economic_evaluation": "not_run"}
