"""New-source placement features and censored, effective-time risk records."""

import pandas as pd

from data.feature_cursor import FeatureCursor
from models.replay.order_exposure import order_exposure


def replay_placement_strategy(root, *, contract, **kwargs):
    """Explicit new F06 distance rules, not an implicitly selected old CIF."""
    from models.replay.public_strategy import replay_configured_strategy

    if contract.get("family") != "F06":
        raise ValueError("F06 action contract required")
    return replay_configured_strategy(root, contract=contract, **kwargs)


def build_placement_panel(root, placements, events, *, feature_columns, missing_policy,
                          max_age_ns, observation_end_ns):
    cursor = FeatureCursor(root)
    if not {"order_id", "decision_ns", "quantity"} <= set(placements):
        raise ValueError("placement identity, decision and quantity required")
    if placements["order_id"].duplicated().any():
        raise ValueError("duplicate placement identity")
    events = list(events)
    cursor.require_binding(placements.to_dict("records"))
    cursor.require_binding(events)
    if set(feature_columns) & set(placements):
        raise ValueError("feature names collide with placement fields")
    grouped = {}
    for event in events:
        grouped.setdefault(event["order_id"], []).append(event)
    if set(grouped) - set(placements["order_id"]):
        raise ValueError("lifecycle refers to unknown placement")
    rows = []
    for placement in placements.to_dict("records"):
        exposure = order_exposure(grouped.get(placement["order_id"], []),
                                  order_id=placement["order_id"], observation_end_ns=observation_end_ns,
                                  initial_quantity=placement["quantity"])
        if exposure["active_ns"] < placement["decision_ns"]:
            raise ValueError("placement activated before decision")
        features = cursor.row(placement["decision_ns"], columns=feature_columns,
                              missing_policy=missing_policy, max_age_ns=max_age_ns)
        rows.append({**placement, **exposure, **features})
    result = pd.DataFrame(rows)
    result["first_fill_ns"] = pd.array([row["first_fill_ns"] for row in rows], dtype="Int64")
    result.attrs.update(input_manifest_id=cursor.input_manifest_id,
                        input_contract_id=cursor.bundle.manifest["input_contract_id"],
                        native_queue_parity="not_proven", missing_policy=missing_policy)
    return result


def placement_risk_targets(panel, *, horizon_ns):
    """First-fill / effective-cancel competing events; unobserved time is censored."""
    if type(horizon_ns) is not int or horizon_ns <= 0:
        raise ValueError("positive integer risk horizon required")
    if not panel.attrs.get("input_manifest_id"):
        raise ValueError("bound placement panel required")
    rows = []
    for item in panel.to_dict("records"):
        deadline = item["active_ns"] + horizon_ns
        end = min(deadline, item["exposure_end_ns"])
        kind = "censored"
        first_fill = item["first_fill_ns"]
        if pd.notna(first_fill) and first_fill < deadline and first_fill <= end:
            end, kind = first_fill, "fill"
        elif item["terminal_reason"] == "cancel" and item["exposure_end_ns"] < deadline:
            kind = "cancel"
        elif item["exposure_end_ns"] >= deadline:
            kind = "horizon_complete"
        rows.append({**item, "risk_end_ns": end, "exposure_ns": end - item["active_ns"],
                     "event_kind": kind, "fill_by_horizon": 1 if kind == "fill" else
                     0 if kind in {"cancel", "horizon_complete"} else None})
    result = pd.DataFrame(rows)
    result.attrs.update(panel.attrs)
    return result
