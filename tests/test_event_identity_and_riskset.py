import pandas as pd

from research.families.f08_side_taker_lifecycle.audit.event_identity_and_riskset import (
    audit_event_identity_and_risksets,
)


def _complete_panel() -> pd.DataFrame:
    decision = 1_000_000_000
    rows = []
    events = (
        ("favorable_fill", decision + 100, 0, 0, 0),
        ("cancel", 0, decision + 200, 0, 0),
        ("adverse_price_jump", 0, 0, decision + 300, 0),
        ("campaign_repair", 0, 0, 0, decision + 400),
    )
    for index, (event, fill_ts, cancel_ts, jump_ts, repair_ts) in enumerate(events):
        first_ts = max(fill_ts, cancel_ts, jump_ts, repair_ts)
        rows.append(
            {
                "day": "2026-01-01",
                "decision_id": f"d{index}",
                "order_id": f"o{index}",
                "campaign_id": f"c{index}",
                "side": "BUY",
                "decision_ts_ns": decision,
                "feature_ready_ts_ns": decision,
                "censor_ts_ns": decision + 1_000,
                "risk_interval_start_ts_ns": decision,
                "risk_interval_end_ts_ns": decision + 1_000,
                "order_activation_ts_ns": decision,
                "remaining_qty_start": 0.001,
                "remaining_qty_end": 0.0 if fill_ts else 0.001,
                "fill_ts_ns": fill_ts,
                "fill_event_seq": 1 if fill_ts else 0,
                "fill_qty": 0.001 if fill_ts else 0.0,
                "fill_is_partial": 0,
                "remaining_qty_after_fill": 0.0 if fill_ts else 0.001,
                "cancel_request_ts_ns": decision + 150 if cancel_ts else 0,
                "cancel_request_event_seq": 1 if cancel_ts else 0,
                "cancel_ack_ts_ns": cancel_ts,
                "cancel_event_seq": 2 if cancel_ts else 0,
                "remaining_qty_at_cancel_request": 0.001 if cancel_ts else 0.0,
                "remaining_qty_at_cancel_ack": 0.001 if cancel_ts else 0.0,
                "fill_while_cancel_pending_qty": 0.0,
                "future_mid_first_hit_ts_ns": jump_ts,
                "future_mid_first_hit_direction": -1 if jump_ts else 0,
                "future_mid_first_hit_source": "native_snapshot_delta",
                "future_mid_first_hit_event_seq": 1 if jump_ts else 0,
                "same_ms_ordering_resolved": 1,
                "repair_risk_entry_ts_ns": decision,
                "repair_risk_exit_ts_ns": repair_ts or decision + 1_000,
                "repair_at_risk": 1,
                "campaign_active": 1,
                "reducing_quote_active": 1,
                "reducing_quote_eligible": 1,
                "inventory": 0.001,
                "repair_ts_ns": repair_ts,
                "campaign_repair_event_seq": 1 if repair_ts else 0,
                "first_event": event,
                "first_event_ts_ns": first_ts,
                "adverse_price_jump_ts_ns": jump_ts,
                "label_identity": "native_dynamic_multistate.v1",
            }
        )
    return pd.DataFrame(rows)


def test_complete_event_identity_contract_passes() -> None:
    summary = audit_event_identity_and_risksets(_complete_panel())

    assert summary["status"] == "passed"
    assert summary["dynamic_fill_hazard_event_gate_passed"] is True
    assert summary["dynamic_fill_hazard_allowed"] is False
    assert summary["action_family_allowed"] is False


def test_legacy_first_event_panel_fails_closed() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01"],
            "decision_id": ["d1"],
            "order_id": ["o1"],
            "campaign_id": ["c1"],
            "side": ["BUY"],
            "decision_ts_ns": [1_000],
            "feature_ready_ts_ns": [1_000],
            "censor_ts_ns": [2_000],
            "fill_ts_ns": [0],
            "cancel_ack_ts_ns": [1_500],
            "adverse_price_jump_ts_ns": [0],
            "repair_ts_ns": [0],
            "first_event": ["cancel"],
            "label_identity": ["exact_order_id_first_event"],
        }
    )

    summary = audit_event_identity_and_risksets(frame)

    assert summary["status"] == "blocked"
    assert summary["dynamic_fill_hazard_event_gate_passed"] is False
    assert "missing_common_columns" in summary["block_reasons"]
    assert "cancel:missing_required_columns" in summary["block_reasons"]
