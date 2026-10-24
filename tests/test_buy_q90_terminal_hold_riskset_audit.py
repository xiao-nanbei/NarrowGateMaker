from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_terminal_hold_riskset_audit as audit,
)


def test_terminal_invalid_classification_never_reenters_active_riskset() -> None:
    invalid = pd.DataFrame(
        {
            "timestamp": [10.2, 10.3, 10.4],
            "client_order_id": ["a", "a", "b"],
            "order_price": [101.0, 99.0, 98.0],
            "mid": [100.0, 100.0, 100.0],
        }
    )
    classified = audit._classify_invalid_rows(
        invalid,
        {"a": 10.1, "b": 10.5},
    )

    assert classified.loc[0, "invalid_class"] == "terminal_price_above_best_bid"
    assert (
        classified.loc[1, "invalid_class"]
        == "terminal_price_outside_book_unresolved_without_best_bid"
    )
    assert (
        classified.loc[2, "invalid_class"]
        == "active_price_outside_snapshot_range_unresolved_without_floor"
    )
    assert classified["active_fill_riskset_violation"].tolist() == [
        True,
        True,
        False,
    ]


def test_journal_separates_exchange_terminal_hold_and_recovery() -> None:
    shadow = pd.DataFrame(
        {
            "timestamp": [10.0, 10.2, 10.3, 20.0, 20.2],
            "client_order_id": ["a", "a", "a", "b", "b"],
            "valid": [1, 0, 1, 1, 0],
            "reason": ["ok", "order_price_outside_deep_book", "ok", "ok", "order_price_outside_deep_book"],
            "executed_action": ["cancel", "hold_invalid", "baseline_reenter", "cancel", "hold_invalid"],
            "order_price": [101.0, 101.0, 101.0, 99.0, 99.0],
            "mid": [100.0, 99.0, 101.0, 100.0, 98.0],
        }
    )
    actions = pd.DataFrame(
        {
            "timestamp": [10.0, 10.3, 20.0],
            "client_order_id": ["a", "a", "b"],
            "event": ["cancel_request", "score_recovered", "cancel_request"],
            "adverse_value": [0.01, 0.001, 0.02],
            "entry_threshold": [0.005, 0.005, 0.005],
            "order_state": ["PENDING_CANCEL", "CANCELED", "PENDING_CANCEL"],
            "cancel_succeeded": [1, 1, 1],
            "inventory_role": ["opener", "opener", "add"],
        }
    )
    outcomes = pd.DataFrame(
        {
            "timestamp": [10.1, 20.1],
            "client_order_id": ["a", "b"],
            "event_type": ["canceled", "canceled"],
            "price": [101.0, 99.0],
            "age_ms": [100.0, 100.0],
        }
    )

    journal = audit._build_journal(
        shadow,
        actions,
        outcomes,
        {"a", "b"},
        30.0,
    )

    a_events = journal.loc[journal["client_order_id"].eq("a"), "event"].tolist()
    b_events = journal.loc[journal["client_order_id"].eq("b"), "event"].tolist()
    assert "cancel_ack_exchange_terminal" in a_events
    assert "score_recovered_from_terminal_active_riskset" in a_events
    assert "release_and_baseline_reentry" in a_events
    assert "day_end_censor_terminal_hold" in b_events
    terminal = journal.loc[
        journal["event"].eq("cancel_ack_exchange_terminal")
    ]
    assert set(terminal["exchange_order_state"]) == {"terminal_canceled"}
    assert set(terminal["q90_hold_state"]) == {"terminal_hold"}


def test_spec_rejects_any_permission(tmp_path: Path) -> None:
    spec = {
        "schema_version": audit.SCHEMA_VERSION,
        "identity": audit.IDENTITY,
        "status": "frozen_before_journal_generation",
        "economic_outputs_prohibited": True,
        "permissions": {"prediction_supported": True},
    }
    spec["canonical_spec_sha256"] = audit.canonical_spec_sha256(spec)
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")

    try:
        audit.load_spec(path)
    except ValueError as exc:
        assert "cannot grant permissions" in str(exc)
    else:
        raise AssertionError("permission-bearing audit spec must fail closed")
