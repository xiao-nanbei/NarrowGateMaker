from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError

import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    CausalMultichannelEmaState,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CONTROL_POLICY_ID,
    HISTORICAL_EXCHANGE_EVENT_PROFILE,
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
    SNAPSHOT_SCHEMA_VERSION,
    SnapshotContractError,
    capture_cooldown_assignment_snapshot,
    snapshot_schema,
)

BASE_NS = 1_800_000_000_000_000_000


def _m0(*, side: str, decision_ns: int) -> dict:
    after = 0.001 if side == "BUY" else -0.001
    return {
        "assignment_ts_ns": decision_ns,
        "fill_visible_ts_ns": decision_ns,
        "side": side,
        "role_at_fill": "opener",
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": after,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.001,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "fill_is_partial": False,
        "order_age_s": 1.25,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "known_zero",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": 0.0,
        "target_price_displayed_qty_status": "known_zero",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": 0.0,
        "last_opposite_side_fill_age_s": 0.0,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _feature_row(
    *, block: str = "M1", side: str = "BUY", last_window_gap: bool = False
) -> tuple[dict, dict, int]:
    state = CausalMultichannelEmaState(
        block=block,
        warmup_admitted=True,
        warmup_identity="d-minus-1:0123456789abcdef",
    )
    for index in range(1, 4):
        right = BASE_NS + index * BASE_WINDOW_WIDTH_NS
        values = {
            channel.name: 100.0 + index + channel_index / 100.0
            for channel_index, channel in enumerate(CHANNELS_BY_BLOCK[block])
        }
        state.update(
            CausalWindowObservation(
                left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right,
                market_generation=index,
                depth_generation=index,
                values=values,
                source_gap=bool(last_window_gap and index == 3),
            )
        )
    decision_ns = BASE_NS + 3 * BASE_WINDOW_WIDTH_NS
    m0 = _m0(side=side, decision_ns=decision_ns)
    return (
        state.feature_row(
            side=side,
            decision_ts_ns=decision_ns,
            m0_context=m0,
        ),
        m0,
        decision_ns,
    )


def _status(*, valid: bool = True, unknown: bool = False, reason: str = "valid") -> dict:
    return {"valid": valid, "unknown": unknown, "reason": reason}


def _source(generation: int, cursor: str) -> dict:
    return {
        "generation": generation,
        "cursor": cursor,
        "feature_generation": generation,
        "feature_cursor": cursor,
        **_status(),
    }


def _payload(
    *, block: str = "M1", side: str = "BUY", last_window_gap: bool = False
) -> dict:
    feature_row, m0, decision_ns = _feature_row(
        block=block, side=side, last_window_gap=last_window_gap
    )
    return {
        "snapshot_id": "snapshot-1",
        "assignment_id": "assignment-1",
        "fill_event_id": "fill-event-1",
        "client_order_id": "client-order-1",
        "lineage_id": "cooldown-lineage-1",
        "lineage_revision": 1,
        "partial_fill_ordinal": 1,
        "partial_fill_qty_btc": 0.001,
        "visibility_profile": PROSPECTIVE_RECEIVE_TIME_PROFILE,
        "clocks": {
            "assignment": {"ts_ns": decision_ns, **_status()},
            "fill_exchange": {"ts_ns": decision_ns - 2_000_000, **_status()},
            "fill_receive": {"ts_ns": decision_ns - 1_000_000, **_status()},
            "fill_visible": {"ts_ns": decision_ns, **_status()},
            "feature_ready": {
                "ts_ns": feature_row["feature_ready_ts_ns"],
                **_status(),
            },
        },
        "sources": {
            "market": _source(3, "market-cursor-3"),
            "depth": _source(3, "depth-cursor-3"),
            "trade": _source(9, "trade-cursor-9"),
        },
        "identity_hashes": {
            "config_sha256": "a" * 64,
            "code_sha256": "b" * 64,
            "model_sha256": "c" * 64,
            "p3_sha256": "d" * 64,
            "feature_dag_sha256": "e" * 64,
            "execution_abi_sha256": "f" * 64,
            "baseline_identity_sha256": "1" * 64,
        },
        "m0_context": m0,
        "feature_row": feature_row,
    }


def test_valid_m1_snapshot_is_deeply_immutable_and_policy_ready() -> None:
    snapshot = capture_cooldown_assignment_snapshot(_payload())

    assert snapshot.schema_version == SNAPSHOT_SCHEMA_VERSION
    assert snapshot.policy_input_valid is True
    assert snapshot.policy_input is not None
    assert snapshot.fallback_policy_id is None
    assert snapshot.economic_outcomes_read is False
    assert snapshot.field_validity["m0.fill_qty_btc"].valid is True
    assert len(snapshot.source_bundle_sha256) == 64

    cross_age_statuses = [
        value
        for key, value in snapshot.field_validity.items()
        if key.endswith("::cross_age_s")
    ]
    assert cross_age_statuses
    assert any(status.valid and status.unknown for status in cross_age_statuses)

    with pytest.raises(FrozenInstanceError):
        snapshot.snapshot_id = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        snapshot.feature_row["support_valid"] = False  # type: ignore[index]


def test_gap_snapshot_is_retained_but_policy_falls_back_to_control() -> None:
    snapshot = capture_cooldown_assignment_snapshot(
        _payload(block="M2", last_window_gap=True)
    )

    assert snapshot.policy_input_valid is False
    assert snapshot.policy_input is None
    assert snapshot.fallback_policy_id == CONTROL_POLICY_ID
    assert "channel_support_invalid" in str(snapshot.fallback_reason)
    field = snapshot.field_validity["feature.value::mid_usdc_per_btc::ema::h0p5s"]
    assert field.valid is False
    assert field.unknown is True
    assert field.reason == "channel_unobserved"


def test_historical_missing_receive_is_valid_exploration_but_not_transport() -> None:
    payload = _payload()
    payload["visibility_profile"] = HISTORICAL_EXCHANGE_EVENT_PROFILE
    payload["clocks"]["fill_receive"] = {
        "ts_ns": None,
        **_status(
            valid=False,
            unknown=True,
            reason="private_fill_receive_clock_unavailable",
        ),
    }
    snapshot = capture_cooldown_assignment_snapshot(payload)

    assert snapshot.policy_input_valid is True
    assert snapshot.receive_time_transport_eligible is False
    assert snapshot.fallback_policy_id is None
    status = snapshot.field_validity["clock.fill_receive"]
    assert status.valid is False
    assert status.unknown is True


def test_prospective_profile_requires_valid_receive_clock() -> None:
    payload = _payload()
    payload["clocks"]["fill_receive"] = {
        "ts_ns": None,
        **_status(valid=False, unknown=True, reason="missing_receive"),
    }
    with pytest.raises(SnapshotContractError, match="requires a valid fill receive"):
        capture_cooldown_assignment_snapshot(payload)


def test_nullable_m0_fill_history_is_not_encoded_as_zero() -> None:
    payload = _payload()
    for field in ("last_same_side_fill_age_s", "last_opposite_side_fill_age_s"):
        payload["m0_context"][field] = None
        payload["feature_row"][field] = None
    snapshot = capture_cooldown_assignment_snapshot(payload)
    assert snapshot.policy_input_valid is True
    assert snapshot.field_validity["m0.last_same_side_fill_age_s"].unknown is True
    assert snapshot.m0_context["last_same_side_fill_age_s"] is None


def test_future_ready_feature_and_mixed_source_generation_fail_closed() -> None:
    future = _payload()
    future_ns = future["clocks"]["fill_visible"]["ts_ns"] + 1
    future["clocks"]["feature_ready"]["ts_ns"] = future_ns
    future["feature_row"]["feature_ready_ts_ns"] = future_ns
    with pytest.raises(SnapshotContractError, match="feature_ready <= fill_visible"):
        capture_cooldown_assignment_snapshot(future)

    mixed = _payload()
    mixed["sources"]["depth"]["feature_generation"] = 2
    with pytest.raises(SnapshotContractError, match="silently mixed"):
        capture_cooldown_assignment_snapshot(mixed)

    non_atomic = _payload()
    non_atomic["sources"]["depth"]["generation"] = 2
    non_atomic["sources"]["depth"]["feature_generation"] = 2
    non_atomic["feature_row"]["depth_generation"] = 2
    with pytest.raises(SnapshotContractError, match="one atomic generation"):
        capture_cooldown_assignment_snapshot(non_atomic)


def test_exact_schema_rejects_outcomes_unknown_columns_and_m0_extras() -> None:
    economic = _payload()
    economic["terminal_pnl_usdc"] = 1.0
    with pytest.raises(SnapshotContractError, match="economic outcomes"):
        capture_cooldown_assignment_snapshot(economic)

    feature_extra = _payload()
    feature_extra["feature_row"]["hidden_label"] = 1
    with pytest.raises(SnapshotContractError, match="economic outcomes"):
        capture_cooldown_assignment_snapshot(feature_extra)

    m0_extra = _payload()
    m0_extra["m0_context"]["future_reward"] = 1.0
    with pytest.raises(SnapshotContractError, match="economic outcomes"):
        capture_cooldown_assignment_snapshot(m0_extra)

    unknown = _payload()
    unknown["feature_row"]["innocent_but_unknown"] = 1
    with pytest.raises(SnapshotContractError, match="unknown feature_row"):
        capture_cooldown_assignment_snapshot(unknown)


@pytest.mark.parametrize(
    ("field", "value"),
    (("snapshot_id", ""), ("assignment_id", "nan"), ("lineage_id", " NaN ")),
)
def test_empty_or_nan_like_ids_are_rejected(field: str, value: str) -> None:
    payload = _payload()
    payload[field] = value
    with pytest.raises(SnapshotContractError, match="empty or NaN-like"):
        capture_cooldown_assignment_snapshot(payload)


def test_hash_partial_fill_and_duplicate_feature_context_are_bound() -> None:
    bad_hash = _payload()
    bad_hash["identity_hashes"]["execution_abi_sha256"] = "not-a-hash"
    with pytest.raises(SnapshotContractError, match="exact SHA256"):
        capture_cooldown_assignment_snapshot(bad_hash)

    wrong_quantity = _payload()
    wrong_quantity["partial_fill_qty_btc"] = 0.002
    with pytest.raises(SnapshotContractError, match="quantity disagrees"):
        capture_cooldown_assignment_snapshot(wrong_quantity)

    drifted_context = _payload()
    drifted_context["feature_row"]["campaign_add_count"] = 1
    with pytest.raises(SnapshotContractError, match="feature/M0 field drifted"):
        capture_cooldown_assignment_snapshot(drifted_context)


def test_schema_is_bounded_and_declares_outcomes_forbidden() -> None:
    schema = snapshot_schema("M2")
    assert schema["unknown_columns_allowed"] is False
    assert schema["economic_outcomes_allowed"] is False
    assert schema["unsupported_policy_fallback"] == CONTROL_POLICY_ID
    assert schema["feature_block"] == "M2"
    assert HISTORICAL_EXCHANGE_EVENT_PROFILE in schema["visibility_profiles"]
    assert snapshot_schema("R0")["feature_block"] == "R0"
    assert len(schema["feature_columns"]) > len(snapshot_schema("M1")["feature_columns"])

    payload = _payload()
    original = copy.deepcopy(payload)
    capture_cooldown_assignment_snapshot(payload)
    assert payload == original
