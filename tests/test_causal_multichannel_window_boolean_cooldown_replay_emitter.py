from __future__ import annotations

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)

BASE_NS = 1_800_000_000_000_000_000


def _observations(*, raw_warmup_admitted: bool = True) -> iter:
    rows = []
    for index in range(1, 4):
        right = BASE_NS + index * BASE_WINDOW_WIDTH_NS
        rows.append(
            CausalWindowObservation(
                left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right,
                market_generation=index,
                depth_generation=index,
                values={
                    channel.name: 100.0 + index
                    for channel in CHANNELS_BY_BLOCK["M1"]
                },
                warmup_admitted=bool(raw_warmup_admitted and index > 1),
            )
        )
    return iter(rows)


def _identity_hashes() -> dict[str, str]:
    return {
        "config_sha256": "a" * 64,
        "code_sha256": "b" * 64,
        "model_sha256": "c" * 64,
        "p3_sha256": "d" * 64,
        "feature_dag_sha256": "e" * 64,
        "execution_abi_sha256": "f" * 64,
        "baseline_identity_sha256": "1" * 64,
    }


def _m0(decision_ns: int) -> dict:
    return {
        "assignment_ts_ns": decision_ns,
        "fill_visible_ts_ns": decision_ns,
        "side": "BUY",
        "role_at_fill": "opener",
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": 0.001,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.002,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.001,
        "partial_fill_ordinal": 1,
        "fill_is_partial": True,
        "order_age_s": 1.0,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "exact",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": 0.025,
        "target_price_displayed_qty_status": "exact",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _emitter(*, raw_warmup_admitted: bool = True) -> CooldownV2ReplayEmitter:
    return CooldownV2ReplayEmitter(
        feature_block="M1",
        observations=_observations(
            raw_warmup_admitted=raw_warmup_admitted
        ),
        warmup_cutoff_ts_ns=BASE_NS + BASE_WINDOW_WIDTH_NS,
        warmup_identity="d-minus-1-bound",
        identity_hashes=_identity_hashes(),
        source_cursor_prefixes={
            "market": "native-market",
            "depth": "native-depth",
            "trade": "individual-trade",
        },
        retain_snapshots=True,
    )


def test_replay_emitter_freezes_one_fill_visible_snapshot() -> None:
    emitter = _emitter()
    decision_ns = BASE_NS + 3 * BASE_WINDOW_WIDTH_NS + 50_000_000
    snapshot = emitter.capture_exposure_fill(
        assignment_id="assignment-1",
        fill_event_id="fill-1",
        client_order_id="order-1",
        lineage_id="buy-lineage",
        lineage_revision=1,
        partial_fill_ordinal=1,
        partial_fill_qty_btc=0.001,
        fill_exchange_ts_ns=decision_ns,
        fill_visible_ts_ns=decision_ns,
        m0_context=_m0(decision_ns),
    )

    assert snapshot.policy_input_valid is True
    assert snapshot.receive_time_transport_eligible is False
    assert snapshot.feature_row["last_window_right_ts_ns"] < decision_ns
    assert len(emitter.snapshots) == 1
    audit = emitter.audit()
    assert audit.windows_consumed == 3
    assert audit.snapshots_emitted == 1
    assert audit.warmup_admitted is True
    assert audit.economic_outcomes_read is False


def test_replay_emitter_falls_back_when_completed_window_stream_is_stale() -> None:
    emitter = _emitter()
    decision_ns = BASE_NS + 4 * BASE_WINDOW_WIDTH_NS
    snapshot = emitter.capture_exposure_fill(
        assignment_id="assignment-1",
        fill_event_id="fill-1",
        client_order_id="order-1",
        lineage_id="buy-lineage",
        lineage_revision=1,
        partial_fill_ordinal=1,
        partial_fill_qty_btc=0.001,
        fill_exchange_ts_ns=decision_ns,
        fill_visible_ts_ns=decision_ns,
        m0_context=_m0(decision_ns),
    )

    assert snapshot.policy_input_valid is False
    assert snapshot.fallback_policy_id == "CONTROL_85N"
    assert "completed_window_stream_stale" in str(snapshot.fallback_reason)
    assert emitter.audit().fallback_snapshots == 1


def test_replay_emitter_does_not_infer_rejected_raw_warmup_at_target() -> None:
    emitter = _emitter(raw_warmup_admitted=False)
    decision_ns = BASE_NS + 2 * BASE_WINDOW_WIDTH_NS + 50_000_000
    snapshot = emitter.capture_exposure_fill(
        assignment_id="assignment-raw-warmup-rejected",
        fill_event_id="fill-raw-warmup-rejected",
        client_order_id="order-raw-warmup-rejected",
        lineage_id="buy-lineage",
        lineage_revision=1,
        partial_fill_ordinal=1,
        partial_fill_qty_btc=0.001,
        fill_exchange_ts_ns=decision_ns,
        fill_visible_ts_ns=decision_ns,
        m0_context=_m0(decision_ns),
    )

    assert snapshot.policy_input_valid is False
    assert snapshot.fallback_policy_id == "CONTROL_85N"
    assert snapshot.feature_row["warmup_admitted"] is False
    assert snapshot.feature_row["warmup_identity"] is None
    assert emitter.audit().warmup_admitted is False
