from __future__ import annotations

import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal_v2 import (
    ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS,
    OrderLifecycleJournalV2BatchEmitter,
    OrderLifecycleJournalV2Cursor,
    OrderLifecycleJournalV2SourceCallback,
    validate_order_lifecycle_journal_v2_payload,
)


def _emitter(**kwargs: object) -> OrderLifecycleJournalV2BatchEmitter:
    defaults: dict[str, object] = {
        "lifecycle_id": "epoch-1:client-17",
        "runtime_source": "replay",
        "client_order_id": "client-17",
        "exchange_order_id": "exchange-42",
        "symbol": "BTCUSDC",
        "side": "BUY",
    }
    defaults.update(kwargs)
    return OrderLifecycleJournalV2BatchEmitter(**defaults)


def _callback(
    callback_id: str,
    received_ts_ns: int,
    exchange_ts_ns: int | None = None,
) -> OrderLifecycleJournalV2SourceCallback:
    return OrderLifecycleJournalV2SourceCallback(
        callback_id=callback_id,
        callback_type="execution_report",
        received_ts_ns=received_ts_ns,
        exchange_ts_ns=exchange_ts_ns,
    )


def test_emitter_returns_every_unseen_event_with_callback_ordinals() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()

    submit = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    assert [row.lifecycle_event for row in submit.rows] == ["submit"]
    assert submit.rows[0].source_callback_event_ordinal == 1
    assert submit.rows[0].source_callback_event_count == 1

    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=2_300_000_000,
        exchange_ts_ns=2_000_000_000,
    )
    batch = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback(
            "callback-activate-fill",
            2_000_000_000,
            2_000_000_000,
        ),
    )

    assert [row.lifecycle_event for row in batch.rows] == [
        "activate",
        "partial_fill",
    ]
    assert [row.source_callback_event_ordinal for row in batch.rows] == [1, 2]
    assert {row.source_callback_id for row in batch.rows} == {"callback-activate-fill"}
    assert {row.source_callback_event_count for row in batch.rows} == {2}
    assert batch.rows[-1].remaining_quantity_after == pytest.approx(0.0004)
    assert batch.checkpoint["last_emitted_sequence"] == 3


def test_checkpoint_resume_emits_only_new_events_and_event_ids_are_stable() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    first = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    event_id = first.rows[0].event_id

    resumed = OrderLifecycleJournalV2BatchEmitter.from_checkpoint(
        checkpoint=first.checkpoint,
        runtime_source="replay",
        symbol="BTCUSDC",
        side="BUY",
        exchange_order_id="exchange-42",
    )
    assert (
        resumed.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-noop", 1_100_000_000),
        ).rows
        == ()
    )

    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    second = resumed.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
    )
    assert [row.lifecycle_sequence for row in second.rows] == [2]
    assert first.rows[0].event_id == event_id
    assert second.rows[0].event_id != event_id


def test_exact_schema_has_no_economic_outcome_fields() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    row = (
        _emitter()
        .emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-submit", 1_000_000_000),
        )
        .payloads()[0]
    )
    assert tuple(row) == ORDER_LIFECYCLE_JOURNAL_V2_COLUMNS
    assert not any(
        fragment in column.lower() for column in row for fragment in ("pnl", "reward", "markout")
    )
    malformed = dict(row)
    malformed["reward_usdc"] = 1.0
    with pytest.raises(ValueError, match="schema mismatch"):
        validate_order_lifecycle_journal_v2_payload(malformed)
    bad_reason = dict(row)
    bad_reason["event_reason"] = "economic_override"
    with pytest.raises(ValueError, match="unsupported reason"):
        validate_order_lifecycle_journal_v2_payload(bad_reason)
    bad_event_id = dict(row)
    bad_event_id["event_id"] = "0" * 64
    with pytest.raises(ValueError, match="event id is inconsistent"):
        validate_order_lifecycle_journal_v2_payload(bad_event_id)
    legacy_shutdown = dict(row)
    legacy_shutdown["lifecycle_event"] = "exchange_terminal"
    legacy_shutdown["event_reason"] = "local_shutdown_cancel"
    with pytest.raises(ValueError, match="not authoritative"):
        validate_order_lifecycle_journal_v2_payload(legacy_shutdown)


def test_partial_fill_and_exchange_terminal_carry_event_level_exposure() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
    )
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=4_500_000_000,
        exchange_ts_ns=4_000_000_000,
    )
    partial = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-fill", 4_000_000_000, 4_000_000_000),
    ).rows[0]
    assert partial.remaining_quantity_before == pytest.approx(0.001)
    assert partial.remaining_quantity_after == pytest.approx(0.0004)
    assert partial.quantity_time_exposure_visible_btc_s == pytest.approx(0.0023)
    assert partial.quantity_time_exposure_exchange_btc_s == pytest.approx(0.002)
    assert partial.visible_exposure_complete is False
    assert partial.exchange_exposure_complete is False

    lifecycle.request_cancel(5_000_000_000)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-cancel-request", 5_000_000_000),
    )
    lifecycle.exchange_terminal(
        9_000_000_000,
        reason="cancel_ack",
        exchange_ts_ns=8_000_000_000,
    )
    terminal = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-cancel-ack", 8_000_000_000, 8_000_000_000),
    ).rows[0]
    assert terminal.terminal_observation == "EXCHANGE_TERMINAL"
    assert terminal.exchange_terminal_reason == "cancel_ack"
    assert terminal.local_censor_reason == ""
    assert terminal.visible_exposure_complete is True
    assert terminal.exchange_exposure_complete is True
    assert terminal.remaining_quantity_after == pytest.approx(0.0004)


def test_legacy_local_shutdown_exchange_terminal_fails_closed() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
    )
    lifecycle.exchange_terminal(
        3_200_000_000,
        reason="local_shutdown_cancel",
        exchange_ts_ns=3_000_000_000,
    )
    checkpoint = emitter.cursor.checkpoint()
    with pytest.raises(ValueError, match="explicit local_shutdown_censor"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-shutdown", 3_000_000_000, 3_000_000_000),
        )
    assert emitter.cursor.checkpoint() == checkpoint


def test_legacy_local_shutdown_without_exchange_clock_also_fails_closed() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
    )
    lifecycle.exchange_terminal(
        3_200_000_000,
        reason="local_shutdown_cancel",
    )
    checkpoint = emitter.cursor.checkpoint()
    with pytest.raises(ValueError, match="explicit local_shutdown_censor"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-shutdown", 3_000_000_000),
        )
    assert emitter.cursor.checkpoint() == checkpoint


def test_explicit_local_shutdown_censor_preserves_unknown_exchange_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
    )

    events = [dict(event) for event in lifecycle.events()]
    events.append(
        {
            "sequence": 3,
            "event": "local_shutdown_censor",
            "visibility_ts_ns": 3_200_000_000,
            "exchange_ts_ns": 0,
            "phase_before": "ACTIVE",
            "phase_after": "ACTIVE",
            "remaining_qty_before": 0.001,
            "remaining_qty_after": 0.001,
            "quantity_time_exposure_btc_s": 0.001,
            "quantity_time_exposure_visible_btc_s": 0.001,
            "quantity_time_exposure_exchange_btc_s": 0.0,
            "exchange_exposure_valid": True,
            "reason": "local_shutdown_cancel",
        }
    )
    snapshot = dict(lifecycle.snapshot())
    snapshot["quantity_time_exposure_btc_s"] = 0.001
    snapshot["quantity_time_exposure_visible_btc_s"] = 0.001
    snapshot["quantity_time_exposure_visibility_minus_exchange_btc_s"] = 0.001
    monkeypatch.setattr(lifecycle, "events", lambda: tuple(events))
    monkeypatch.setattr(lifecycle, "snapshot", lambda: dict(snapshot))

    row = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-local-censor", 3_200_000_000),
    ).rows[0]
    assert row.lifecycle_event == "local_shutdown_censor"
    assert row.terminal_observation == "LOCAL_SHUTDOWN_CENSOR"
    assert row.phase_before == row.phase_after == "ACTIVE"
    assert row.fill_risk_active_after is None
    assert row.visible_exposure_complete is False
    assert row.exchange_exposure_complete is False


def test_orphan_adoption_is_explicit_left_truncation() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter(
        orphan_adoption=True,
        left_truncation_reason="exchange_open_order_adopted",
    )
    row = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-adopt", 1_000_000_000),
    ).rows[0]
    assert row.observation_origin == "ORPHAN_ADOPTION"
    assert row.left_truncated is True
    assert row.left_truncation_reason == "exchange_open_order_adopted"


@pytest.mark.parametrize(
    "mutator,match",
    [
        (
            lambda events: events[1].__setitem__("sequence", 1),
            "unique and contiguous",
        ),
        (
            lambda events: events[1].__setitem__("sequence", 3),
            "unique and contiguous",
        ),
        (
            lambda events: events[1].__setitem__("event", "mystery"),
            "unsupported lifecycle event",
        ),
        (
            lambda events: events[1].__setitem__("reason", "mystery"),
            "unsupported reason",
        ),
    ],
)
def test_bad_event_stream_fails_closed_without_advancing_cursor(
    monkeypatch: pytest.MonkeyPatch,
    mutator,
    match: str,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    original = [dict(event) for event in lifecycle.events()]
    malformed = [dict(event) for event in original]
    mutator(malformed)
    monkeypatch.setattr(lifecycle, "events", lambda: tuple(malformed))
    emitter = _emitter()
    before = emitter.cursor.checkpoint()
    with pytest.raises(ValueError, match=match):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-bad", 1_000_000_000),
        )
    assert emitter.cursor.checkpoint() == before


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"lifecycle_id": ""}, "lifecycle id is required"),
        ({"client_order_id": "nan"}, "client order id is required"),
        ({"runtime_source": ""}, "runtime source is required"),
    ],
)
def test_missing_identity_fails_closed(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        _emitter(**kwargs)


def test_callback_clock_disagreement_fails_without_advancing() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    checkpoint = emitter.cursor.checkpoint()
    with pytest.raises(ValueError, match="exchange clocks disagree"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback(
                "callback-activate",
                2_100_000_000,
                2_100_000_000,
            ),
        )
    assert emitter.cursor.checkpoint() == checkpoint


def test_non_submit_event_requires_exchange_order_id() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter(exchange_order_id=None)
    emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    lifecycle.activate(2_200_000_000, exchange_ts_ns=2_000_000_000)
    checkpoint = emitter.cursor.checkpoint()
    with pytest.raises(ValueError, match="lacks exchange order id"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-activate", 2_000_000_000, 2_000_000_000),
        )
    assert emitter.cursor.checkpoint() == checkpoint


def test_inconsistent_final_snapshot_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    original_snapshot = lifecycle.snapshot

    def bad_snapshot():
        snapshot = dict(original_snapshot())
        snapshot["remaining_quantity"] = 0.0005
        return snapshot

    monkeypatch.setattr(lifecycle, "snapshot", bad_snapshot)
    emitter = _emitter()
    with pytest.raises(ValueError, match="remaining quantity is inconsistent"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-submit", 1_000_000_000),
        )
    assert emitter.cursor.last_emitted_sequence == 0


def test_checkpoint_schema_and_prior_event_mutation_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    emitter = _emitter()
    batch = emitter.emit_unseen(
        lifecycle=lifecycle,
        callback=_callback("callback-submit", 1_000_000_000),
    )
    malformed_checkpoint = dict(batch.checkpoint)
    malformed_checkpoint["extra"] = True
    with pytest.raises(ValueError, match="checkpoint schema mismatch"):
        OrderLifecycleJournalV2Cursor.from_checkpoint(malformed_checkpoint)

    events = [dict(event) for event in lifecycle.events()]
    events[0]["visibility_ts_ns"] = 1_000_000_001
    monkeypatch.setattr(lifecycle, "events", lambda: tuple(events))
    with pytest.raises(ValueError, match="submitted timestamp is inconsistent"):
        emitter.emit_unseen(
            lifecycle=lifecycle,
            callback=_callback("callback-noop", 1_100_000_000),
        )
