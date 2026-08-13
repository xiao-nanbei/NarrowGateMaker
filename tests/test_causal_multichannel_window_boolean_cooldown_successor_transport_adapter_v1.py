from __future__ import annotations

from dataclasses import replace

import pytest

from models.tick_data_types import HistoricalExchangeBookEvent
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_successor_transport_adapter_v1 as transport,
)

BASE_NS = 1_800_000_000_000_000_000


def _book(
    offset_ms: int,
    *,
    ordinal: int,
    last_update_id: int | None = None,
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    previous_final_update_id: int | None = None,
    bid_qty: float = 5.0,
    ready_delay_ms: int = 15,
) -> transport.RecordedBookVisibility:
    exchange = BASE_NS + offset_ms * 1_000_000
    receive = exchange + 10_000_000
    event_type = "snapshot" if last_update_id is not None else "delta"
    event = HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type=event_type,
        exchange_ts_ns=exchange,
        exchange_ts_source="transaction",
        local_receive_ts_ns=receive,
        event_time_ns=exchange,
        transaction_time_ns=exchange,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        last_update_id=last_update_id,
        levels=(("bid", 999, bid_qty), ("ask", 1001, 5.0)),
        source="fixture",
        source_ordinal=ordinal,
    )
    return transport.RecordedBookVisibility(
        event=event,
        feature_ready_ts_ns=exchange + ready_delay_ms * 1_000_000,
        total_order_ordinal=ordinal * 10,
    )


def _trade(
    offset_ms: int = 150,
    *,
    ordinal: int = 21,
    ready_delay_ms: int = 20,
) -> transport.RecordedTradeVisibility:
    exchange = BASE_NS + offset_ms * 1_000_000
    return transport.RecordedTradeVisibility(
        event_id=f"trade-{offset_ms}-{ordinal}",
        exchange_ts_ns=exchange,
        receive_ts_ns=exchange + 12_000_000,
        feature_ready_ts_ns=exchange + ready_delay_ms * 1_000_000,
        total_order_ordinal=ordinal,
        side="SELL",
        price=100.0,
        quantity=0.01,
        source_sequence=ordinal,
    )


def _fill(
    *,
    fill_id: str = "fill-1",
    offset_ms: int = 200,
    ordinal: int = 31,
    partial_ordinal: int = 1,
    lifecycle_sequence: int = 1,
    recorded: bool = True,
) -> transport.ArmFillTruthEvent:
    exchange = BASE_NS + offset_ms * 1_000_000
    return transport.ArmFillTruthEvent(
        fill_event_id=fill_id,
        lifecycle_id="lifecycle-1",
        side="SELL",
        role="add",
        exchange_ts_ns=exchange,
        price=100.0,
        quantity=0.01,
        partial_fill_ordinal=partial_ordinal,
        lifecycle_sequence=lifecycle_sequence,
        total_order_ordinal=ordinal,
        recorded_receive_ts_ns=exchange + 25_000_000 if recorded else None,
        recorded_feature_ready_ts_ns=(
            exchange + 30_000_000 if recorded else None
        ),
    )


def _bundle(
    *,
    books: tuple[transport.RecordedBookVisibility, ...] | None = None,
    trades: tuple[transport.RecordedTradeVisibility, ...] | None = None,
) -> transport.ProspectiveReplayTransportBundle:
    if books is None:
        books = (
            _book(100, ordinal=1, last_update_id=100),
            _book(
                300,
                ordinal=2,
                first_update_id=101,
                final_update_id=101,
                previous_final_update_id=100,
                bid_qty=2.0,
                ready_delay_ms=50,
            ),
        )
    return transport.ProspectiveReplayTransportBundle.build(
        market_source_manifest_sha256="a" * 64,
        book_events=books,
        trade_events=trades or (_trade(),),
    )


def _delay_artifact(
    *,
    minimum_support: int = 2,
) -> transport.PastOnlyPrivateFillDelayArtifact:
    return transport.PastOnlyPrivateFillDelayArtifact(
        identity="past_only_private_fill_delay_fixture_v1",
        fitted_through_ts_ns=BASE_NS - 1,
        minimum_support=minimum_support,
        cohorts=(
            transport.FillDelayCohort(
                side="SELL",
                role="add",
                receive_delay_ns=(20_000_000, 30_000_000),
                feature_after_receive_ns=(5_000_000, 7_000_000),
            ),
        ),
    )


def _drain(
    arm: transport.ArmReplayTransport,
    bundle: transport.ProspectiveReplayTransportBundle,
    fills: tuple[transport.ArmFillTruthEvent, ...],
    artifact: transport.PastOnlyPrivateFillDelayArtifact | None,
) -> None:
    arm.advance_strategy_to(
        transport.latest_transport_timestamp(
            bundle=bundle,
            fills=fills,
            delay_artifact=artifact,
        )
    )


def test_exchange_fill_truth_does_not_update_strategy_before_private_visibility() -> None:
    bundle = _bundle()
    fill = _fill()
    arm = bundle.spawn_arm(arm="control", fill_events=(fill,))

    arm.advance_exchange_to(fill.exchange_ts_ns)

    assert arm.visible_state().inventory_btc == 0.0
    assert arm.visible_state().private_fill_callback_count == 0


def test_private_callback_updates_inventory_only_at_feature_ready_clock() -> None:
    bundle = _bundle()
    fill = _fill()
    arm = bundle.spawn_arm(arm="control", fill_events=(fill,))
    seen: list[str] = []
    arm.set_private_fill_callback(lambda row: seen.append(row.fill_event_id))

    arm.advance_strategy_to(int(fill.recorded_feature_ready_ts_ns) - 1)
    assert arm.visible_state().inventory_btc == 0.0
    arm.advance_strategy_to(int(fill.recorded_feature_ready_ts_ns))

    assert arm.visible_state().inventory_btc == pytest.approx(-0.01)
    assert seen == [fill.fill_event_id]


def test_exchange_truth_and_visible_book_diverge_then_converge_without_leakage() -> None:
    bundle = _bundle()
    arm = bundle.spawn_arm(arm="control", fill_events=())

    arm.advance_strategy_to(BASE_NS + 300_000_000)
    truth_bids, _ = arm.truth_top_levels()
    visible_bids, _ = arm.visible_top_levels()

    assert truth_bids[0][1] == pytest.approx(2.0)
    assert visible_bids[0][1] == pytest.approx(5.0)

    arm.advance_strategy_to(BASE_NS + 350_000_000)
    converged_bids, _ = arm.visible_top_levels()
    assert converged_bids[0][1] == pytest.approx(2.0)


def test_public_trade_becomes_feature_visible_on_ready_not_exchange_clock() -> None:
    bundle = _bundle()
    arm = bundle.spawn_arm(arm="control", fill_events=())
    trade = bundle.trade_events[0]

    arm.advance_strategy_to(trade.feature_ready_ts_ns - 1)
    assert arm.visible_state().public_trade_count == 0
    arm.advance_strategy_to(trade.feature_ready_ts_ns)
    assert arm.visible_state().public_trade_count == 1


def test_ambiguous_same_timestamp_exchange_order_fails_closed() -> None:
    book = _book(100, ordinal=1, last_update_id=100)
    trade = _trade(offset_ms=100, ordinal=book.total_order_ordinal)

    with pytest.raises(
        transport.TransportContractError,
        match="ambiguous_same_timestamp_order",
    ):
        _bundle(books=(book,), trades=(trade,))


def test_cross_stream_feature_ready_tie_fails_instead_of_inventing_order() -> None:
    book = _book(100, ordinal=1, last_update_id=100, ready_delay_ms=20)
    trade = _trade(offset_ms=105, ordinal=21, ready_delay_ms=15)
    bundle = _bundle(books=(book,), trades=(trade,))

    with pytest.raises(
        transport.TransportContractError,
        match="cannot be represented without inventing order",
    ):
        bundle.spawn_arm(arm="control", fill_events=())


def test_recorded_private_fill_receipt_preserves_exact_authority() -> None:
    bundle = _bundle()
    fill = _fill()
    arm = bundle.spawn_arm(arm="control", fill_events=(fill,))
    _drain(arm, bundle, (fill,), None)

    receipt = arm.transport_receipt()

    assert receipt.private_fill_visibility_authority == "recorded_exact"
    assert receipt.formal_replay_support_valid is True
    assert receipt.private_fill_visible_count == 1


def test_counterfactual_private_fill_uses_hash_bound_past_only_delay() -> None:
    bundle = _bundle()
    fill = _fill(recorded=False)
    artifact = _delay_artifact()
    first = artifact.resolve(fill)
    second = artifact.resolve(fill)

    assert first == second
    assert first is not None
    assert first.receive_ts_ns > fill.exchange_ts_ns
    arm = bundle.spawn_arm(
        arm="candidate",
        fill_events=(fill,),
        counterfactual_delay_artifact=artifact,
    )
    _drain(arm, bundle, (fill,), artifact)
    receipt = arm.transport_receipt()
    assert receipt.private_fill_visibility_authority == "modeled_sensitivity"
    assert receipt.delay_artifact_sha256 == artifact.artifact_sha256


def test_unsupported_counterfactual_private_fill_is_censored_not_zero_delay() -> None:
    bundle = _bundle()
    fill = _fill(recorded=False)
    artifact = _delay_artifact(minimum_support=3)
    arm = bundle.spawn_arm(
        arm="candidate",
        fill_events=(fill,),
        counterfactual_delay_artifact=artifact,
    )
    arm.advance_strategy_to(BASE_NS + 1_000_000_000)

    receipt = arm.transport_receipt()

    assert arm.visible_state().private_fill_callback_count == 0
    assert receipt.counterfactual_fill_censored_count == 1
    assert receipt.formal_replay_support_valid is False
    assert "counterfactual_private_fill_delay_unsupported" in receipt.exclusion_reasons


def test_counterfactual_zero_receive_delay_is_rejected() -> None:
    with pytest.raises(transport.TransportContractError):
        transport.FillDelayCohort(
            side="SELL",
            role="add",
            receive_delay_ns=(0,),
            feature_after_receive_ns=(1,),
        )


def test_partial_fills_preserve_lifecycle_sequence() -> None:
    bundle = _bundle()
    first = _fill(fill_id="fill-1", offset_ms=200, ordinal=31)
    second = _fill(
        fill_id="fill-2",
        offset_ms=220,
        ordinal=32,
        partial_ordinal=2,
        lifecycle_sequence=2,
    )
    arm = bundle.spawn_arm(arm="control", fill_events=(first, second))
    _drain(arm, bundle, (first, second), None)
    assert arm.visible_state().private_fill_callback_count == 2

    with pytest.raises(transport.TransportContractError):
        bundle.spawn_arm(
            arm="control",
            fill_events=(first, replace(second, partial_fill_ordinal=1)),
        )


def test_paired_arms_share_market_bytes_but_not_mutable_strategy_state() -> None:
    bundle = _bundle()
    control_fill = _fill(fill_id="control-fill")
    candidate_fill = _fill(fill_id="candidate-fill", recorded=False)
    control = bundle.spawn_arm(arm="control", fill_events=(control_fill,))
    candidate = bundle.spawn_arm(
        arm="candidate",
        fill_events=(candidate_fill,),
        counterfactual_delay_artifact=_delay_artifact(),
    )

    _drain(candidate, bundle, (candidate_fill,), _delay_artifact())

    assert candidate.visible_state().inventory_btc == pytest.approx(-0.01)
    assert control.visible_state().inventory_btc == 0.0
    assert (
        candidate.bundle.common_market_source_sha256
        == control.bundle.common_market_source_sha256
    )


def test_transport_receipt_binds_source_and_denies_live_equivalence() -> None:
    bundle = _bundle()
    arm = bundle.spawn_arm(arm="control", fill_events=())
    _drain(arm, bundle, (), None)
    receipt = arm.transport_receipt()

    validated = transport.validate_transport_receipt(
        receipt.to_dict(),
        expected_arm="control",
        expected_common_market_source_sha256=bundle.common_market_source_sha256,
    )

    assert validated.transport_receipt_sha256 == receipt.transport_receipt_sha256
    assert validated.live_equivalent is False
    assert validated.action_authorized is False
    assert validated.live_policy_authorized is False


def test_transport_receipt_rejects_hash_or_arm_drift() -> None:
    bundle = _bundle()
    arm = bundle.spawn_arm(arm="control", fill_events=())
    _drain(arm, bundle, (), None)
    payload = arm.transport_receipt().to_dict()

    with pytest.raises(transport.TransportContractError):
        transport.validate_transport_receipt(
            payload,
            expected_arm="candidate",
            expected_common_market_source_sha256=bundle.common_market_source_sha256,
        )
    with pytest.raises(transport.TransportContractError):
        transport.validate_transport_receipt(
            payload,
            expected_arm="control",
            expected_common_market_source_sha256="b" * 64,
        )
