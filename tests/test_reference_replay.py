import numpy as np
import pytest

from research.families.f04_external_market_alpha.reference_replay import (
    CampaignRepairCursor,
    HistoricalGlobalFlowCursor,
    HistoricalReferenceScheduler,
    apply_global_flow_visibility_delay,
)
from models.tick_data_types import (
    HistoricalCampaignRepairData,
    HistoricalGlobalFlowData,
    HistoricalReferenceEvent,
)


def _book(market_id: str, ready_ns: int, bid: float, ask: float):
    return HistoricalReferenceEvent(
        market_id=market_id,
        event_type="book",
        exchange_event_ts_ns=ready_ns - 10,
        local_receive_ts_ns=ready_ns - 5,
        feature_ready_ts_ns=ready_ns,
        bid=bid,
        bid_size=1.0,
        ask=ask,
        ask_size=1.0,
    )


def test_scheduler_merges_tapes_by_feature_ready_time_without_future_visibility():
    start = 1_700_000_000_000_000_000
    scheduler = HistoricalReferenceScheduler(
        {
            "late": [
                _book("bitget:perp:BTCUSDT", start + 20, 99.0, 101.0),
                _book("bitget:perp:BTCUSDT", start + 40, 100.0, 102.0),
            ],
            "early": [
                _book("bybit:perp:BTCUSDT", start + 10, 99.0, 101.0),
                _book("bybit:perp:BTCUSDT", start + 30, 98.0, 100.0),
            ],
        }
    )

    assert scheduler.advance_to(start + 25) == 2
    first = scheduler.stats()
    assert first.consumed_events == 2
    assert first.last_ready_ts_ns == start + 20

    assert scheduler.advance_to(start + 30) == 1
    second = scheduler.stats()
    assert second.consumed_events == 3
    assert second.last_ready_ts_ns == start + 30


def test_scheduler_fails_fast_when_one_tape_regresses():
    start = 1_700_000_000_000_000_000
    scheduler = HistoricalReferenceScheduler(
        {
            "broken": [
                _book("okx:spot:BTCUSDT", start + 20, 99.0, 101.0),
                _book("okx:spot:BTCUSDT", start + 10, 99.0, 101.0),
            ]
        }
    )

    with pytest.raises(ValueError, match="not sorted"):
        scheduler.advance_to(start + 20)


def test_formal_scheduler_rejects_one_shot_tape_iterators():
    start = 1_700_000_000_000_000_000
    events = iter([_book("okx:spot:BTCUSDT", start + 10, 99.0, 101.0)])

    with pytest.raises(TypeError, match="one-shot iterator"):
        HistoricalReferenceScheduler({"one-shot": events})


def test_campaign_repair_cursor_is_asof_and_reports_lookback_change():
    start = 1_700_000_000_000_000_000
    cursor = CampaignRepairCursor(
        HistoricalCampaignRepairData(
            ts_ns=np.asarray([start, start + 1_000_000_000, start + 2_000_000_000]),
            probability=np.asarray([0.8, 0.6, 0.4]),
        )
    )

    current, change, age_ms = cursor.asof(
        start + 2_500_000_000,
        lookback_ms=1_500,
        max_age_ms=1_000,
    )
    assert current == pytest.approx(0.4)
    assert change == pytest.approx(-0.2)
    assert age_ms == pytest.approx(500.0)

    stale, stale_change, stale_age = cursor.asof(
        start + 4_000_000_000,
        lookback_ms=1_000,
        max_age_ms=500,
    )
    assert np.isnan(stale)
    assert np.isnan(stale_change)
    assert stale_age == pytest.approx(2_000.0)


def test_global_flow_cursor_has_right_edge_visibility_and_freshness_guard():
    start = 1_700_000_000_000_000_000
    data = HistoricalGlobalFlowData(
        ts_ns=np.asarray([start + 1_000_000_000, start + 2_000_000_000]),
        spot_move_bps=np.asarray([-0.5, 0.7]),
        perp_move_bps=np.asarray([-0.7, 0.9]),
        spot_flow_pressure=np.asarray([-0.4, 0.5]),
        perp_flow_pressure=np.asarray([-0.6, 0.7]),
        spot_venue_agreement=np.asarray([1.0, 1.0]),
        perp_venue_agreement=np.asarray([1.0, 1.0]),
        fresh_spot_venues=np.asarray([3, 3]),
        fresh_perp_venues=np.asarray([3, 3]),
        local_bridge_move_bps=np.asarray([-0.2, 0.3]),
        spot_source_age_ms=np.asarray([0.0, 0.0]),
        perp_source_age_ms=np.asarray([0.0, 0.0]),
        spot_valid=np.asarray([1, 1]),
        perp_valid=np.asarray([1, 1]),
        source_age_ms=np.asarray([0.0, 0.0]),
    )
    cursor = HistoricalGlobalFlowCursor(data, max_age_ms=1_500)

    before = cursor.asof(start + 999_999_999, horizon_ms=1_000)
    assert not before.window(1_000)

    exact = cursor.asof(start + 1_000_000_000, horizon_ms=1_000)
    exact_window = exact.window(1_000)
    assert exact_window is not None
    assert exact_window["spot"]["mid_move_bps"] == pytest.approx(-0.5)

    # The second row is still in the future here; the cursor must retain row 1.
    between = cursor.asof(start + 1_999_999_999, horizon_ms=1_000)
    assert between.window(1_000)["spot"]["mid_move_bps"] == pytest.approx(-0.5)

    stale = cursor.asof(start + 3_600_000_000, horizon_ms=1_000)
    stale_window = stale.window(1_000)
    assert stale_window is not None
    assert stale_window["valid"] == 0
    assert stale_window["spot"]["valid"] == 0


def test_global_flow_cursor_adds_embedded_source_age_to_cursor_age():
    start = 1_700_000_000_000_000_000
    data = HistoricalGlobalFlowData(
        ts_ns=np.asarray([start]),
        spot_move_bps=np.asarray([-0.5]),
        perp_move_bps=np.asarray([-0.7]),
        spot_flow_pressure=np.asarray([-0.4]),
        perp_flow_pressure=np.asarray([-0.6]),
        spot_venue_agreement=np.asarray([1.0]),
        perp_venue_agreement=np.asarray([1.0]),
        fresh_spot_venues=np.asarray([3]),
        fresh_perp_venues=np.asarray([3]),
        local_bridge_move_bps=np.asarray([-0.2]),
        spot_source_age_ms=np.asarray([1_200.0]),
        perp_source_age_ms=np.asarray([100.0]),
        spot_valid=np.asarray([1]),
        perp_valid=np.asarray([1]),
        source_age_ms=np.asarray([1_200.0]),
    )
    cursor = HistoricalGlobalFlowCursor(data, max_age_ms=1_500)
    window = cursor.asof(start + 400_000_000, horizon_ms=1_000).window(1_000)

    assert window["spot"]["valid"] == 0
    assert window["spot"]["source_age_ms"] == pytest.approx(1_600.0)
    assert window["perp"]["valid"] == 1
    assert window["perp"]["source_age_ms"] == pytest.approx(500.0)


def test_global_flow_visibility_delay_models_serialized_callback_stall():
    start = 1_700_000_000_000_000_000
    data = HistoricalGlobalFlowData(
        ts_ns=np.asarray(
            [start + 1_000_000_000, start + 2_000_000_000, start + 3_000_000_000]
        ),
        spot_move_bps=np.asarray([-0.5, 0.0, 0.5]),
        perp_move_bps=np.asarray([-0.7, 0.0, 0.7]),
        spot_flow_pressure=np.asarray([-0.4, 0.0, 0.4]),
        perp_flow_pressure=np.asarray([-0.6, 0.0, 0.6]),
        spot_venue_agreement=np.ones(3),
        perp_venue_agreement=np.ones(3),
        fresh_spot_venues=np.full(3, 3),
        fresh_perp_venues=np.full(3, 3),
        local_bridge_move_bps=np.asarray([-0.2, 0.0, 0.2]),
        spot_source_age_ms=np.zeros(3),
        perp_source_age_ms=np.zeros(3),
        spot_valid=np.ones(3),
        perp_valid=np.ones(3),
        source_age_ms=np.zeros(3),
    )

    delayed = apply_global_flow_visibility_delay(
        data,
        np.asarray([100.0, 1_500.0, 100.0]),
        profile_id="provider_neutral_test",
        mode="profile_stable_spike",
    )

    assert delayed.ts_ns.tolist() == [
        start + 1_100_000_000,
        start + 3_500_000_000,
        start + 3_500_000_000,
    ]
    assert "latency_profile=provider_neutral_test" in delayed.source
    assert data.ts_ns[0] == start + 1_000_000_000
