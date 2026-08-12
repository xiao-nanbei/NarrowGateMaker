import math

import pytest

from market_fusion import (
    STABLECOIN_ANCHOR_ROLE,
    build_external_reference_specs,
    build_market_specs,
)
from strategy.global_reference import ReferenceObservation, build_global_reference_state
from strategy.signal import SignalEngine


def _obs(move_bps: float, *, mid: float = 60_000.0, age_ms: float = 100.0):
    prior = mid / math.exp(move_bps / 10_000.0)
    return ReferenceObservation(mid=mid, prior_mid=prior, source_age_ms=age_ms)


def test_external_market_specs_keep_venue_and_instrument_separate():
    specs = build_external_reference_specs()

    assert len(specs) == 6
    assert len({spec.market_id for spec in specs}) == 6
    okx = {spec.market_type: spec for spec in specs if spec.venue == "okx"}
    assert okx["perp"].instrument_id == "BTC-USDT-SWAP"
    assert okx["perp"].contract_multiplier == pytest.approx(0.01)
    assert okx["spot"].instrument_id == "BTC-USDT"


def test_enhanced_binance_specs_include_usdcusdt_as_non_voting_anchor():
    specs = build_market_specs("BTCUSDC", "enhanced", "BTCUSDT", "USDCUSDT")
    stablecoin = [spec for spec in specs if spec.role == STABLECOIN_ANCHOR_ROLE]

    assert len(stablecoin) == 1
    assert stablecoin[0].market_id == "binance:spot:USDCUSDT"


def test_three_venue_median_rejects_one_outlier_and_converts_usdc():
    spot = {"bitget": _obs(1.0), "bybit": _obs(1.2), "okx": _obs(20.0)}
    perp = {"bitget": _obs(1.1), "bybit": _obs(1.3), "okx": _obs(18.0)}
    state = build_global_reference_state(
        external_spot=spot,
        external_perp=perp,
        binance_btcusdt_perp=_obs(0.4, mid=60_000.0),
        execution_btcusdc_perp_mid=59_940.0,
        usdcusdt_mid=1.001,
        slow_bridge_basis_bps=0.0,
        max_dispersion_bps=25.0,
        correction_cap_bps=1.0,
    )

    assert state.valid
    assert state.global_spot_move_bps == pytest.approx(1.2)
    assert state.global_perp_move_bps == pytest.approx(1.3)
    assert state.local_bridge_px_usdc == pytest.approx(60_000.0 / 1.001)
    assert state.fresh_spot_venues == 3
    assert state.fresh_perp_venues == 3
    assert state.consensus_direction == 1


def test_two_fresh_venues_have_lower_confidence_than_three():
    common = dict(
        binance_btcusdt_perp=_obs(0.2),
        execution_btcusdc_perp_mid=60_000.0,
        binance_btcusdc_spot_mid=60_000.0,
        slow_bridge_basis_bps=0.0,
        max_dispersion_bps=2.0,
    )
    two = {"bitget": _obs(1.0), "bybit": _obs(1.1)}
    three = {**two, "okx": _obs(1.05)}

    state_two = build_global_reference_state(external_spot=two, external_perp=two, **common)
    state_three = build_global_reference_state(external_spot=three, external_perp=three, **common)

    assert state_two.valid and state_three.valid
    assert state_two.confidence < state_three.confidence


def test_spot_perp_direction_conflict_disables_external_correction():
    up = {"bitget": _obs(1.0), "bybit": _obs(1.1), "okx": _obs(0.9)}
    down = {"bitget": _obs(-1.0), "bybit": _obs(-1.1), "okx": _obs(-0.9)}

    state = build_global_reference_state(
        external_spot=up,
        external_perp=down,
        binance_btcusdt_perp=_obs(0.0),
        execution_btcusdc_perp_mid=60_000.0,
        binance_btcusdc_spot_mid=60_000.0,
        slow_bridge_basis_bps=0.0,
    )

    assert not state.valid
    assert state.validity_reason == "direction"
    assert state.confidence == 0.0
    assert state.external_correction_bps == 0.0


def test_book_history_respects_local_receive_time_at_prior_endpoint():
    engine = SignalEngine(enable_ml=False)
    key = engine._market_key("perp", "BTCUSDT", venue="okx")
    engine._record_book_ticker(
        key,
        60_000.0,
        60_000.1,
        1_000.0,
        receive_time_ms=2_000.0,
    )

    assert engine._book_ticker_record_at(
        "perp", "BTCUSDT", 1_500.0, venue="okx"
    ) is None
    assert engine._book_ticker_record_at(
        "perp", "BTCUSDT", 2_100.0, venue="okx"
    ) is not None


def test_binance_bridge_basis_is_sampled_per_10s_bucket_before_health():
    engine = SignalEngine(enable_ml=False, stablecoin_anchor_symbol="USDCUSDT")
    for index in range(31):
        timestamp_ms = 1_000_000 + index * 10_000
        receive_ns = timestamp_ms * 1_000_000
        for market_type, symbol, mid in (
            ("perp", "BTCUSDC", 59_950.0),
            ("perp", "BTCUSDT", 60_000.0),
            ("spot", "BTCUSDC", 59_940.0),
            ("spot", "USDCUSDT", 1.001),
        ):
            engine.on_book_ticker(
                {"s": symbol, "b": str(mid - 0.05), "a": str(mid + 0.05), "E": timestamp_ms},
                market_type=market_type,
                receive_ts_ns=receive_ns,
            )

    assert len(engine._global_bridge_basis_history) == 31
    assert len({bucket for bucket, _ in engine._global_bridge_basis_history}) == 31


def test_slow_stablecoin_anchor_does_not_need_a_two_second_book_change():
    engine = SignalEngine(enable_ml=False, stablecoin_anchor_symbol="USDCUSDT")
    for index in range(31):
        timestamp_ms = 1_000_000 + index * 10_000
        receive_ns = timestamp_ms * 1_000_000
        if index % 2 == 0:
            engine.on_book_ticker(
                {"s": "USDCUSDT", "b": "1.0009", "a": "1.0011", "E": timestamp_ms},
                market_type="spot",
                receive_ts_ns=receive_ns,
            )
        for market_type, symbol, mid in (
            ("perp", "BTCUSDT", 60_000.0),
            ("perp", "BTCUSDC", 59_940.0),
        ):
            engine.on_book_ticker(
                {"s": symbol, "b": str(mid - 0.05), "a": str(mid + 0.05), "E": timestamp_ms},
                market_type=market_type,
                receive_ts_ns=receive_ns,
            )

    assert len(engine._global_bridge_basis_history) == 31
