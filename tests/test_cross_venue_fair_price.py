import math
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from live.config import (
    Config,
    ExternalVenueSourceConfig,
    _validate_config,
)
from market_fusion import BINANCE_VENUE
from research.families.f04_external_market_alpha.audit.cross_venue_causal_fair_price import (
    HISTORICAL_SOURCE_KIND,
    HistoricalFairPriceCursor,
    HistoricalFairPriceData,
    load_common_support_variants,
)
from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceConfig,
    CrossVenueFairPriceEstimator,
    CrossVenueFairPriceState,
    FairPriceSource,
    project_fair_center_shadow,
    weighted_median,
)
from strategy.maker_engine import CrossVenueFairPriceShadowLogRow, MakerEngine


def _source(
    venue: str,
    market: str,
    mid: float,
    ready_ns: int,
    *,
    receive_lag_ns: int = 1_000_000,
) -> FairPriceSource:
    return FairPriceSource(
        venue=venue,
        market_type=market,
        bid=mid - 0.5,
        ask=mid + 0.5,
        exchange_ts_ns=ready_ns - 2_000_000,
        local_receive_ts_ns=ready_ns - receive_lag_ns,
        feature_ready_ts_ns=ready_ns,
    )


def _state(*, shift_price: float, valid: bool = True) -> CrossVenueFairPriceState:
    return CrossVenueFairPriceState(
        schema_version="cross_venue_causal_fair_price.v1",
        decision_ts_ns=1,
        valid=valid,
        reason="valid" if valid else "invalid",
        local_mid=100.0,
        fair_price=100.0 + shift_price,
        raw_lead_bps=shift_price * 100.0,
        gain=1.0,
        center_shift_price=shift_price,
        center_shift_bps=shift_price * 100.0,
        confidence=1.0,
        dispersion_bps=0.0,
        valid_venues=3,
        venue_ids=("bitget", "bybit", "okx"),
        minimum_basis_samples=30,
        lead_variance_bps2=1.0,
        noise_variance_bps2=0.1,
        max_source_age_ms=1.0,
        max_feed_latency_ms=0.5,
        max_feature_latency_ms=0.5,
        source_kinds=("receive_time_bbo",),
        transport_supported=True,
    )


def test_weighted_median_prevents_light_outlier_from_setting_fair_price() -> None:
    assert weighted_median([(100.0, 1.0), (100.1, 1.0), (120.0, 0.1)]) == 100.1


def test_weighted_median_is_symmetric_for_two_equal_weight_loo_venues() -> None:
    assert weighted_median([(100.0, 1.0), (100.2, 1.0)]) == pytest.approx(100.1)


def test_historical_cursor_is_backward_only_and_transport_unsupported() -> None:
    data = HistoricalFairPriceData(
        feature_ready_ts_ns=np.array([1_000_000_000, 2_000_000_000]),
        fair_price=np.array([101.0, 102.0]),
        gain=np.array([0.5, 0.75]),
        confidence=np.array([0.4, 0.6]),
        dispersion_bps=np.array([0.2, 0.3]),
        valid_venues=np.array([3, 3]),
        minimum_basis_samples=np.array([30, 31]),
        lead_variance_bps2=np.array([1.0, 2.0]),
        noise_variance_bps2=np.array([0.5, 0.5]),
        max_source_age_ms=np.array([20.0, 30.0]),
        valid=np.array([1, 1]),
        reason=np.array(["valid", "valid"]),
    )
    cursor = HistoricalFairPriceCursor(data, max_state_age_ms=1_000.0)
    state = cursor.asof(1_500_000_000, local_mid=100.0)
    assert state.valid
    assert state.fair_price == 101.0
    assert state.center_shift_price == pytest.approx(0.5)
    assert state.source_kinds == (HISTORICAL_SOURCE_KIND,)
    assert not state.transport_supported

    state = cursor.asof(2_000_000_000, local_mid=100.0)
    assert state.fair_price == 102.0


def test_loo_variants_share_one_hash_verified_visibility_mask(
    tmp_path: Path,
) -> None:
    rows = []
    validity = {
        "all_venues": [1, 1, 1],
        "leave_bitget_out": [1, 0, 1],
        "leave_bybit_out": [1, 1, 1],
        "leave_okx_out": [1, 1, 0],
    }
    for variant, valid in validity.items():
        frame = pd.DataFrame(
            {
                "feature_ready_ts_ns": [1, 2, 3],
                "fair_price": [100.0, 100.1, 100.2],
                "gain": [0.5, 0.5, 0.5],
                "confidence": [1.0, 1.0, 1.0],
                "dispersion_bps": [0.1, 0.1, 0.1],
                "valid_venues": [3, 3, 3],
                "minimum_basis_samples": [30, 30, 30],
                "lead_variance_bps2": [1.0, 1.0, 1.0],
                "noise_variance_bps2": [0.5, 0.5, 0.5],
                "max_source_age_ms": [0.0, 0.0, 0.0],
                "valid": valid,
                "reason": ["valid", "valid", "valid"],
            }
        )
        path = tmp_path / f"{variant}.parquet"
        frame.to_parquet(path, index=False)
        import hashlib

        rows.append(
            {
                "day": "2026-04-20",
                "variant": variant,
                "output_path": str(path),
                "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )

    outputs, audit = load_common_support_variants(
        pd.DataFrame(rows),
        "2026-04-20",
    )

    assert audit["common_valid_rows"] == 1
    assert audit["identical_validity_denominator"] is True
    assert all(data.valid.tolist() == [1, 0, 0] for data in outputs.values())


def test_mid_override_never_masquerades_as_bbo() -> None:
    source = FairPriceSource(
        venue="bitget",
        market_type="perp",
        bid=math.nan,
        ask=math.nan,
        exchange_ts_ns=1,
        local_receive_ts_ns=2,
        feature_ready_ts_ns=3,
        mid_override=100.25,
        source_kind=HISTORICAL_SOURCE_KIND,
        transport_supported=False,
    )
    assert source.mid == 100.25
    assert not source.transport_supported


def test_estimator_is_past_only_and_requires_basis_and_gain_warmup() -> None:
    config = CrossVenueFairPriceConfig(
        minimum_basis_samples=2,
        minimum_gain_samples=2,
        basis_half_life_s=1.0,
        variance_half_life_s=1.0,
    )
    estimator = CrossVenueFairPriceEstimator(config)
    anchor = 1.0
    state = None
    for second in range(1, 8):
        ready_ns = second * 1_000_000_000
        local = 100.0 + 0.01 * second
        sources = [
            _source(venue, market, local + 0.02 * (index - 1), ready_ns)
            for index, venue in enumerate(("bitget", "bybit", "okx"))
            for market in ("spot", "perp")
        ]
        state = estimator.observe(
            decision_ts_ns=ready_ns + 10_000_000,
            local_mid=local,
            stablecoin_mid=anchor,
            stablecoin_feature_ready_ts_ns=ready_ns,
            sources=sources,
        )
    assert state is not None
    assert state.valid
    assert state.reason == "valid"
    assert state.valid_venues == 3
    assert 0.0 <= state.gain <= 1.0
    assert math.isfinite(state.fair_price)


def test_future_feature_ready_source_is_never_admitted() -> None:
    estimator = CrossVenueFairPriceEstimator(
        CrossVenueFairPriceConfig(minimum_basis_samples=2, minimum_gain_samples=2)
    )
    decision_ns = 10_000_000_000
    state = estimator.observe(
        decision_ts_ns=decision_ns,
        local_mid=100.0,
        stablecoin_mid=1.0,
        stablecoin_feature_ready_ts_ns=decision_ns - 1,
        sources=[
            _source(venue, "perp", 100.0, decision_ns + 1)
            for venue in ("bitget", "bybit", "okx")
        ],
    )
    assert not state.valid
    assert state.valid_venues == 0


def test_projection_moves_whole_pair_and_preserves_gtx_and_spread() -> None:
    shadow = project_fair_center_shadow(
        _state(shift_price=0.2),
        baseline_bid=99.0,
        baseline_ask=101.0,
        best_bid=99.5,
        best_ask=100.5,
        tick_size=0.1,
    )
    assert shadow.valid
    assert shadow.requested_shift_ticks == 2
    assert shadow.effective_shift_ticks == 2
    assert shadow.candidate_bid == pytest.approx(99.2)
    assert shadow.candidate_ask == pytest.approx(101.2)
    assert shadow.candidate_ask - shadow.candidate_bid == pytest.approx(2.0)
    assert shadow.candidate_bid < 100.5
    assert shadow.candidate_ask > 99.5


def test_projection_clamps_pair_shift_without_changing_pair_spread() -> None:
    shadow = project_fair_center_shadow(
        _state(shift_price=2.0),
        baseline_bid=99.9,
        baseline_ask=100.9,
        best_bid=99.8,
        best_ask=100.1,
        tick_size=0.1,
    )
    assert shadow.valid
    assert shadow.gtx_clamped
    assert shadow.effective_shift_ticks == 1
    assert shadow.candidate_bid == pytest.approx(100.0)
    assert shadow.candidate_ask == pytest.approx(101.0)
    assert shadow.candidate_ask - shadow.candidate_bid == pytest.approx(1.0)


def test_invalid_state_falls_back_to_baseline_pair() -> None:
    shadow = project_fair_center_shadow(
        _state(shift_price=1.0, valid=False),
        baseline_bid=99.0,
        baseline_ask=101.0,
        best_bid=99.5,
        best_ask=100.5,
        tick_size=0.1,
    )
    assert not shadow.valid
    assert shadow.candidate_bid == 99.0
    assert shadow.candidate_ask == 101.0


def test_signal_engine_admits_only_feature_ready_external_bbo() -> None:
    from strategy.signal import SignalEngine

    engine = SignalEngine(
        enable_ml=False,
        symbol="BTCUSDC",
        reference_symbol="BTCUSDT",
        stablecoin_anchor_symbol="USDCUSDT",
    )
    engine._cross_venue_fair_price = CrossVenueFairPriceEstimator(
        CrossVenueFairPriceConfig(
            minimum_basis_samples=2,
            minimum_gain_samples=2,
            basis_half_life_s=1.0,
            variance_half_life_s=1.0,
        )
    )
    state = None
    for cycle in range(8):
        receive_ns = time.time_ns() - 1_000_000
        event_ms = (receive_ns - 1_000_000) // 1_000_000
        engine.on_book_ticker(
            {
                "s": "USDCUSDT",
                "b": "0.9999",
                "a": "1.0001",
                "E": event_ms,
            },
            market_type="spot",
            venue=BINANCE_VENUE,
            receive_ts_ns=receive_ns,
        )
        local = 60_000.0 + cycle * 0.5
        for venue_index, venue in enumerate(("bitget", "bybit", "okx")):
            for market in ("spot", "perp"):
                mid = local + (venue_index - 1) * 0.1
                engine.on_book_ticker(
                    {
                        "s": "BTCUSDT",
                        "b": str(mid - 0.05),
                        "a": str(mid + 0.05),
                        "E": event_ms,
                    },
                    market_type=market,
                    venue=venue,
                    receive_ts_ns=receive_ns,
                )
        decision_ns = time.time_ns()
        state = engine.cross_venue_fair_price_state(
            local_mid=local,
            now_ns=decision_ns,
        )
        snapshot = engine.market_source_snapshot(now_ns=decision_ns)
        assert all(
            int(row.get("last_book_feature_ready_ts_ns", 0)) <= decision_ns
            for row in snapshot.values()
            if int(row.get("last_book_feature_ready_ts_ns", 0)) > 0
        )

    assert state is not None and state.valid
    assert state.valid_venues == 3


def test_config_requires_shadow_only_two_venue_support() -> None:
    cfg = Config()
    cfg.strategy.cross_venue_fair_price_shadow_enabled = True
    cfg.multi_market.enabled = True
    cfg.external_venues.enabled = True
    cfg.external_venues.shadow_only = True
    cfg.external_venues.sources = [
        ExternalVenueSourceConfig(venue="bitget", enabled=True),
    ]
    with pytest.raises(ValueError, match="at least two enabled venues"):
        _validate_config(cfg)


def test_maker_shadow_writes_candidate_without_action_authority(
    tmp_path: Path,
) -> None:
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        strategy=SimpleNamespace(cross_venue_fair_price_shadow_enabled=True),
        tick_size=0.1,
    )
    engine._best_bid = 99.5
    engine._best_ask = 100.5
    engine.signal = SimpleNamespace(
        cross_venue_fair_price_state=lambda **_: _state(shift_price=0.2)
    )
    engine._cross_venue_fair_price_shadow_log_path = str(tmp_path / "shadow.csv")
    engine._csv_log_lock = threading.Lock()
    engine._cross_venue_fair_price_shadow_rows = 0
    engine._cross_venue_fair_price_shadow_valid_rows = 0
    engine._cross_venue_fair_price_shadow_last_time = 0.0
    engine._cross_venue_fair_price_shadow_last_warning = 0.0
    MakerEngine._init_csv_log(
        engine._cross_venue_fair_price_shadow_log_path,
        list(CrossVenueFairPriceShadowLogRow.__dataclass_fields__.keys()),
    )

    baseline = (99.0, 101.0)
    shadow = engine._record_cross_venue_fair_price_shadow(
        symbol="BTCUSDC",
        mid=100.0,
        baseline_bid=baseline[0],
        baseline_ask=baseline[1],
        best_bid=99.5,
        best_ask=100.5,
        decision_ts_ns=10,
    )

    assert baseline == (99.0, 101.0)
    assert shadow.candidate_bid == pytest.approx(99.2)
    assert shadow.candidate_ask == pytest.approx(101.2)
    payload = (tmp_path / "shadow.csv").read_text()
    assert ",0,shadow_only," in payload
