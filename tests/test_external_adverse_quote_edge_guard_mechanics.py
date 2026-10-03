import math
from pathlib import Path

import pandas as pd
import pytest

from research.families.f04_external_market_alpha.audit.external_adverse_quote_edge_guard_mechanics import (
    load_quote_opportunities,
    project_adverse_edge_guard,
    quote_role,
)
from strategy.cross_venue_fair_price import (
    CrossVenueFairPriceState,
    VenueFairPriceState,
)
from strategy.external_adverse_quote_edge_guard import (
    project_external_adverse_quote_edge,
)


def _venue(name: str, fair: float, *, weight: float = 1.0) -> VenueFairPriceState:
    return VenueFairPriceState(
        venue=name,
        fair_price=fair,
        weight=weight,
        source_count=2,
        max_source_age_ms=5.0,
        max_feed_latency_ms=3.0,
        max_feature_latency_ms=0.1,
        tracking_variance_bps2=0.01,
        minimum_basis_samples=40,
        source_kinds=("aws_tokyo_receive_time_bbo",),
        transport_supported=True,
    )


def _state(local: float, fairs: tuple[float, float, float]) -> CrossVenueFairPriceState:
    venues = {
        name: _venue(name, fair)
        for name, fair in zip(("bitget", "bybit", "okx"), fairs, strict=True)
    }
    fair = sorted(fairs)[1]
    return CrossVenueFairPriceState(
        schema_version="cross_venue_causal_fair_price.v1",
        decision_ts_ns=1,
        valid=True,
        reason="valid",
        local_mid=local,
        fair_price=fair,
        raw_lead_bps=math.log(fair / local) * 1e4,
        gain=0.5,
        center_shift_price=0.0,
        center_shift_bps=0.0,
        confidence=1.0,
        dispersion_bps=0.1,
        valid_venues=3,
        venue_ids=("bitget", "bybit", "okx"),
        minimum_basis_samples=40,
        lead_variance_bps2=1.0,
        noise_variance_bps2=0.1,
        max_source_age_ms=5.0,
        max_feed_latency_ms=3.0,
        max_feature_latency_ms=0.1,
        source_kinds=("aws_tokyo_receive_time_bbo",),
        transport_supported=True,
        venues=venues,
    )


def test_role_contract_keeps_reducing_surface_separate() -> None:
    assert quote_role("BUY", 0.0) == "opener"
    assert quote_role("SELL", 0.0) == "opener"
    assert quote_role("BUY", 0.2) == "add"
    assert quote_role("SELL", -0.2) == "add"
    assert quote_role("BUY", -0.2) == "reducing"
    assert quote_role("SELL", 0.2) == "reducing"


def test_positive_external_edge_only_moves_sell_outward() -> None:
    projection = project_adverse_edge_guard(
        _state(100.0, (100.5, 100.6, 100.7)),
        baseline_bid=99.9,
        baseline_ask=100.1,
        tick_size=0.1,
        max_pair_spread_bps=100.0,
    )
    assert projection.valid
    assert projection.loo_consistent
    assert projection.adverse_side == "SELL"
    assert projection.candidate_bid == pytest.approx(99.9)
    assert projection.candidate_ask > 100.1


def test_negative_external_edge_only_moves_buy_outward() -> None:
    projection = project_adverse_edge_guard(
        _state(100.0, (99.3, 99.4, 99.5)),
        baseline_bid=99.9,
        baseline_ask=100.1,
        tick_size=0.1,
        max_pair_spread_bps=100.0,
    )
    assert projection.valid
    assert projection.adverse_side == "BUY"
    assert projection.candidate_bid < 99.9
    assert projection.candidate_ask == pytest.approx(100.1)


def test_spread_cap_clips_without_ever_tightening() -> None:
    projection = project_adverse_edge_guard(
        _state(100.0, (101.0, 101.1, 101.2)),
        baseline_bid=99.95,
        baseline_ask=100.05,
        tick_size=0.05,
        max_pair_spread_bps=20.0,
    )
    assert projection.valid
    assert projection.cap_clipped
    assert projection.candidate_bid == pytest.approx(99.95)
    assert projection.candidate_ask >= 100.05
    assert projection.candidate_ask - projection.candidate_bid <= 0.2 + 1e-12


def test_loo_direction_disagreement_fails_closed() -> None:
    projection = project_adverse_edge_guard(
        _state(100.0, (99.0, 100.0, 101.0)),
        baseline_bid=99.9,
        baseline_ask=100.1,
        tick_size=0.1,
        max_pair_spread_bps=100.0,
    )
    assert not projection.valid
    assert projection.reason == "loo_direction_disagreement"
    assert projection.candidate_bid == pytest.approx(99.9)
    assert projection.candidate_ask == pytest.approx(100.1)


@pytest.mark.parametrize(
    "fairs",
    [
        (100.5, 100.6, 100.7),
        (99.3, 99.4, 99.5),
        (99.0, 100.0, 101.0),
    ],
)
def test_runtime_projection_matches_frozen_v1_mechanics(
    fairs: tuple[float, float, float],
) -> None:
    state = _state(100.0, fairs)
    expected = project_adverse_edge_guard(
        state,
        baseline_bid=99.9,
        baseline_ask=100.1,
        tick_size=0.1,
        max_pair_spread_bps=100.0,
    )
    observed = project_external_adverse_quote_edge(
        state,
        baseline_bid=99.9,
        baseline_ask=100.1,
        tick_size=0.1,
        max_pair_spread_bps=100.0,
    )

    assert observed.valid == expected.valid
    assert observed.reason == expected.reason
    assert observed.adverse_side == expected.adverse_side
    assert observed.requested_ticks == expected.requested_ticks
    assert observed.effective_ticks == expected.effective_ticks
    assert observed.candidate_bid == pytest.approx(expected.candidate_bid)
    assert observed.candidate_ask == pytest.approx(expected.candidate_ask)


def _quote_rows(timestamp: float) -> pd.DataFrame:
    common = {
        "timestamp": timestamp,
        "symbol": "BTCUSDC",
        "mode": "normal",
        "allow_post": 1,
        "allow_exposure_increase": 1,
        "inventory_ratio": 0.0,
        "mid": 100.0,
        "final_size": 0.001,
        "can_post_after_inventory": 1,
        "order_active_before": 1,
        "needs_update": 0,
        "action": "keep",
    }
    return pd.DataFrame(
        [
            {**common, "side": "BUY", "final_price": 99.9},
            {**common, "side": "SELL", "final_price": 100.1},
        ]
    )


def test_overlapping_quote_logs_are_hash_bound_and_deduplicated(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv.gz"
    inventory = tmp_path / "inventory.csv"
    frame = _quote_rows(1_700_000_000.123)
    frame["inventory_ratio"] = 0.5
    frame.to_csv(first, index=False)
    frame.to_csv(second, index=False, compression="gzip")
    pd.DataFrame(
        [{"timestamp": 1_700_000_000.100, "position": -0.002}]
    ).to_csv(inventory, index=False)

    output, audit = load_quote_opportunities(
        [first, second], inventory_state_paths=[inventory]
    )

    assert len(output) == 1
    assert output.loc[0, "inventory_q"] == pytest.approx(-0.002)
    assert output.loc[0, "inventory_ratio_abs"] == pytest.approx(0.5)
    assert bool(output.loc[0, "inventory_state_available"])
    assert not bool(output.loc[0, "inventory_same_ms_ambiguous"])
    assert audit["input_pairs"] == 2
    assert audit["unique_pairs"] == 1
    assert audit["deduplicated_pairs"] == 1
    assert audit["conflicting_pairs"] == 0
    assert not audit["exact_decision_start_clock_available"]
    assert audit["inventory_ratio_field_semantics"] == "absolute_magnitude_only"
    assert audit["inventory_join"]["role_available_pairs"] == 1


def test_same_millisecond_inventory_state_is_not_used_for_role(tmp_path: Path) -> None:
    quotes = tmp_path / "quotes.csv"
    inventory = tmp_path / "inventory.csv"
    frame = _quote_rows(1_700_000_000.123)
    frame.to_csv(quotes, index=False)
    pd.DataFrame(
        [
            {"timestamp": 1_700_000_000.100, "position": 0.001},
            {"timestamp": 1_700_000_000.123, "position": -0.001},
        ]
    ).to_csv(inventory, index=False)

    output, audit = load_quote_opportunities(
        [quotes], inventory_state_paths=[inventory]
    )

    assert output.loc[0, "inventory_q"] == pytest.approx(0.001)
    assert bool(output.loc[0, "inventory_same_ms_ambiguous"])
    assert audit["inventory_join"]["same_millisecond_ambiguous_pairs"] == 1
    assert audit["inventory_join"]["role_available_pairs"] == 0
