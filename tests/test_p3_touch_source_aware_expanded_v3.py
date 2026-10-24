import json
from pathlib import Path

import numpy as np

from research.families.f02_empirical_p3_touch.audit.p3_touch_quote_path_comparison import (
    _trace_frame,
    compare_quote_frames,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_source_aware_expanded import (
    _bootstrap_delta,
    empirical_curve,
    integrated_brier,
    reach_cache_key,
    validate_contract_structure,
)
from research.governance.public_machine_projection import source_document_path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "research/families/f02_empirical_p3_touch/docs"


def test_integrated_brier_matches_materialized_binary_surface():
    grid = np.asarray([0.1, 0.2, 0.3, 0.4], dtype=np.float64)
    probability = np.asarray([0.8, 0.6, 0.4, 0.2], dtype=np.float64)
    reaches = np.asarray([-np.inf, 0.15, 0.3, 1.0], dtype=np.float64)
    labels = reaches[:, None] >= grid[None, :]
    expected = float(np.mean(np.square(probability[None, :] - labels)))
    assert integrated_brier(reaches, probability, grid) == expected


def test_empirical_curve_preserves_no_touch_denominator_and_monotonicity():
    grid = np.asarray([0.1, 0.2, 0.3], dtype=np.float64)
    curve = empirical_curve(
        np.asarray([-np.inf, 0.1, 0.25, 0.5], dtype=np.float64), grid
    )
    assert curve.tolist() == [0.75, 0.5, 0.25]
    assert np.all(np.diff(curve) <= 0.0)


def test_reach_cache_key_binds_inputs_and_estimator():
    base = dict(
        day="2025-08-01",
        bbo_sha256="a" * 64,
        trade_sha256="b" * 64,
        horizon_s=10.0,
        max_bbo_age_ms=5000,
        estimator_sha256="c" * 64,
    )
    key = reach_cache_key(**base)
    assert len(key) == 64
    assert key == reach_cache_key(**base)
    assert key != reach_cache_key(**{**base, "trade_sha256": "d" * 64})
    assert key != reach_cache_key(**{**base, "estimator_sha256": "e" * 64})


def test_frozen_v3_contract_has_source_separation_and_no_authority():
    public_spec = DOCS / "p3_touch_source_aware_expanded_v3_spec_20260803.json"
    public_manifest = (
        DOCS / "p3_touch_source_aware_expanded_v3_day_manifest_20260803.json"
    )
    spec = json.loads(
        source_document_path(public_spec, require_private=True).read_text()
    )
    day_manifest = json.loads(
        source_document_path(public_manifest, require_private=True).read_text()
    )
    validate_contract_structure(spec, day_manifest)
    assert len(day_manifest["panels"]["fit_2025_provider"]) == 93
    assert len(day_manifest["panels"]["fit_2026_current"]) == 69
    assert len(day_manifest["panels"]["historical_2026_validation"]) == 24
    assert (
        len(day_manifest["panels"]["historical_2026_test_diagnostic"]) == 24
    )
    assert spec["fit_models"]["expanded"]["panels"] == [
        "fit_2025_provider",
        "fit_2026_current",
    ]
    assert spec["permissions"]["prediction_authority"] is False
    assert spec["permissions"]["action_authority"] is False
    assert spec["permissions"]["live_authority"] is False


def test_day_bootstrap_is_deterministic_and_uses_improvement_sign():
    values = np.asarray([-0.03, -0.01, 0.02, -0.04], dtype=np.float64)
    first = _bootstrap_delta(values, draws=2000, seed=7)
    second = _bootstrap_delta(values, draws=2000, seed=7)
    assert first == second
    assert first["mean_delta"] == float(np.mean(values))
    assert first["improved_day_rate"] == 0.75


def test_quote_path_coordinate_comparison_matches_timestamp_side_and_ordinal():
    current = _trace_frame(
        [
            {
                "order_id": 1,
                "side": "Side.Buy",
                "quote_ts": 1000,
                "final_price": 99.0,
                "raw_half_spread": 14.0,
                "final_pair_spread": 28.0,
                "outcome": "TraceOutcome.Cancelled",
                "cancel_reason": "CancelReason.RequoteReplace",
            },
            {
                "order_id": 2,
                "side": "Side.Sell",
                "quote_ts": 1000,
                "final_price": 101.0,
                "raw_half_spread": 14.0,
                "final_pair_spread": 28.0,
                "outcome": "TraceOutcome.Cancelled",
                "cancel_reason": "CancelReason.RequoteReplace",
            },
        ]
    )
    expanded = _trace_frame(
        [
            {
                "order_id": 7,
                "side": "Side.Buy",
                "quote_ts": 1000,
                "final_price": 99.1,
                "raw_half_spread": 13.8,
                "final_pair_spread": 27.6,
                "outcome": "TraceOutcome.Cancelled",
                "cancel_reason": "CancelReason.RequoteReplace",
            },
            {
                "order_id": 8,
                "side": "Side.Sell",
                "quote_ts": 1000,
                "final_price": 100.9,
                "raw_half_spread": 13.8,
                "final_pair_spread": 27.6,
                "outcome": "TraceOutcome.Cancelled",
                "cancel_reason": "CancelReason.RequoteReplace",
            },
        ]
    )
    rows = compare_quote_frames(current, expanded, tick_size=0.1)
    pooled = next(row for row in rows if row["side"] == "POOLED")
    assert pooled["matched_orders"] == 2
    assert pooled["matched_price_change_rate"] == 1.0
    assert np.isclose(pooled["mean_abs_price_change_ticks"], 1.0)
