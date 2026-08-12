from __future__ import annotations

import numpy as np
import pytest

from research.families.f06_placement_fill_cif.audit.request_state_features import (
    compute_request_state_features_native,
    compute_request_state_features_python,
    flatten_request_state,
)


def _inputs() -> dict[str, object]:
    return {
        "request_ts_ms": [100, 200],
        "book_cutoff_ts_ms": [100, 200],
        "trade_cutoff_ts_ms": [100, 200],
        "activation_ts_ms": [20, 150],
        "terminal_ts_ms": [150, 230],
        "cancel_ack_ts_ms": [120, 220],
        "bbo_ts_ms": [50, 90, 100, 190, 200],
        "bbo_best_bid": [99.0, 100.0, 500.0, 101.0, 700.0],
        "bbo_best_ask": [100.0, 101.0, 501.0, 102.0, 701.0],
        "bbo_bid_qty": [2.0, 3.0, 99.0, 4.0, 99.0],
        "bbo_ask_qty": [1.0, 1.0, 99.0, 2.0, 99.0],
        "l2_ts_ms": [40, 90, 100, 180, 200],
        "l2_bid_px": [[99.0, 98.0], [100.0, 99.0], [500.0, 499.0], [101.0, 100.0], [700.0, 699.0]],
        "l2_bid_qty": [[1.0, 2.0], [2.0, 3.0], [99.0, 99.0], [4.0, 2.0], [99.0, 99.0]],
        "l2_ask_px": [[100.0, 101.0], [101.0, 102.0], [501.0, 502.0], [102.0, 103.0], [701.0, 702.0]],
        "l2_ask_qty": [[1.0, 1.0], [1.0, 2.0], [99.0, 99.0], [2.0, 1.0], [99.0, 99.0]],
        "trade_ts_ms": [25, 75, 100, 175, 200],
        "trade_price": [99.5, 100.0, 900.0, 101.5, 900.0],
        "trade_qty": [1.0, 2.0, 99.0, 3.0, 99.0],
        "is_buyer_maker": [0, 1, 0, 0, 1],
        "windows_ms": [50, 100],
    }


def test_cpp_request_state_matches_python_reference() -> None:
    try:
        native = compute_request_state_features_native(**_inputs(), depth_levels=2)
    except RuntimeError as exc:
        if "narrowgate_cpp" in str(exc):
            pytest.skip(str(exc))
        raise
    reference = compute_request_state_features_python(**_inputs(), depth_levels=2)

    assert native.keys() == reference.keys()
    for name in native:
        np.testing.assert_allclose(native[name], reference[name], equal_nan=True)


def test_request_state_excludes_same_millisecond_market_events() -> None:
    result = compute_request_state_features_python(**_inputs(), depth_levels=2)

    assert result["best_bid"].tolist() == [100.0, 101.0]
    assert result["book_source_ts_ms"].tolist() == [90, 190]
    assert result["trade_count"].tolist() == [[1, 2], [1, 2]]
    assert result["aggressive_buy_qty"][0, 0] == 0.0
    assert result["aggressive_sell_qty"][0, 0] == 2.0


def test_request_state_flattens_windows_without_changing_estimand() -> None:
    result = compute_request_state_features_python(**_inputs(), depth_levels=2)
    frame = flatten_request_state(result)

    assert len(frame) == 2
    assert "request_market_return_bps_50ms" in frame
    assert "request_taker_imbalance_100ms" in frame
    assert "request_cancel_ack_ts_ms" not in frame
