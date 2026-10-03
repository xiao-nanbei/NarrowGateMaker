import pytest
import numpy as np

from models.backtest_tick import _deduplicate_l2_price_levels
from strategy.quote_core import _near_depth_total, quote_depth_from_l2_rows


def test_quote_depth_from_l2_rows_ignores_repeated_price_levels():
    depth = quote_depth_from_l2_rows(
        [100.0, 100.0, 99.9],
        [1.2, 1.2, 0.4],
        [100.1, 100.1, 100.2],
        [0.8, 0.8, 0.6],
    )

    assert depth.bids == ((100.0, 1.2), (99.9, 0.4))
    assert depth.asks == ((100.1, 0.8), (100.2, 0.6))
    assert _near_depth_total(depth, 3) == pytest.approx(3.0)


def test_l2_loader_helper_compacts_unique_levels_to_the_front():
    px = np.array([[100.0, 100.0, 99.9, 99.8]], dtype=np.float64)
    qty = np.array([[1.2, 1.2, 0.4, 0.2]], dtype=np.float64)

    clean_px, clean_qty, duplicate_count = _deduplicate_l2_price_levels(px, qty)

    assert duplicate_count == 1
    assert clean_px.tolist() == [[100.0, 99.9, 99.8, 0.0]]
    assert clean_qty.tolist() == [[1.2, 0.4, 0.2, 0.0]]
