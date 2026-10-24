from types import SimpleNamespace

import numpy as np
import pytest

from research.families.f08_side_taker_lifecycle.depth_current_information import CurrentInformation
from test_depth_observation import stream


def test_exact_prediction_index_not_nearest_later_feature():
    source = stream()
    info = CurrentInformation(source.profile)
    info.timeline = SimpleNamespace(identity='frozen', predictions=[np.array([0, 200]), np.array([.2, .9]), np.array([7., 99.])],
        manifest=dict(head_count=2, prediction_files=['clock', 'head'], feature_names=['original']))
    for tick in source:
        for observation in tick.observations:
            if observation.ready_ns <= 100000000:
                info.observe(observation)
        if tick.now_ns >= 100000000:
            break
    row = info.query(100000000, 'BUY', {'I_current': {'prediction_generation_index': 0}})
    assert row['frozen_prediction']['retained_input_columns'] == {'original': 7.}
    assert row['frozen_prediction']['outputs']['head'] == .2
    with pytest.raises(ValueError, match='future'):
        info.query(100000000, 'BUY', {'I_current': {'prediction_generation_index': 1}})


def test_price_tick_representation_is_not_policy_rounding():
    from decimal import Decimal
    from research.families.f08_side_taker_lifecycle.depth_regions import canonical_order_price
    assert canonical_order_price(115661.90000000001, '.1') == Decimal('115661.9')
    with pytest.raises(ValueError, match='tick aligned'):
        canonical_order_price(115661.94, '.1')
