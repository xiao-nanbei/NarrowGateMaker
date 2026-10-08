from copy import deepcopy
from types import SimpleNamespace

import pytest

from research.families.f08_side_taker_lifecycle.response_replay_probe import validate_plan


@pytest.fixture
def plan(monkeypatch):
    start = 1755043200_000_000_000
    value = dict(bundle='synthetic', account_start_ns=start,
                 account_end_ns=start+172800_000_000_000, params={})
    monkeypatch.setattr('data.runtime.ConsumerBundle', lambda _: SimpleNamespace(manifest={'plan': {
        'start_ns': value['account_start_ns']-120_000_000_000,
        'end_ns': value['account_end_ns'], 'include_response_observations': True}}))
    return value


def test_probe_preserves_original_parent(plan):
    before = deepcopy(plan)
    validate_plan(plan)
    assert plan == before


@pytest.mark.parametrize('field,value', [('risk_selection_mode', 'EC'),
    ('quote_schedule_mode', 'state_event'), ('replace_price_threshold_mode', 'dynamic_outward')])
def test_probe_rejects_other_strategies(plan, field, value):
    plan['params'][field] = value
    with pytest.raises(ValueError, match='retain B0'):
        validate_plan(plan)


def test_final_forbidden(plan):
    plan['account_start_ns'] = 1779926400_000_000_000
    plan['account_end_ns'] = plan['account_start_ns']+172800_000_000_000
    with pytest.raises(ValueError, match='non-Final'):
        validate_plan(plan)


def test_shortened_parent_forbidden(plan):
    plan['account_end_ns'] = plan['account_start_ns']+900_000_000_000
    with pytest.raises(ValueError, match='original warmed parent'):
        validate_plan(plan)
