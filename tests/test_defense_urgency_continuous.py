import json
from types import SimpleNamespace

import pytest

from research.families.f01_fixed_parameter_racing.defense_urgency import BASELINE, PATCHES
from research.families.f01_fixed_parameter_racing.defense_urgency_continuous import (
    continuous_bounds, admit_continuous_inputs,
)


def contract():
    return dict(continuous_final=dict(account_contract='FINAL107_CONTINUOUS',
        baseline_id=BASELINE, initialize_economic_state_once=True, reset_at_48h=False,
        preserve_response_state_and_compute_watermarks=True, scored_days=107,
        start_utc='2026-05-28T00:00:00Z', end_utc_exclusive='2026-09-12T00:00:00Z'),
        analysis_views={'B_SELECTION_PARENTS': {'parent_ids': ['B'+str(i) for i in range(25)]}})


def test_continuous_requires_global_endpoint_and_frozen_challenger(tmp_path):
    calendar = contract()
    start, end = continuous_bounds(calendar)
    selection = dict(schema='defense_urgency_selection.v1', baseline_id=BASELINE,
        run_identity='test-only', parent_ids=sorted(calendar['analysis_views']['B_SELECTION_PARENTS']['parent_ids']),
        frozen_challenger='DEFENSE_OFF__URGENCY_OFF', scores=dict.fromkeys(PATCHES, 0))
    path = tmp_path/'selection.json'
    path.write_text(json.dumps(selection))
    plan = dict(start_ns=start-86400*10**9, end_ns=end)
    prepared = SimpleNamespace(inputs={'bundle': SimpleNamespace(manifest={'plan': plan})})
    kwargs = dict(calendar=calendar, selection_path=path, run_identity='test-only')
    params = dict(account_start_ns=start)
    for arm in ('B0', selection['frozen_challenger']):
        assert admit_continuous_inputs(prepared, params, arm=arm, **kwargs) == (start, end)
    with pytest.raises(ValueError, match='challenger only'):
        admit_continuous_inputs(prepared, params, arm='DEFENSE_ON__URGENCY_OFF', **kwargs)
    plan['end_ns'] = start+2*86400*10**9
    with pytest.raises(ValueError, match='48h parent'):
        admit_continuous_inputs(prepared, params, arm='B0', **kwargs)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        admit_continuous_inputs(prepared, params, arm='B0', **kwargs)


def test_continuous_rejects_calendar_reset_or_shortening():
    calendar = contract()
    calendar['continuous_final']['reset_at_48h'] = True
    with pytest.raises(ValueError, match='authorized'):
        continuous_bounds(calendar)
    calendar = contract()
    calendar['continuous_final']['end_utc_exclusive'] = '2026-09-11T00:00:00Z'
    with pytest.raises(ValueError, match='authorized'):
        continuous_bounds(calendar)
