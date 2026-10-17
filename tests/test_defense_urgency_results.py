from copy import deepcopy

import pytest

from research.families.f01_fixed_parameter_racing.defense_urgency_results import (
    freeze_selection, read_after_selection, read_selection_cells,
)
from tests.test_defense_urgency import BASELINE, calendar, selection


def rows():
    return [dict(row,baseline_id=BASELINE,binding_verified=True,
        terminal_cut_verified=True,accounting_verified=True) for row in selection()]


def test_selector_never_opens_other_rewards():
    cells=rows()
    index=[dict(parent_id=r['parent_id'],arm_id=r['arm_id']) for r in cells]
    index.append(dict(parent_id='parent203',arm_id='B0'))
    called=[]
    def reader(e):
        assert e['parent_id']!='parent203'
        called.append(e)
        return next(r for r in cells if all(r[k]==e[k] for k in e))
    assert read_selection_cells(calendar(),index,reader)==sorted(
        cells,key=lambda r:(r['parent_id'],r['arm_id']))
    assert len(called)==225


def test_missing_cell_rejects_before_any_reward_read():
    with pytest.raises(ValueError,match='all B25'):
        read_selection_cells(calendar(),rows()[:-1],lambda e:pytest.fail('premature reward read'))


def test_requires_actual_cell_admission():
    cells=rows()
    cells[0]['binding_verified']=False
    with pytest.raises(ValueError,match='admitted'):
        read_selection_cells(calendar(),cells,lambda e:e)


def test_frozen_choice_idempotent_and_other_rewards_wait(tmp_path):
    target=tmp_path/'selection.json'
    entry=dict(parent_id='parent203',arm_id='B0')
    called=[]
    def reader(e):
        called.append(e)
        return e
    kwargs=dict(selection_path=target,calendar=calendar(),run_identity='frozen-test')
    with pytest.raises(FileNotFoundError):
        read_after_selection(entry,reader,**kwargs)
    assert not called
    frozen=freeze_selection(target,calendar(),rows(),run_identity='frozen-test')
    assert freeze_selection(target,calendar(),rows(),run_identity='frozen-test')==frozen
    changed=deepcopy(rows())
    changed[0]['net_pnl']=1000
    with pytest.raises(ValueError,match='differs'):
        freeze_selection(target,calendar(),changed,run_identity='frozen-test')
    assert read_after_selection(entry,reader,**kwargs)==entry
    with pytest.raises(ValueError,match='mismatched'):
        read_after_selection(entry,reader,**dict(kwargs,run_identity='other'))
    assert len(called)==1
