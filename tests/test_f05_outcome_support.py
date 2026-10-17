"""Production-entry contracts on synthetic opportunities, not study fitting."""
import json

import numpy as np
import pandas as pd
import pytest

from tests.test_research_public_inputs import bundle, binding, SECOND  # noqa: F401
from research.families.f05_fill_quality_quote_ev.public_input import (
    _outcome_support, build_opportunity_panel, train_opportunity_models,
)


def inputs(root):
    opportunities = pd.DataFrame([
        dict(opportunity_id=str(i), decision_ns=2 * SECOND, side="bid", **binding(root))
        for i in range(5)])
    outcomes = pd.DataFrame([
        dict(opportunity_id=str(i), horizon_ns=h * SECOND, actual_outcome_end_ns=33 * SECOND,
             right_censored=(i == 4 and h == 30), filled_quantity=0. if i < 2 else .1,
             markout_bps=None if i < 2 else -2. if i in (2, 4) else 2., **binding(root))
        for i in range(5) for h in (1, 5, 30)])
    return opportunities, outcomes


def prepare(root, opportunities, outcomes, **extra):
    return build_opportunity_panel(root, opportunities, outcomes, feature_columns=["mid"],
        missing_policy="reject", max_age_ns=SECOND, training_boundary_ns=40 * SECOND, **extra)


def test_explicit_administrative_censor_with_unknown_end_is_not_missing_metadata():
    rows = pd.DataFrame([dict(side='bid', horizon_ns=SECOND,
                              decision_ns=2*SECOND, actual_outcome_end_ns=pd.NA,
                              right_censored=censor, filled_quantity=float('nan'))
                         for censor in (True, False)])
    rows['actual_outcome_end_ns'] = rows.actual_outcome_end_ns.astype('Int64')
    eligible, counts = _outcome_support(rows, 4*SECOND)
    assert eligible.tolist() == [False, False]
    assert counts[f'bid:{SECOND}']['censored'] == 1
    assert counts[f'bid:{SECOND}']['missing_metadata'] == 1


def test_actual_builder_counts_unknown_boundary_gap_and_roundtrip(bundle, tmp_path):  # noqa: F811
    opportunities, outcomes = inputs(bundle)
    outcomes['actual_outcome_end_ns'] = outcomes.actual_outcome_end_ns.astype('Int64')
    outcomes.loc[0, 'actual_outcome_end_ns'] = pd.NA
    outcomes.loc[1, 'actual_outcome_end_ns'] = 40 * SECOND
    outcomes.loc[2, 'actual_outcome_end_ns'] = 39 * SECOND
    path = tmp_path / 'outcomes.parquet'
    outcomes.to_parquet(path)
    panel = prepare(bundle, opportunities, pd.read_parquet(path))
    stats = panel.attrs['outcome_support_counts']
    assert stats[f'bid:{SECOND}']['missing_metadata'] == 1
    assert stats[f'bid:{5*SECOND}']['out_of_support'] == 1
    assert stats[f'bid:{30*SECOND}']['censored'] == 1
    assert panel.loc[panel.fill_label == 0, 'opportunity_markout_bps'].eq(0).all()
    assert panel.loc[panel.fill_label == 0, 'conditional_markout_bps'].isna().all()
    # All decisions are inside the first interval; endpoints crossing the gap
    # remain inadmissible even though they are below the global maximum.
    gap = prepare(bundle, opportunities, outcomes,
                  training_intervals_ns=[[0, 10*SECOND], [20*SECOND, 40*SECOND]])
    assert gap.empty
    explicit = prepare(bundle, opportunities, outcomes, training_intervals_ns=((0, 40*SECOND),))
    explicit.to_parquet(tmp_path / 'explicit.parquet')
    assert pd.read_parquet(tmp_path / 'explicit.parquet').attrs['training_intervals_ns'] == [[0, 40*SECOND]]
    panel.to_parquet(tmp_path / 'panel.parquet')
    restored = pd.read_parquet(tmp_path / 'panel.parquet')
    assert restored.attrs == panel.attrs
    assert restored.conditional_markout_bps.isna().sum() == panel.conditional_markout_bps.isna().sum()


@pytest.mark.parametrize("side", ["bid", "ask"])
def test_actual_training_entry_per_head_support_with_interface_stub(bundle, tmp_path, monkeypatch, side):  # noqa: F811
    import lightgbm
    opportunities, outcomes = inputs(bundle)
    opportunities['side'] = side
    outcomes.loc[(outcomes.opportunity_id == '4') & (outcomes.horizon_ns == SECOND), 'markout_bps'] = np.nan
    panel = prepare(bundle, opportunities, outcomes)
    path = tmp_path / 'panel.parquet'
    panel.to_parquet(path)
    panel = pd.read_parquet(path)
    seen = []

    class InterfaceOnlyBooster:
        def save_model(self, path):
            from pathlib import Path
            Path(path).write_text('INTERFACE TEST ONLY; NOT A TRAINED MODEL')

    def train(params, dataset, **kwargs):
        seen.append((params['objective'], len(dataset.data), list(dataset.label)))
        assert np.isfinite(dataset.label).all()
        return InterfaceOnlyBooster()

    monkeypatch.setattr(lightgbm, 'train', train)
    identity = {k: panel.attrs[k] for k in ('input_contract_id', 'observation_contract_id', 'feature_contract_id')}
    identity.update(source_manifest_sha256=panel.attrs['input_manifest_id'],
                    training_contract_id='synthetic-interface-only', label_contract_id='explicit-outcomes')
    args = dict(input_identity=identity, feature_columns=['mid'], side=side, missing_policy='reject',
                training_boundary_ns=40*SECOND, bucket_edges=[0.], bucket_values=[-2., 2.],
                adverse_threshold_bps=0., parameters={}, num_boost_round=1)
    train_opportunity_models(panel, tmp_path / 'stub', **args)
    assert [n for _, n, _ in seen] == [4, 2, 3, 2, 2]
    for file in (tmp_path / 'stub').glob('*_meta.json'):
        meta = json.loads(file.read_text())
        assert meta['actual_outcome_end_max_ns'] < meta['training_boundary_ns']
        assert meta['panel_outcome_support_counts'][f'{side}:{30*SECOND}']['censored'] == 1
    with pytest.raises(ValueError, match='interval support differs'):
        train_opportunity_models(panel, tmp_path / 'bad', **args, training_intervals_ns=[[0, 40*SECOND]])
