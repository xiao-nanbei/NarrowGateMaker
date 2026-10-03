"""Synthetic admission tests; no private replay or fitted model is asserted."""

import json

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.logged_outcomes import TARGET
from research.families.f05_fill_quality_quote_ev.training_binding import (
    account_support, admit_account, combine_training_panels, digest, fit_bound_training,
    preflight_head_support, training_only_calibration,
)
from tests.test_research_public_inputs import SECOND, binding, bundle  # noqa: F401


def _contract(parent):
    return dict(target=TARGET, post_fill_horizons_ms=[1000, 5000, 30000],
        source_commit='synthetic-source', baseline=dict(model_manifest_sha256='synthetic-model'),
        feature_columns=['mid'], missing_policy='reject',
        training=[dict(index=70, shard='synthetic-070', start_utc='1970-01-01T00:00:01Z',
                       end_exclusive_utc='1970-01-01T00:00:04Z', input_manifest_sha256=parent),
                  dict(index=71, shard='synthetic-071', start_utc='1970-01-01T00:00:04Z',
                       end_exclusive_utc='1970-01-01T00:00:07Z', input_manifest_sha256='b'*64)],
        evaluation=[dict(index=72, start_utc='1970-01-01T00:00:08Z')])


def _account(root, artifacts, contract_path):
    parent = binding(root)['input_manifest_id']
    artifact = artifacts / 'support-070'
    artifact.mkdir()
    opportunities = pd.DataFrame([dict(opportunity_id='quote', decision_ns=2*SECOND,
                                       side='bid', input_manifest_id=parent)])
    outcomes = pd.DataFrame([dict(opportunity_id='quote', horizon_ns=h*SECOND,
        actual_outcome_end_ns=3*SECOND, right_censored=False, filled_quantity=.01,
        markout_bps=float(h), input_manifest_id=parent, training_admitted=False,
        target=TARGET) for h in (1, 5, 30)])
    opportunities.to_parquet(artifact/'opportunities.parquet')
    outcomes.to_parquet(artifact/'raw-outcomes.parquet')
    pd.DataFrame().to_parquet(artifact/'non-quote-actions.parquet')
    (artifact/'launch.json').write_text(json.dumps(dict(contract_sha256=digest(contract_path),
        source_commit='synthetic-source', model_manifest_sha256='synthetic-model',
        input_manifest_sha256=parent, shard=dict(shard_id='synthetic-070',
        start_ts_ms=1000, end_ts_ms_exclusive=4000))))
    (artifact/'journal-receipt.json').write_text(json.dumps(dict(manifest='journal', production='producer')))
    (artifact/'complete.json').write_text(json.dumps(dict(status='support_produced_not_fitted',
        files={name: digest(artifact/name) for name in
               ('opportunities.parquet', 'raw-outcomes.parquet', 'non-quote-actions.parquet')})))
    return artifact


def test_actual_admission_keeps_immutable_raw_and_parent_lineage(bundle, tmp_path, monkeypatch):  # noqa: F811
    from models.replay import l2_journal

    monkeypatch.setattr(l2_journal, 'audit_l2_delivery', lambda *args: None)
    parent = binding(bundle)['input_manifest_id']
    contract = _contract(parent)
    contract_path = tmp_path/'frozen.json'
    contract_path.write_text(json.dumps(contract))
    artifact = _account(bundle, tmp_path, contract_path)
    raw_hash = digest(artifact/'raw-outcomes.parquet')
    panel = admit_account(bundle, artifact, contract_path, 70)
    assert digest(artifact/'raw-outcomes.parquet') == raw_hash
    assert len(panel) == 3 and panel.opportunity_id.eq(parent + ':quote').all()
    assert panel.attrs['training_intervals_ns'] == [[SECOND, 4*SECOND]]
    assert panel.attrs['outcome_support_counts'][f'bid:{SECOND}']['eligible'] == 1
    with pytest.raises(ValueError, match='outside frozen'):
        admit_account(bundle, artifact, contract_path, 69)
    corrupted = pd.read_parquet(artifact/'raw-outcomes.parquet')
    corrupted.loc[0, 'target'] = 'different-target'
    corrupted.to_parquet(artifact/'raw-outcomes.parquet')
    with pytest.raises(ValueError, match='raw outcome artifact identity'):
        admit_account(bundle, artifact, contract_path, 70)


def test_multi_parent_support_and_roundtrip(bundle, tmp_path, monkeypatch):  # noqa: F811
    from models.replay import l2_journal

    monkeypatch.setattr(l2_journal, 'audit_l2_delivery', lambda *args: None)
    parent = binding(bundle)['input_manifest_id']
    contract_path = tmp_path/'frozen.json'
    contract_path.write_text(json.dumps(_contract(parent)))
    first = admit_account(bundle, _account(bundle, tmp_path, contract_path), contract_path, 70)
    second = first.copy(deep=True)
    second['input_manifest_id'] = 'b'*64
    second['opportunity_id'] = 'b'*64 + ':quote'
    second['decision_ns'] = 5*SECOND
    second['actual_outcome_end_ns'] = 6*SECOND
    second.attrs = {**first.attrs, 'input_manifest_id': 'b'*64,
                    'training_intervals_ns': [[4*SECOND, 7*SECOND]],
                    'parent_receipt_sha256': 'second-receipt'}
    merged = combine_training_panels([first, second], contract_path)
    assert len(merged) == 6 and len(merged.attrs['parent_manifests']) == 2
    assert merged.attrs['outcome_support_counts'][f'bid:{SECOND}']['eligible'] == 2
    merged.to_parquet(tmp_path/'merged.parquet')
    restored = pd.read_parquet(tmp_path/'merged.parquet')
    assert restored.attrs == merged.attrs
    assert restored.input_manifest_id.nunique() == 2
    foreign = second.copy(deep=True)
    foreign.loc[foreign.index[0], 'actual_outcome_end_ns'] = 7*SECOND
    with pytest.raises(ValueError, match='foreign or incompatible'):
        combine_training_panels([first, foreign], contract_path)
    with pytest.raises(ValueError, match='foreign or incompatible'):
        combine_training_panels([second, first], contract_path)


def test_support_fails_closed_on_overlap_and_evaluation_collision():
    contract = _contract('a'*64)
    assert account_support(contract)[0] == [[SECOND, 4*SECOND], [4*SECOND, 7*SECOND]]
    contract['training'][1]['start_utc'] = '1970-01-01T00:00:03Z'
    with pytest.raises(ValueError, match='unique ordered'):
        account_support(contract)
    contract['training'][1]['start_utc'] = '1970-01-01T00:00:04Z'
    contract['evaluation'][0]['index'] = 71
    with pytest.raises(ValueError, match='disjoint'):
        account_support(contract)


def test_calibration_uses_only_admitted_filled_training_marks():
    contract = dict(buckets=dict(method='training_only_pooled_filled_markout_quantiles',
                                 edge_quantiles=[0.1, 0.3, 0.5, 0.7, 0.9]))
    panel = pd.DataFrame(dict(fill_label=[1]*30 + [0],
        horizon_ns=[30_000_000_000]*31,
        conditional_markout_bps=[float(i) for i in range(30)] + [float('nan')]))
    panel.attrs['training_admitted'] = True
    fitted = training_only_calibration(panel, contract)
    assert sum(fitted['bucket_counts']) == 30
    assert fitted['observed_conditional_marks'] == 30
    assert fitted['bucket_edges'] == pytest.approx([2.9, 8.7, 14.5, 20.3, 26.1])
    assert fitted['adverse_threshold_bps'] == pytest.approx(2.9)
    panel.attrs['training_admitted'] = False
    with pytest.raises(ValueError, match='admitted'):
        training_only_calibration(panel, contract)


def test_ten_head_preflight_rejects_single_class_before_any_fit():
    panel = pd.DataFrame(dict(side=['bid']*9 + ['ask']*9,
        horizon_ns=([1, 5, 30]*3)*2,
        fill_label=([1]*9)*2,
        conditional_markout_bps=([-2., 2., -1.]*3)*2))
    with pytest.raises(ValueError, match='fill_prob lacks two observed'):
        preflight_head_support(panel, dict(bucket_edges=[0.], adverse_threshold_bps=0.))


def test_bound_ten_head_fit_and_strict_reload_on_synthetic_support(tmp_path):
    from data.tardis_input import CONTRACT

    contract = _contract('a'*64)
    contract.update(buckets=dict(method='training_only_pooled_filled_markout_quantiles',
                                 edge_quantiles=[0.1, 0.3, 0.5, 0.7, 0.9]),
                    fit=dict(scheme='fixed_once_no_inner_selection_no_refit',
                             heads_per_side=5, num_boost_round=1,
                             parameters=dict(min_data_in_leaf=1, num_leaves=3,
                                             num_threads=1, verbosity=-1)),
                    budget=dict(model_fit_calls=10), previous_use='synthetic test only')
    path = tmp_path/'frozen.json'
    path.write_text(json.dumps(contract))
    rows = []
    for parent, base in (('a'*64, 2*SECOND), ('b'*64, 5*SECOND)):
        for side in ('bid', 'ask'):
            for index in range(30):
                for horizon in (1, 5, 30):
                    filled = int(index % 3 != 0)
                    markout = float((index*7+horizon) % 31-15) if filled else float('nan')
                    rows.append(dict(input_manifest_id=parent,
                        opportunity_id=f'{parent}:{side}:{index}', side=side,
                        decision_ns=base, actual_outcome_end_ns=base+1,
                        horizon_ns=horizon*SECOND, right_censored=False,
                        filled_quantity=0.01 if filled else 0., fill_label=filled,
                        conditional_markout_bps=markout, mid=float(index+100)))
    panel = pd.DataFrame(rows)
    panel.attrs = dict(input_contract_id=CONTRACT, observation_contract_id='obs',
        feature_contract_id='feature', feature_columns=['mid'], missing_policy='reject',
        input_manifest_id='synthetic-collection', training_contract_id=digest(path),
        label_contract_id='synthetic-label', training_intervals_ns=account_support(contract)[0],
        training_admitted=True, label_units='maker_signed_bps_per_opportunity_not_net_pnl',
        parent_manifests=[dict(input_manifest_id=parent, parent_receipt_sha256=parent,
                               outcome_support_counts={}) for parent in ('a'*64, 'b'*64)],
        outcome_support_counts={})
    receipt = fit_bound_training(panel, path, tmp_path/'models')
    assert len(receipt['head_support']) == 10
    assert sum(len(side['heads']) for side in receipt['sides'].values()) == 10
    assert len(receipt['model_files']) == 20
    assert (tmp_path/'models/fit-receipt.json').exists()
    with pytest.raises(FileExistsError):
        fit_bound_training(panel, path, tmp_path/'models')
