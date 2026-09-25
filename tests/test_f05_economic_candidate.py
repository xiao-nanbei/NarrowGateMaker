"""Synthetic executor wiring; no private strategy result or live authority."""

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from data.feature_cursor import FeatureCursor
from data.tardis_input import CONTRACT
from research.families.f05_fill_quality_quote_ev import economic_candidate as candidate
from tests.test_research_public_inputs import SECOND, binding, bundle  # noqa: F401


def _policy(root, tmp_path, monkeypatch):
    cursor = FeatureCursor(root)
    identity = dict(input_contract_id=CONTRACT,
        observation_contract_id=cursor.bundle.manifest['observation_contract_id'],
        feature_contract_id=cursor.bundle.manifest['feature_contract_id'],
        source_manifest_sha256='synthetic-training-collection',
        training_contract_id='training', label_contract_id='label')
    models = tmp_path/'models'
    models.mkdir()
    (models/'fit-receipt.json').write_text(json.dumps(dict(contract_sha256='frozen-training',
        input_identity=identity, model_files={})))
    class FakeModel:
        def __init__(self, side):
            self.side = side
            self.input_identity = identity

        def predict_frame(self, frame, *, decision_ns):
            assert frame.cutoff_ns <= decision_ns
            assert (frame.max_dependency_ready_ns is None
                    or frame.max_dependency_ready_ns <= decision_ns)
            return SimpleNamespace(fill_and_extreme_adverse_probability_30000ms=.8 if self.side == 'bid' else .2)

    monkeypatch.setattr(candidate, 'QuoteEVModel', SimpleNamespace(
        load=lambda path, side, input_identity: FakeModel(side)))
    contract = tmp_path/'candidate.json'
    contract.write_text(json.dumps(dict(schema='f05_existing_quote_widen_candidate.v1',
        action='existing_public_quote_widen',
        risk_score='p_fill_times_p_extreme_adverse_given_fill_30000ms',
        threshold_source='training_predicted_risk_q90', widen_mult=1.25,
        input_manifest_id=binding(root)['input_manifest_id'],
        side_thresholds={'BUY': .5, 'SELL': .5}, max_age_ns=SECOND,
        trace_limit=20, fit_receipt_sha256=candidate._digest(models/'fit-receipt.json'),
        training_contract_sha256='frozen-training')))
    return candidate.F05RiskWidenPolicy(root, models, contract), contract


def test_candidate_is_bound_and_falls_back_on_missing_context(bundle, tmp_path, monkeypatch):  # noqa: F811
    policy, contract = _policy(bundle, tmp_path, monkeypatch)
    manifest = binding(bundle)['input_manifest_id']
    policy.start(manifest)
    assert policy.decide(0)['BUY']['action'] == 'default'
    assert policy.counts['missing_sides'] == 2
    actions = policy.decide(2*SECOND)
    assert actions['BUY'] == {'action': 'widen', 'spread_mult': 1.25}
    assert actions['SELL']['action'] == 'default'
    assert policy.decide(2*SECOND) is actions
    frame_predictions = policy.counts['model_frame_predictions']
    policy.decide(2*SECOND + 1)
    assert policy.counts['model_frame_predictions'] == frame_predictions
    policy.resolved(2*SECOND + 1, 'BUY', action='place', price=100., quantity=.001, route_due=True)
    assert policy.report()['counts']['widen_requested'] == 2
    assert policy.report()['decisions'][0]['feature_cutoff_ns'] <= 2*SECOND
    with pytest.raises(ValueError, match='regressed'):
        policy.decide(0)
    wrong = json.loads(contract.read_text())
    wrong['fit_receipt_sha256'] = 'bad'
    contract.write_text(json.dumps(wrong))
    with pytest.raises(ValueError, match='receipt identity'):
        candidate.F05RiskWidenPolicy(bundle, tmp_path/'models', contract)


def test_f05_model_action_reaches_original_executor(bundle, tmp_path, monkeypatch):  # noqa: F811
    from models.backtest_tick import simulate_public_inputs

    policy, _ = _policy(bundle, tmp_path, monkeypatch)
    params = dict(eta_inventory=.01, a_spread=.01, risk_per_order=.01, inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2., order_size=.001, max_inventory=.01,
        requote_interval=.2, rq_min=.2, rq_max=.2, requote_clock='fixed', maker_fee=0.,
        taker_fee=0., tick_size=.1, lot_size=.001, queue_base=0., queue_decay=0.,
        maker_fill_prob=1., use_bar_pricing=True, replay_event_clock='merged',
        replay_clock_interval_ms=100, exchange_book_queue_mode='diagnostic',
        public_fill_volume_policy='all_public_volume_eligible', max_exec_book_age_s=10.,
        collect_curves=False, position_timeout=0., markout_ema_span_fills=0,
        account_start_ns=1_200_000_000, trace_fills_max=1000, trace_quotes_max=1000,
        trace_decisions_max=1000, ml_enabled=False)
    baseline = simulate_public_inputs(bundle, params)
    altered = simulate_public_inputs(bundle, params, public_strategy=policy)
    assert altered['public_strategy']['counts']['widen_requested'] > 0
    assert [x['price'] for x in altered['_quote_trace']] != [x['price'] for x in baseline['_quote_trace']]
    assert altered['all_in_net_pnl'] is None  # full fees, funding and MTM remain a separate settlement


def test_risk_cutoff_uses_exact_training_parents_and_actual_end(monkeypatch):
    parents = [{'input_manifest_id': 'a', 'parent_receipt_sha256': 'receipt-a'},
               {'input_manifest_id': 'b', 'parent_receipt_sha256': 'receipt-b'}]
    receipt = {'contract_sha256': 'contract', 'parents': parents,
               'input_identity': {'source_manifest_sha256': 'collection'}}
    rows = [dict(input_manifest_id=parent, decision_ns=start+1,
                 actual_outcome_end_ns=start+2, side=side,
                 horizon_ns=30_000_000_000, mid=float(index+1))
            for parent, start in (('a', 0), ('b', 100))
            for side in ('bid', 'ask') for index in range(120)]
    panel = pd.DataFrame(rows)
    panel.attrs = {'training_admitted': True, 'support_role': 'training',
        'training_contract_id': 'contract', 'parent_manifests': parents,
        'input_manifest_id': 'collection', 'training_intervals_ns': [[0, 100], [100, 200]]}

    class FakeModel:
        fill_prob_features = ['mid']
        extreme_adverse_features = ['mid']
        fill_prob_model = SimpleNamespace(predict=lambda matrix: matrix[:, 0]/120)
        extreme_adverse_model = SimpleNamespace(predict=lambda matrix: np.full(len(matrix), .5))

    monkeypatch.setattr(candidate, 'QuoteEVModel', SimpleNamespace(load=lambda *a, **k: FakeModel()))
    cutoffs, counts = candidate.freeze_training_risk_thresholds(panel, 'unused', receipt)
    assert cutoffs == pytest.approx({'BUY': .9*120/120*.5, 'SELL': .9*120/120*.5}, abs=.005)
    assert counts['BUY']['training_opportunities'] == 240
    leaked = panel.copy(deep=True)
    leaked.loc[0, 'actual_outcome_end_ns'] = 201
    with pytest.raises(ValueError, match='out-of-training'):
        candidate.freeze_training_risk_thresholds(leaked, 'unused', receipt)
    leaked = panel.copy(deep=True)
    leaked.attrs = {**panel.attrs, 'parent_manifests': parents[:1]}
    with pytest.raises(ValueError, match='training-only'):
        candidate.freeze_training_risk_thresholds(leaked, 'unused', receipt)
