"""Production outcome adapter tests; fixtures are synthetic, never study data."""
import pandas as pd
import pytest

from models.replay.l2_journal import ReplayL2Journal, ReplayL2Production
from research.families.f05_fill_quality_quote_ev.logged_outcomes import produce_logged_outcomes
from tests.test_research_public_inputs import bundle  # noqa: F401


@pytest.mark.parametrize('mode', ['fill', 'partial', 'unknown', 'nofill', 'cutoff', 'wait', 'reject'])
@pytest.mark.parametrize('side', ['BUY', 'SELL'])
def test_observed_lifecycle_units_and_unknowns_survive_serialization(tmp_path, mode, side):
    journal = ReplayL2Journal(tmp_path/'journal', identity={})
    producer = ReplayL2Production()
    base = 1_754_006_400_001
    def emit(kind, ts, row):
        token = producer.observe(kind, ts, side)
        journal.emit(kind, ts, side=side, payload=row, production_event=token)
        return token['sequence']
    parent = emit('decision', base, dict(action='place' if mode != 'wait' else 'WAIT'))
    if mode != 'wait':
        emit('order_created', base+1, dict(order_id=10, creation_opportunity_event_id=parent,
                                          creation_source='quote_decision'))
        emit('order_submit', base+2, dict(order_id=10))
        if mode in ('fill', 'partial', 'unknown', 'cutoff'):
            for index, quantity in enumerate([.001, .002] if mode == 'partial' else [.003]):
                fields = dict(order_id=10, fill_qty=quantity, quote_px=100.,
                              markout_unit='USDC/BTC')
                for h in (1, 5, 30):
                    prefix = f'markout_{h}s'
                    fields.update({prefix: None if mode == 'unknown' else (1.+index),
                        prefix+'_requested_end_ms': base+10+h*1000,
                        prefix+'_actual_end_ms': None if mode == 'unknown' else base+11+h*1000,
                        prefix+'_method': 'first_replay_price_at_or_after_target',
                        prefix+'_status': 'censored' if mode == 'unknown' else 'delayed'})
                emit('fill', base+10+index, fields)
        if mode == 'cutoff':
            emit('order_observation_cutoff', base+100, dict(order_id=10))
        elif mode == 'reject':
            emit('order_reject', base+20, dict(order_id=10))
        else:
            emit('order_outcome', base+100, dict(order_id=10, outcome='cancel', remaining=0.))
    journal.close(production=producer.receipt())
    opportunities, outcomes, non_quote = produce_logged_outcomes(journal.writer.manifest_path,
        production=producer.receipt(), input_manifest_id='synthetic-bound-input')
    assert len(opportunities) == 1 and len(outcomes) == 3 and non_quote.empty
    outcomes.to_parquet(tmp_path/'outcome.parquet')
    readback = pd.read_parquet(tmp_path/'outcome.parquet')
    pd.testing.assert_series_equal(readback.actual_outcome_end_ns, outcomes.actual_outcome_end_ns)
    assert not readback.training_admitted.any()
    if mode in ('wait', 'cutoff', 'reject'):
        assert readback.right_censored.all() and readback.filled_quantity.isna().all()
        assert readback.markout_bps.isna().all()
    elif mode == 'unknown':
        assert not readback.right_censored.any()
        assert (readback.filled_quantity == .003).all() and readback.markout_bps.isna().all()
    elif mode == 'nofill':
        assert (readback.filled_quantity == 0).all() and readback.markout_bps.isna().all()
    else:
        expected = 5/3 if mode == 'partial' else 1.
        assert readback.markout_price_delta_usdc_per_btc.iloc[0] == pytest.approx(expected)
        assert readback.markout_bps.iloc[0] == pytest.approx(expected*100)
        assert readback.markout_amount_usdc.iloc[0] == pytest.approx(expected*.003)
        assert readback.actual_outcome_end_ns.iloc[0] == (base+1011)*1_000_000


def test_dangling_creation_link_rejected(tmp_path):
    journal = ReplayL2Journal(tmp_path/'bad', identity={})
    producer = ReplayL2Production()
    token = producer.observe('order_created', 10, 'BUY')
    journal.emit('order_created', 10, side='BUY', production_event=token,
                 payload=dict(order_id=1, creation_opportunity_event_id=42))
    journal.close(production=producer.receipt())
    with pytest.raises(ValueError, match='missing creation opportunity'):
        produce_logged_outcomes(journal.writer.manifest_path, production=producer.receipt(), input_manifest_id='test')


def test_real_panel_entry_separates_engineering_support_from_training(bundle, tmp_path):  # noqa: F811
    from data.feature_cursor import FeatureCursor
    from research.families.f05_fill_quality_quote_ev.public_input import build_opportunity_panel, train_opportunity_models
    identity = FeatureCursor(bundle).input_manifest_id
    opportunities = pd.DataFrame([dict(opportunity_id=1, decision_ns=2_000_000_000,
                                      side='bid', input_manifest_id=identity)])
    outcomes = pd.DataFrame([dict(opportunity_id=1, horizon_ns=1_000_000_000,
        actual_outcome_end_ns=3_000_000_001, right_censored=False,
        filled_quantity=0., markout_bps=float('nan'), input_manifest_id=identity,
        training_admitted=False)])
    kwargs = dict(feature_columns=['mid'], missing_policy='reject', max_age_ns=1_000_000_000,
                  training_boundary_ns=4_000_000_000,
                  training_intervals_ns=[[1_000_000_000, 4_000_000_000]])
    with pytest.raises(ValueError, match='engineering outcomes'):
        build_opportunity_panel(bundle, opportunities, outcomes, **kwargs)
    panel = build_opportunity_panel(bundle, opportunities, outcomes,
                                    support_role='engineering_observation', **kwargs)
    assert panel.fill_label.tolist() == [0]
    assert panel.opportunity_markout_bps.tolist() == [0.]
    assert panel.conditional_markout_bps.isna().all()
    panel.to_parquet(tmp_path/'panel.parquet')
    panel = pd.read_parquet(tmp_path/'panel.parquet')
    assert panel.attrs['training_admitted'] is False
    with pytest.raises(ValueError, match='not a training contract'):
        train_opportunity_models(panel, tmp_path/'model', input_identity={}, feature_columns=['mid'],
            side='bid', missing_policy='reject', training_boundary_ns=4_000_000_000,
            bucket_edges=[0.], bucket_values=[-1., 1.], adverse_threshold_bps=-1., parameters={},
            num_boost_round=1)
