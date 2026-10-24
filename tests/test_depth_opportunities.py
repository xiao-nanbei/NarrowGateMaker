import json
import sqlite3

from models.replay.l2_journal import ReplayL2Journal, ReplayL2Production
from research.families.f08_side_taker_lifecycle.depth_panel import ingest_journal


def test_full_decision_denominator_exact_links_and_no_future_covariates(tmp_path):
    sink = ReplayL2Journal(tmp_path / 'journal', identity={'test': True})
    producer = ReplayL2Production()
    rows = [
        ('optional_update_observation', dict(read_ns=100000000, side='BUY', old_price=99, target_price=98)),
        ('decision', dict(action='KEEP', prediction_generation_index=3, inventory=.001,
                          decision_queue_ahead_btc=999, activation_book_evidence={'future': True})),
        ('decision_route_excluded', dict(action='none', final_price=98, prediction_index=3)),
        ('order_created', dict(order_id=0, creation_opportunity_event_id=2)),
        ('decision_action_revision', dict(opportunity_event_id=2, action='place')),
        ('order_outcome', dict(order_id=0, outcome='nonfill', eventual_pnl=999)),
    ]
    for index, (kind, payload) in enumerate(rows):
        # The excluded route is a different exact decision clock.
        now = 101 if kind == 'decision_route_excluded' else 100 if index < 3 else 102+index
        sink.emit(kind, now, side='BUY', payload=payload,
                  production_event=producer.observe(kind, now, 'BUY'))
    receipt = sink.close(production=producer.receipt())
    result = ingest_journal(receipt['manifest'], tmp_path/'events.sqlite', receipt['production'])
    assert result['linkage']['decision_opportunities'] == 2
    assert result['linkage']['unique_exact_clock_side_and_producer_order'] == 1
    with sqlite3.connect(tmp_path/'events.sqlite') as db:
        rows = [json.loads(x[0]) for x in db.execute('SELECT payload FROM decision_opportunities ORDER BY sequence')]
        assert rows[0]['action'] == 'KEEP'
        assert rows[1]['kind'] == 'decision_route_excluded'
        assert rows[1]['I_current']['prediction_generation_index'] == 3
        assert 'decision_queue_ahead_btc' not in rows[0]['I_current']
        assert 'eventual_pnl' not in str(rows)
        assert 'activation_book_evidence' not in str(rows)
        assert db.execute("SELECT count(*) FROM event_links WHERE kind='order_terminal'").fetchone()[0] == 1


def test_ambiguous_same_clock_is_not_nearest_matched(tmp_path):
    sink = ReplayL2Journal(tmp_path/'journal', identity={'test': True})
    producer = ReplayL2Production()
    for kind in ('optional_update_observation', 'optional_update_observation', 'decision'):
        sink.emit(kind, 100, side='BUY', payload=dict(read_ns=100000000, side='BUY'),
                  production_event=producer.observe(kind, 100, 'BUY'))
    receipt = sink.close(production=producer.receipt())
    result = ingest_journal(receipt['manifest'], tmp_path/'events.sqlite', receipt['production'])
    assert result['linkage']['missing_or_ambiguous'] == 1
