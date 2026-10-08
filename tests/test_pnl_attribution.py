"""Synthetic accounting and missing-reference cases; no strategy execution."""
import pytest

from models.replay.pnl_attribution import decompose, economic_fills, ledger
from models.replay.public_accounting import funding_window

NS = 10**9


def fill(t, side, qty, price, fee=0., identity=0):
    return dict(fill_ts=t*1000, side=side, fill_qty=qty, quote_px=price,
                fill_fee_usdc=fee, economic_id=identity, order_id=str(identity))


def analyze(fills, marks, terminal=None, funding=(), end=40):
    book=ledger(fills,funding,start=0,end=end*NS,terminal_mid=terminal)
    account=dict(book,account_start_ns=0,account_end_ns=end*NS,
                 valuation_price=terminal,all_in_net_pnl=book['net_pnl'])
    return decompose(fills,account,funding,{int(t*NS):v for t,v in marks.items()})


def test_empty_flat_does_not_need_terminal_price():
    summary,records,lives=analyze([],{},None)
    assert summary['net_pnl']==0 and records==[] and lives==[]


@pytest.mark.parametrize('side,opposite,price,close_price,mid1,mid2',[
    ('BUY','SELL',99.,102.,100.,101.),
    ('SELL','BUY',102.,99.,101.,100.)])
def test_round_trip(side,opposite,price,close_price,mid1,mid2):
    s,_,l=analyze([fill(1,side,1,price,.1),fill(3,opposite,1,close_price,.2,1)],{1:mid1,3:mid2})
    assert s['E']==2 and s['H']==1
    assert s['net_pnl']==pytest.approx(2.7)
    assert len(l)==1 and l[0]['closed']


def test_negative_unrealized_holding_and_funding_are_added():
    funding=[dict(settlement_ns=2*NS,mark_price=100.,rate=.01)]
    s,_,l=analyze([fill(1,'BUY',1,100)],{1:101},90.,funding)
    assert s['terminal_unrealized_pnl']==-10
    assert s['E']==1 and s['H']==-11 and s['funding_cashflow']==-1
    assert s['net_pnl']==-11 and not l[0]['closed']


def test_partial_add_reduce_flip_conserves_quantities_fees_and_identity():
    fills=[fill(1,'BUY',2,100,.2),fill(2,'BUY',1,101,.1,1),
           fill(3,'SELL',1,102,.1,2),fill(4,'SELL',3,103,.3,3)]
    s,r,l=analyze(fills,{1:100,2:101,3:102,4:103},102.)
    assert len(r)==4 and len(l)==2
    assert r[-1]['reducing_qty']==2 and r[-1]['opening_qty']==1
    assert sum(x['fees'] for x in l)==pytest.approx(.7)
    assert s['terminal_inventory']==-1
    assert s['net_pnl']==pytest.approx(7.3)
    assert sum(x['abs_inventory_btc_seconds'] for x in l)==43


def test_funding_precedes_equal_time_fill_and_window_edges():
    funding=dict(source_identity='synthetic',coverage_start_ns=0,coverage_end_ns=4*NS,
        expected_settlements_ns=[0,NS,2*NS,4*NS],events=[dict(settlement_ns=t*NS,mark_price=100.,rate=.01) for t in (0,1,2,4)])
    selected=funding_window(funding,start_ns=0,end_ns=4*NS)['events']
    s,_,_=analyze([fill(1,'BUY',1,100),fill(2,'SELL',1,100,identity=1)],{1:100,2:100},funding=selected,end=4)
    assert s['funding_cashflow']==-1
    with pytest.raises(ValueError,match='outside account'):
        ledger([fill(4,'BUY',1,100)],[],start=0,end=4*NS,terminal_mid=100)


def test_delayed_notification_not_used_as_economic_clock():
    a=dict(fill(1,'BUY',1,100),match_sequence=0)
    b=dict(fill(2,'SELL',1,102,identity=1),match_sequence=1)
    result=dict(private_fill_visibility_enabled=True,economic_fill_contract='match_facts_and_local_notifications.v1',
        _economic_fill_trace=[a,b],_fill_trace=[dict(a,economic_match_sequence=0,fill_clock_context={'processed_ts_ms':3000})])
    facts=economic_fills(result)
    s,_,l=analyze(facts,{1:100,2:102})
    assert s['fills']==2 and s['net_pnl']==2 and l[0]['duration_seconds']==1
    result['_economic_fill_trace']=[b,a]
    with pytest.raises(ValueError):economic_fills(result)


def test_missing_mid_remains_unallocated_not_zero_price_or_holding_loss():
    s,r,l=analyze([fill(1,'BUY',1,99),fill(3,'SELL',1,102,identity=1)],{1:100,3:None})
    assert s['net_pnl']==3 and s['E']==1 and s['H']==0 and s['unallocated']==2
    assert r[1]['reference_mid'] is None and s['missing_mid_fills']==1
    assert l[0]['missing_boundaries']>0


def test_negative_fee_is_rebate():
    s,_,_=analyze([fill(1,'BUY',1,100,-.2),fill(2,'SELL',1,100,-.1,1)],{1:100,2:100})
    assert s['fees']==pytest.approx(-.3) and s['net_pnl']==pytest.approx(.3)


def test_sparse_holding_equals_dense_market_changes():
    s,_,_=analyze([fill(1,'BUY',2,100),fill(4,'SELL',2,104,identity=1)],{1:101,4:103})
    dense=2*((102-101)+(99-102)+(103-99))
    assert s['H']==dense and s['E']+dense==8


def test_markouts_are_diagnostics_only_and_do_not_cross_account_end():
    fills=[fill(1,'BUY',1,100),fill(3,'SELL',1,102,identity=1)]
    a,r,_=analyze(fills,{1:101,2:110,3:101,4:80,6:130},end=10)
    b,_,_=analyze(fills,{1:101,2:999,3:101,4:1,6:500},end=10)
    assert a['net_pnl']==b['net_pnl']==2
    assert r[0]['markout_1s_price']==r[0]['edge_price']+r[0]['change_1s_price']
    assert r[0]['reason_30s']=='outside_account'


def test_delivered_mid_excludes_equal_time_and_never_skips_invalid_latest(monkeypatch):
    import pyarrow as pa
    from models.replay import public_accounting as accounting

    class SyntheticBundle:
        def __init__(self, root):
            self.manifest = {'plan': {'start_ns': 0, 'end_ns': 10*NS}}
            self.input_manifest_id = 'synthetic'

        def batches(self, channel):
            assert channel == 'depth'
            yield pa.RecordBatch.from_pylist([
                dict(ready_ns=t*NS, source_asof_ns=t*NS, valid=valid,
                     stale=False, bid_px=[99.], ask_px=[101.])
                for t, valid in ((1, True), (2, False), (3, True))])

    monkeypatch.setattr(accounting, 'ConsumerBundle', SyntheticBundle)
    marks = accounting.terminal_valuations('synthetic', [NS, 2*NS, 3*NS, 4*NS, 5*NS], max_mark_age_ns=NS)
    assert marks[NS].price is None
    assert marks[2*NS].price == 100
    assert marks[3*NS].price is None
    assert marks[4*NS].price == 100
    assert marks[5*NS].price is None
