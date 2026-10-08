"""Read-only attribution of completed independent accounts, without strategy replay.

CLI: python -m models.replay.pnl_attribution --comparison FILE --output DIRECTORY
The private comparison supplies rows with account_id, arm and result path. Each
result directory supplies task.json, accounting.json and replay.json. No market
or model inference is performed. A delivered mid is a valuation convention, not
the exchange's unobserved instantaneous fair price.
"""
import argparse
import csv
import heapq
import json
import math
from pathlib import Path
import resource
import sys
import time

from models.replay.continuous_accounting import funding_cashflow_usdc
from models.replay.public_accounting import (
    TerminalValuation, funding_window, settle_public_replay, terminal_valuations,
)

HORIZONS = (1, 5, 10, 30)
EPS = 1e-12


def close(actual, expected, name, tol=1e-8):
    if not math.isclose(actual, expected, rel_tol=0, abs_tol=tol):
        raise ValueError(f'{name} does not reconcile: {actual!r} != {expected!r}')


def economic_fills(result):
    """Select original economic identities, never sort local notifications."""
    delayed = result.get('private_fill_visibility_enabled', False)
    if delayed and result.get('economic_fill_contract') != 'match_facts_and_local_notifications.v1':
        raise ValueError('missing economic matching contract')
    key = '_economic_fill_trace' if delayed else '_fill_trace'
    seq = 'match_sequence' if delayed else 'fill_sequence'
    fills = result[key]
    if [f[seq] for f in fills] != list(range(len(fills))):
        raise ValueError('noncontiguous economic identities')
    if any(a['fill_ts'] > b['fill_ts'] for a, b in zip(fills, fills[1:])):
        raise ValueError('economic clock regressed')
    contexts = {}
    if delayed:
        for f in result['_fill_trace']:
            identity = f['economic_match_sequence']
            if identity in contexts:
                raise ValueError('duplicate local notification')
            contexts[identity] = f.get('fill_clock_context')
    return [{**f, 'economic_id': f[seq],
             'clock_context': contexts.get(f[seq], f.get('fill_clock_context'))} for f in fills]


def ledger(fills, funding, *, start, end, terminal_mid):
    """Reproduce the original weighted-entry ledger with its funding ordering."""
    if start >= end:
        raise ValueError('invalid account interval')
    for i, f in enumerate(fills):
        t = f['fill_ts'] * 10**6
        if not start <= t < end or (i and f['fill_ts'] < fills[i-1]['fill_ts']):
            raise ValueError('economic fill outside account or clock regression')
        if (f['side'] not in ('BUY', 'SELL') or
            any(not math.isfinite(f[k]) for k in ('fill_qty', 'quote_px', 'fill_fee_usdc')) or
            f['fill_qty'] <= 0 or f['quote_px'] <= 0):
            raise ValueError('invalid economic fill')
    clocks = [f['settlement_ns'] for f in funding]
    if clocks != sorted(set(clocks)) or any(not start < t <= end for t in clocks):
        raise ValueError('invalid funding window or sequence')
    q = cash = fee_sum = payments = entry = realized = 0.
    payment_rows = []
    events = heapq.merge(((f['fill_ts']*10**6, 1, f) for f in fills),
                        ((f['settlement_ns'], 0, f) for f in funding), key=lambda x: x[:2])
    for t, kind, f in events:
        if kind == 0:
            value = funding_cashflow_usdc(q, f['mark_price'], f['rate'])
            payments += value
            payment_rows.append(dict(time_ns=t, cashflow=value, inventory=q))
            continue
        dq = f['fill_qty'] * (1 if f['side'] == 'BUY' else -1)
        price, fee = f['quote_px'], f['fill_fee_usdc']
        cash -= dq*price + fee
        fee_sum += fee
        new_q = q+dq
        if q == 0 or q*dq > 0:
            entry = (abs(q)*entry+abs(dq)*price)/abs(new_q)
        else:
            realized += min(abs(q), abs(dq))*(price-entry)*(1 if q > 0 else -1)
            if q*new_q < 0:
                entry = price
            elif abs(new_q) < EPS:
                new_q, entry = 0., 0.
        q = new_q
    if q and terminal_mid is None:
        raise ValueError('nonflat account needs original terminal valuation')
    before = cash + (q*terminal_mid if q else 0.)
    return dict(terminal_inventory=q, cash_fill=cash, fees=fee_sum,
        funding_cashflow=payments, realized_trading_pnl=realized,
        terminal_unrealized_pnl=q*(terminal_mid-entry) if q else 0.,
        net_pnl=before+payments, funding_events=payment_rows,
        fills=len(fills), btc_volume=math.fsum(f['fill_qty'] for f in fills),
        usdc_volume=math.fsum(f['fill_qty']*f['quote_px'] for f in fills))


def decompose(fills, accounting, funding, marks):
    """Exact sparse-boundary E/H decomposition, plus independent lifecycles.

    Missing marks leave an explicitly unallocated component, never a zero mid.
    Lifecycle net comes from its own fill cash and terminal inventory, not from
    forcing E/H to match. Cross-zero fills split quantity and fees, not identity.
    """
    start, end = accounting['account_start_ns'], accounting['account_end_ns']
    terminal = accounting['valuation_price']
    book = ledger(fills, funding, start=start, end=end, terminal_mid=terminal)
    close(book['net_pnl'], accounting['all_in_net_pnl'], 'net PnL')
    for k in ('fees', 'funding_cashflow', 'terminal_inventory', 'realized_trading_pnl', 'terminal_unrealized_pnl'):
        close(book[k], accounting[k], k, 1e-10 if k == 'terminal_inventory' else 1e-8)
    close(book['realized_trading_pnl']+book['terminal_unrealized_pnl']-book['fees']+book['funding_cashflow'], book['net_pnl'], 'realized identity')
    q = 0.; last_time = start; prev_mid = None
    current = None; lives = []; records = []

    def create(t, direction):
        life = dict(lifecycle_id=len(lives), start_ns=t, end_ns=None, direction=direction,
            closed=False, opening_qty=0., increasing_qty=0., reducing_qty=0.,
            max_abs_inventory=0., abs_inventory_btc_seconds=0., E=0., H=0.,
            fees=0., funding_cashflow=0., gross_fill_cash=0., missing_boundaries=0,
            first_economic_id=None, last_economic_id=None)
        lives.append(life)
        return life

    def finish(life, t, position, mark):
        life.update(end_ns=t, closed=position == 0., duration_seconds=(t-life['start_ns'])/1e9)
        gross = life['gross_fill_cash'] + (position*mark if position else 0.)
        life['unallocated'] = gross-life['E']-life['H'] if life['missing_boundaries'] else 0.
        life['net_pnl'] = gross-life['fees']+life['funding_cashflow']
        close(life['E']+life['H']+life['unallocated']-life['fees']+life['funding_cashflow'],life['net_pnl'],'lifecycle identity')

    events = heapq.merge(((f['fill_ts']*10**6, 1, f) for f in fills),
        ((f['time_ns'], 0, f) for f in book['funding_events']), key=lambda x:x[:2])
    for t, kind, f in events:
        if current is not None:
            current['abs_inventory_btc_seconds'] += abs(q)*(t-last_time)/1e9
        last_time = t
        if kind == 0:
            if current is not None:
                current['funding_cashflow'] += f['cashflow']
            elif f['cashflow'] != 0:
                raise ValueError('funding while flat')
            continue
        mid = marks.get(t)
        if current is not None:
            if mid is not None and prev_mid is not None:
                current['H'] += q*(mid-prev_mid)
            else:
                current['missing_boundaries'] += 1
        sign = 1 if f['side']=='BUY' else -1
        left = f['fill_qty']; roles = dict(opening=0., increasing=0., reducing=0.)
        while left > EPS:
            if abs(q) < EPS:
                q = 0.
                current = create(t, 'LONG' if sign > 0 else 'SHORT')
                role, qty = 'opening', left
            elif q*sign > 0:
                role, qty = 'increasing', left
            else:
                role, qty = 'reducing', min(abs(q), left)
            roles[role] += qty
            current[role+'_qty'] += qty
            current['gross_fill_cash'] -= sign*qty*f['quote_px']
            current['fees'] += f['fill_fee_usdc']*qty/f['fill_qty']
            if mid is None:
                current['missing_boundaries'] += 1
            else:
                current['E'] += sign*qty*(mid-f['quote_px'])
            if current['first_economic_id'] is None:
                current['first_economic_id'] = f['economic_id']
            current['last_economic_id'] = f['economic_id']
            q += sign*qty; left -= qty
            current['max_abs_inventory'] = max(current['max_abs_inventory'], abs(q))
            if abs(q) < EPS:
                q = 0.
                finish(current,t,0.,None);current=None
        edge = None if mid is None else sign*(mid-f['quote_px'])
        record = dict(economic_id=f['economic_id'], order_id=str(f.get('order_id','')),
            match_ns=t, side=f['side'], quantity=f['fill_qty'], price=f['quote_px'], fee=f['fill_fee_usdc'],
            reference_mid=mid, missing_reason='missing_invalid_or_stale_delivered_mid' if mid is None else None,
            clock_context=json.dumps(f.get('clock_context'),sort_keys=True),
            **{k+'_qty':v for k,v in roles.items()}, edge_price=edge,
            edge_bps=None if edge is None else 10000*edge/mid,
            edge_usdc=None if edge is None else edge*f['fill_qty'])
        for h in HORIZONS:
            future = marks.get(t+h*10**9) if t+h*10**9 <= end else None
            reason = 'outside_account' if t+h*10**9 > end else 'missing_future_mid' if future is None else 'missing_fill_mid' if mid is None else None
            change = None if reason else sign*(future-mid)
            full = None if reason else sign*(future-f['quote_px'])
            record.update({f'mid_{h}s':future,f'reason_{h}s':reason,
                f'change_{h}s_price':change,f'markout_{h}s_price':full,
                f'change_{h}s_bps':None if reason else 10000*change/mid,
                f'markout_{h}s_bps':None if reason else 10000*full/mid,
                f'change_{h}s_usdc':None if reason else change*f['fill_qty'],
                f'markout_{h}s_usdc':None if reason else full*f['fill_qty']})
        records.append(record);prev_mid=mid
    if current is not None:
        current['abs_inventory_btc_seconds'] += abs(q)*(end-last_time)/1e9
        if terminal is not None and prev_mid is not None:
            current['H'] += q*(terminal-prev_mid)
        else:
            current['missing_boundaries'] += 1
        finish(current,end,q,terminal)
    components = {k:math.fsum(l[k] for l in lives) for k in ('E','H','unallocated','net_pnl','fees','funding_cashflow')}
    close(components['net_pnl'],book['net_pnl'],'all lifecycles')
    missing = [r for r in records if r['reference_mid'] is None]
    summary = {k:v for k,v in book.items() if k != 'funding_events'}
    summary.update(components, original_pnl=accounting['all_in_net_pnl'],
        net_pnl=book['net_pnl'],
        lifecycle_residual=components['net_pnl']-book['net_pnl'],
        accounting_residual=book['net_pnl']-accounting['all_in_net_pnl'],
        decomposition_residual=components['E']+components['H']+components['unallocated']-book['fees']+book['funding_cashflow']-book['net_pnl'],
        missing_mid_fills=len(missing),missing_mid_qty=math.fsum(r['quantity'] for r in missing),
        missing_mid_notional=math.fsum(r['quantity']*r['price'] for r in missing),
        mid_coverage=1-len(missing)/len(fills) if fills else 1.)
    return summary,records,lives


def grouped_diagnostics(records, lives):
    groups=[]
    for side in ('ALL','BUY','SELL'):
        for role in ('all','opening','increasing','reducing'):
            selected=[(r,r['quantity'] if role=='all' else r[role+'_qty']) for r in records
                      if (side=='ALL' or r['side']==side) and (role=='all' or r[role+'_qty'] > 0)]
            for h in HORIZONS:
                valid=[(r,q) for r,q in selected if r[f'reason_{h}s'] is None]
                qty=math.fsum(q for _,q in valid)
                groups.append(dict(side=side,role=role,horizon_s=h,fills=len(selected),valid_fills=len(valid),
                    quantity=math.fsum(q for _,q in selected),valid_quantity=qty,
                    edge_usdc=math.fsum(r['edge_price']*q for r,q in valid),
                    change_usdc=math.fsum(r[f'change_{h}s_price']*q for r,q in valid),
                    markout_usdc=math.fsum(r[f'markout_{h}s_price']*q for r,q in valid),
                    weighted_markout_bps=math.fsum(r[f'markout_{h}s_bps']*q for r,q in valid)/qty if qty else None))
    buckets=[]
    for lo,hi in ((0,60),(60,300),(300,1800),(1800,7200),(7200,float('inf'))):
        selected=[l for l in lives if lo<=l['duration_seconds']<hi]
        buckets.append(dict(seconds_from=lo,seconds_to=None if math.isinf(hi) else hi,count=len(selected),
            net_pnl=math.fsum(l['net_pnl'] for l in selected),
            negative_pnl=math.fsum(min(0,l['net_pnl']) for l in selected),
            E=math.fsum(l['E'] for l in selected),H=math.fsum(l['H'] for l in selected)))
    return dict(markouts=groups,lifecycle_buckets=buckets,
                worst_lifecycles=sorted(lives,key=lambda l:l['net_pnl'])[:5],
                lifecycle_count=len(lives),open_lifecycles=sum(not l['closed'] for l in lives))


def load_result(path):
    def read(name): return json.loads((path/name).read_text())
    task, accounting, result = read('task.json'),read('accounting.json'),read('replay.json')
    if not accounting['economic_complete'] or accounting['accounting_contract'] != 'public_independent_mtm.v1':
        raise ValueError('requires complete independent MTM accounting')
    funding=funding_window(json.loads(Path(task['funding']).read_text()),
        start_ns=task['account_start_ns'],end_ns=task['account_end_ns'])
    contract=result['public_input_contract']
    valuation=TerminalValuation(contract['input_manifest_id'],accounting['account_end_ns'],
        task['max_mark_age_ns'],accounting['valuation_price'],accounting['valuation_age_ns'])
    recomputed=settle_public_replay(task['bundle'],result,initial_capital=task['initial_capital'],
        max_mark_age_ns=task['max_mark_age_ns'],funding=funding,terminal_valuation=valuation)
    for k in ('all_in_net_pnl','fees','funding_cashflow','terminal_inventory','realized_trading_pnl','terminal_unrealized_pnl'):
        close(recomputed[k],accounting[k],k)
    return task,accounting,economic_fills(result),funding['events']


def write_csv(path,rows):
    fields=list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields)
        writer.writeheader();writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq
    wall,cpu=time.time(),time.process_time()
    args.output.mkdir(parents=True,exist_ok=False)
    specifications=json.loads(args.comparison.read_text())['rows']
    if len({(r['account_id'],r['arm']) for r in specifications})!=len(specifications):
        raise ValueError('duplicate account/arm')
    account_rows=[]; lifecycle_rows=[]; details={};writer=None
    try:
        for account in sorted({r['account_id'] for r in specifications}):
            selected=[r for r in specifications if r['account_id']==account]
            loaded=[(r,load_result(Path(r['path']))) for r in selected]
            task=loaded[0][1][0]; endpoints=set()
            for spec,(t,a,fills,funding) in loaded:
                if t != task:
                    raise ValueError('paired tasks have different input or account binding')
                # First reconcile without reading any middle-of-account book.
                l=ledger(fills,funding,start=a['account_start_ns'],end=a['account_end_ns'],terminal_mid=a['valuation_price'])
                close(l['net_pnl'],a['all_in_net_pnl'],'pre-scan ledger')
                for key in ('fills','btc_volume','usdc_volume','fees','funding_cashflow','net_pnl'):
                    if key in spec:
                        close(l[key],spec[key],'original comparison '+key)
                endpoints.add(a['account_end_ns'])
                for f in fills:
                    at=f['fill_ts']*10**6;endpoints.add(at)
                    endpoints.update(at+h*10**9 for h in HORIZONS if at+h*10**9<=a['account_end_ns'])
            valuations=terminal_valuations(task['bundle'],endpoints,max_mark_age_ns=task['max_mark_age_ns'])
            marks={t:v.price for t,v in valuations.items()}
            paired={}
            for spec,(_,a,fills,funding) in loaded:
                if a['valuation_price'] is not None:
                    close(marks[a['account_end_ns']],a['valuation_price'],'same terminal valuation')
                summary,records,lives=decompose(fills,a,funding,marks)
                identity=dict(account_id=account,arm=spec['arm'])
                account_rows.append(dict(identity,**summary));paired[spec['arm']]=summary
                lifecycle_rows.extend(dict(identity,**l) for l in lives)
                for r in records:
                    r.update(identity,reference_age_ns=valuations[r['match_ns']].age_ns)
                if records:
                    table=pa.Table.from_pylist(records)
                    if writer is None:
                        # Explicit nullable strings avoid null-only schema drift.
                        schema=pa.schema([pa.field(f.name,pa.string() if f.name=='missing_reason' or f.name.startswith('reason_') else f.type) for f in table.schema])
                        writer=pq.ParquetWriter(args.output/'fill_diagnostics.parquet',schema,compression='zstd')
                    writer.write_table(table.cast(writer.schema))
                details[account+':'+spec['arm']]=grouped_diagnostics(records,lives)
            for left,right in (('D','B0'),('S','B0'),('D','S')):
                if left in paired and right in paired:
                    keys=('E','H','fees','funding_cashflow','unallocated','net_pnl','original_pnl')
                    delta={k:paired[left][k]-paired[right][k] for k in keys}
                    close(delta['E']+delta['H']-delta['fees']+delta['funding_cashflow']+delta['unallocated'],delta['net_pnl'],'paired delta')
                    account_rows.append(dict(account_id=account,arm=left+'-'+right,**delta))
            print(json.dumps(dict(account=account,status='offline_analysis_complete',new_strategy_replays=0)),flush=True)
    finally:
        if writer:writer.close()
    write_csv(args.output/'account_attribution.csv',account_rows)
    write_csv(args.output/'inventory_lifecycles.csv',lifecycle_rows)
    receipt=dict(visibility='local_only_do_not_publish',new_strategy_replays=0,
        valuation='terminal_valuations: last depth ready_ns strictly < query; invalid/stale latest not skipped; source age at query; original per-task age limit',
        precision='economic matching clock is milliseconds; no invented within-millisecond ordering; equal-ready BBO excluded',
        markout='1/5/10/30 seconds, diagnostic only; price and bps use fill-reference mid denominator; never added to account PnL',
        fields={'cash_fill':'negative signed fill notional minus signed fees, USDC',
            'E':'signed fill quantity times delivered-mid minus fill-price, USDC',
            'H':'pre-fill inventory times reference-price change, including terminal tail, USDC',
            'funding_cashflow':'funding_cashflow_usdc, (start,end], before equal-time fills',
            'unallocated':'lifecycle gross cash/MTM minus known E/H only where boundary mid missing',
            'source':'public_accounting.settle_public_replay; terminal_valuations; continuous_accounting.funding_cashflow_usdc'},
        comparison=str(args.comparison),command=[sys.executable,'-m','models.replay.pnl_attribution',*sys.argv[1:]],
        details=details,wall_seconds=time.time()-wall,cpu_seconds=time.process_time()-cpu,
        peak_rss_native_units=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        peak_rss_units='bytes' if sys.platform=='darwin' else 'KiB',
        output_bytes_before_receipt=sum(p.stat().st_size for p in args.output.iterdir() if p.is_file()))
    (args.output/'receipt.json').write_text(json.dumps(receipt,indent=2,allow_nan=False)+'\n')


if __name__=='__main__':main()
