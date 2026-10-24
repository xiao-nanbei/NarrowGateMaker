"""Fixed-panel descriptive reporting; no selection, retuning or scheduling."""
from collections import Counter, defaultdict
import math
from pathlib import Path

from .mechanism_ablation import ARMS
from .mechanism_execution import PARENTS, validate_cell
from narrowgate.runtime.artifact_io import load_json as _load

RISK_CONTRACT = {
    'max_inventory':'BTC, maximum absolute inventory',
    'abs_inventory_time_s':'BTC seconds, integral of absolute inventory',
    'sq_inventory_time_s':'BTC squared seconds, integral of squared inventory',
    'max_drawdown':'USDC, drawdown of the engine sampled PnL curve; not relabelled as a separately recomputed all-in funding equity curve',
    'inventory_lifecycle_max_duration_s':'seconds; account-end open lifecycles are censored',
    'inventory_lifecycle_max_adverse_excursion':'USDC, signed nonpositive engine lifecycle PnL excursion',
    'inventory_lifecycle_counts':'counts; unavailable fields remain UNKNOWN',
}


def opportunity_summary(rows):
    opportunities = defaultdict(lambda: {'callbacks':0, 'actions':set(),
        'negative_markout_callbacks':0, 'nonzero_asym_callbacks':0})
    counts = Counter()
    for row in rows:
        counts[row['event_type']] += 1
        if row['event_type'] != 'decision':
            continue
        record = row['record']
        generation = record.get('quote_snapshot_market_generation')
        if not generation or row.get('side') not in ('BUY', 'SELL'):
            counts['decision_key_unknown'] += 1
            continue
        key = (row['side'], generation)
        value = opportunities[key]
        value['callbacks'] += 1
        value['actions'].add(record['action'])
        value['negative_markout_callbacks'] += bool(record['generic_markout_negative_condition'])
        value['nonzero_asym_callbacks'] += bool(record['raw_asym_shift'])
    return opportunities, counts


def compare_opportunities(left_rows, right_rows):
    left, lc = opportunity_summary(left_rows)
    right, rc = opportunity_summary(right_rows)
    common = left.keys() & right.keys()
    return {'key':'side + quote_snapshot_market_generation',
        'left_production':dict(lc), 'right_production':dict(rc),
        'union_keys':len(left.keys() | right.keys()), 'common_keys':len(common),
        'left_only_keys':len(left.keys()-right.keys()), 'right_only_keys':len(right.keys()-left.keys()),
        'common_action_set_disagreements':sum(left[k]['actions'] != right[k]['actions'] for k in common),
        'common_callback_count_disagreements':sum(left[k]['callbacks'] != right[k]['callbacks'] for k in common),
        'left_negative_markout_callbacks':sum(x['negative_markout_callbacks'] for x in left.values()),
        'right_negative_markout_callbacks':sum(x['negative_markout_callbacks'] for x in right.values()),
        'left_nonzero_asym_callbacks':sum(x['nonzero_asym_callbacks'] for x in left.values()),
        'right_nonzero_asym_callbacks':sum(x['nonzero_asym_callbacks'] for x in right.values()),
        'interpretation':'Common keys join market opportunities, not own-arm order IDs or identical RNG requests. Repeated callbacks are retained as counts/action sets, not fabricated one-to-one causal matches.'}


def complete_panel(cells):
    """Require exactly the authorized 36 full paths, with one source identity."""
    from execution.chunked_parquet_journal import iter_chunked_parquet_journal
    expected = {(parent,arm) for parent in PARENTS for arm in ARMS}
    if len(cells) != 36 or {(s['parent_id'],s['arm_id']) for _,s in cells} != expected:
        raise ValueError('report requires the fixed 12 by 3 panel without duplicates')
    if len({s['release_identity'] for _,s in cells}) != 1:
        raise ValueError('mixed source identities')
    rows=[];journals={}
    for directory,spec in cells:
        directory=Path(directory);receipt=validate_cell(directory,spec)
        metrics=_load(directory/'metrics.json');accounting=_load(directory/'accounting.json')
        diagnostic=_load(directory/'diagnostics.json')
        pnl,qty,notional=(metrics[k] for k in ('net_pnl','volume_btc','notional_usdc'))
        rows.append({'parent':spec['parent_id'],'arm':spec['arm_id'], 'P':pnl,'Q':qty,'N':notional,
            'pnl_per_0_001_btc':.001*pnl/qty if qty else None,
            'pnl_per_10000_usdc':10000*pnl/notional if notional else None,
            'fees':accounting['fees'],'funding':accounting['funding_cashflow'],
            'terminal_mtm':accounting['terminal_unrealized_pnl'],
            'terminal_inventory':accounting['terminal_inventory'],'risk':diagnostic['risk'],
            'risk_missing':diagnostic['risk_missing'],'censoring':diagnostic['censoring']})
        manifests=[directory/n for n in receipt['payload_sha256'] if n.startswith('journal/') and n.endswith('/manifest.json')]
        if len(manifests)!=1:raise ValueError('ambiguous final journal')
        journals[spec['parent_id'],spec['arm_id']]=manifests[0]
    comparisons=[]
    for parent in PARENTS:
        for arm in ARMS[1:]:
            comparison=compare_opportunities(
                iter_chunked_parquet_journal(journals[parent,'B0'],verify_checksum=True),
                iter_chunked_parquet_journal(journals[parent,arm],verify_checksum=True))
            comparisons.append({'parent':parent,'candidate':arm,**comparison})
    aggregates={}
    for arm in ARMS:
        selected=[r for r in rows if r['arm']==arm]
        pnl,qty,notional=(math.fsum(r[k] for r in selected) for k in ('P','Q','N'))
        outcomes=sorted(r['P'] for r in selected)
        aggregates[arm]={'P':pnl,'Q':qty,'N':notional,
            'pnl_per_0_001_btc':.001*pnl/qty if qty else None,
            'pnl_per_10000_usdc':10000*pnl/notional if notional else None,
            'minimum_parent_pnl':outcomes[0],
            'empirical_lower_decile_nearest_rank':outcomes[math.ceil(.1*len(outcomes))-1]}
    return {'schema':'mechanism12_descriptive_report.v1','rows':rows,'aggregates':aggregates,
        'risk_contract':dict(RISK_CONTRACT),
        'common_opportunities':comparisons,
        'limits':'Twelve previous-use Development parents are exploratory. No fresh Final holdout, equivalence, risk non-inferiority or live causal claim. Missing risk fields remain UNKNOWN.'}
