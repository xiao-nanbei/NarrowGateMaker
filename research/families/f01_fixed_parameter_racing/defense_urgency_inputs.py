"""Bind continuous D/U inputs from admitted parent source contracts, not results.

This constructs one observation stream. It never joins independently warmed
consumer bundles or changes the frozen market/feature/latency configuration.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path


DAY_NS = 86400 * 10**9


def _days(start, end):
    first = datetime.fromtimestamp(start // 10**9, timezone.utc).date()
    last = datetime.fromtimestamp((end-1) // 10**9, timezone.utc).date()
    return [(first+timedelta(days=i)).isoformat() for i in range((last-first).days+1)]


def continuous_plan(calendar, registry, facts_root, latency_path):
    """Merge immutable fact references and separately bound funding schedules."""
    from .defense_urgency_continuous import continuous_bounds
    start, end = continuous_bounds(calendar)
    rows = sorted((r for r in registry['parents'].values()
                   if r['identity']['parent_id'].startswith('final-')),
                  key=lambda r: r['identity']['start_ns'])
    if len(rows) != 54:
        raise ValueError('all 54 original Final contracts are required')
    cursor = start
    sources, sizes, funding_events, expected, funding_sources = {}, {}, {}, set(), set()
    first = rows[0]
    profile = first['input_config']['observation']['observation_profile']
    market = first['input_config']['observation']['market_id']
    for row in rows:
        identity, config = row['identity'], row['input_config']
        if identity['start_ns'] != cursor or identity['end_ns'] <= cursor:
            raise ValueError('Final dates overlap or have a gap')
        cursor = identity['end_ns']
        observation = config['observation']
        if (observation['observation_profile'] != profile
                or observation['market_id'] != market
                or observation.get('include_response_observations') is not True
                or row['params'].get('cross_market_enabled') is not False):
            raise ValueError('mixed observation/market contracts; explicit adapter required')
        for source in observation['source_bundles']:
            day = Path(source['path']).name
            item = dict(path=str(Path(facts_root)/day), sha256=source['sha256'])
            if day in sources and sources[day] != item:
                raise ValueError('conflicting daily fact identity')
            sources[day] = item
            inventory = config['facts'][day]
            if day in sizes and sizes[day] != inventory:
                raise ValueError('conflicting fact file inventory')
            sizes[day] = inventory
        funding = config['funding']
        if (funding['market_id'] != market or funding['coverage_start_ns'] > identity['start_ns']
                or funding['coverage_end_ns'] < identity['end_ns'] or not funding['source_identity']):
            raise ValueError('funding coverage/source differs')
        clocks = [e['settlement_ns'] for e in funding['events']]
        if clocks != sorted(set(clocks)) or clocks != funding['expected_settlements_ns']:
            raise ValueError('funding schedule incomplete')
        for event in funding['events']:
            t = event['settlement_ns']
            if start < t <= end:
                if t in funding_events and funding_events[t] != event:
                    raise ValueError('conflicting funding event')
                funding_events[t] = deepcopy(event)
                expected.add(t)
        funding_sources.update(funding['source_identity'])
    if cursor != end:
        raise ValueError('wrong global endpoint')
    plan = deepcopy(first['input_config']['observation'])
    needed = _days(plan['start_ns'], end)
    if set(sources) != set(needed):
        raise ValueError('full calendar plus original warmup facts are not bound')
    if plan['start_ns'] > start:
        raise ValueError('warmup starts after account')
    plan.update(end_ns=end, source_start_day=needed[0], source_end_day=needed[-1],
                source_bundles=[sources[d] for d in needed],
                purpose='defense_urgency_final107_single_continuous_account')
    plan['observation_profile']['measured_latency_path'] = str(latency_path)
    funding = dict(market_id=market, coverage_start_ns=start, coverage_end_ns=end,
                   source_identity=sorted(funding_sources),
                   expected_settlements_ns=sorted(expected),
                   events=[funding_events[t] for t in sorted(expected)])
    return dict(observation=plan, facts={d:sizes[d] for d in needed}, funding=funding,
                start_ns=start, end_ns=end, scored_days=107,
                warmup_ns=start-plan['start_ns'], market_id=market,
                cross_market_enabled=False, common_params=deepcopy(first['params']),
                initial_capital=first['input_config']['probe']['initial_capital'],
                max_mark_age_ns=first['input_config']['probe']['max_mark_age_ns'])


def inspect_sources(plan):
    """Exact manifest binding and file/Parquet structure; not full content QA.

Full byte verification, ordered decoding and duplicate-ID rejection remain in
the existing derive_inputs/ConsumerBundle consumers. Unknown gaps stay visible.
"""
    import pyarrow.parquet as pq
    findings, prior_trade_max = [], None
    for source in plan['observation']['source_bundles']:
        root = Path(source['path'])
        payload = (root/'manifest.json').read_bytes()
        if hashlib.sha256(payload).hexdigest() != source['sha256']:
            raise ValueError('source manifest changed: '+root.name)
        manifest = json.loads(payload)
        if manifest['schema'] != 'data.facts.v1':
            raise ValueError('unknown fact contract')
        for name, size in plan['facts'][root.name].items():
            path = root/name
            if path.is_symlink() or not path.is_file() or path.stat().st_size != size:
                raise ValueError('fact file missing/changed: '+str(path))
        channels = {(r['symbol'], r['channel']) for r in manifest['files']}
        if channels != {('BTCUSDC', 'trades'), ('BTCUSDC', 'incremental_book_L2')}:
            raise ValueError('market/channel differs')
        for item in manifest['files']:
            metadata = pq.ParquetFile(root/item['file']).metadata
            quality = item['quality']
            if item['channel'] == 'trades':
                low, high = quality.get('trade_id_min'), quality.get('trade_id_max')
                if prior_trade_max is not None and low is not None and low <= prior_trade_max:
                    raise ValueError('cross-day trade overlap requires reconciliation')
                if high is not None:
                    prior_trade_max = high
            findings.append(dict(day=root.name, channel=item['channel'], rows=metadata.num_rows,
                                 source_quality=quality))
    return dict(status='MANIFEST_AND_STRUCTURE_VERIFIED_CONTENT_SCAN_PENDING',
                source_days=len(plan['facts']), findings=findings)
