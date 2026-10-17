"""Finite mechanism12 accounts with independent recovery and audited diagnostics.

This is not an alias for the historical D/U executor. Source/input admission and
placement remain external; this module never schedules work or selects dates.
"""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import fcntl
import json
import math
from pathlib import Path
import shutil
import time
import uuid

from .mechanism_ablation import ARMS, BASELINE, EXPERIMENT, FULL_EXPERIMENT, PARENT_INDICES, arm_parameters
from .defense_urgency_execution import _atomic, _load, _numeric, cell_lock

DAY_NS = 86400 * 10**9
HOUR_MS = 3_600_000
PARENTS = {
    f'development-{i:03d}-{date(2025, 8, 1)+timedelta(days=2*i)}':
        (1754006400000000000+2*i*DAY_NS, 1754006400000000000+(2*i+2)*DAY_NS)
    for i in PARENT_INDICES
}
FULL_PARENTS = {
    f"{'development' if i < 150 else 'final'}-{i if i < 150 else i-150:03d}-{date(2025, 8, 1)+timedelta(days=2*i)}":
        (1754006400000000000+2*i*DAY_NS,
         1754006400000000000+min(2*i+2, 407)*DAY_NS)
    for i in range(204)
}

def identity(spec):
    keys = ('experiment_id', 'baseline_id', 'account_contract', 'parent_id',
            'arm_id', 'seed_pair', 'start_ns', 'end_ns', 'input_manifest_id', 'release_identity')
    v = {key: deepcopy(spec[key]) for key in keys}
    contracts = {EXPERIMENT: ('MECHANISM12_INDEPENDENT48H', PARENTS),
                 FULL_EXPERIMENT: ('MECHANISM407_INDEPENDENT', FULL_PARENTS)}
    contract, parents = contracts.get(v['experiment_id'], (None, {}))
    if (v['experiment_id'] == FULL_EXPERIMENT
            and v['account_contract'] == 'MECHANISM407_FINAL107_CONTINUOUS'):
        contract = 'MECHANISM407_FINAL107_CONTINUOUS'
        parents = {'Final107-continuous': (1779926400000000000, 1789171200000000000)}
    if (contract is None or v['baseline_id'] != BASELINE
            or v['account_contract'] != contract
            or v['arm_id'] not in ARMS or v['parent_id'] not in parents
            or (v['start_ns'], v['end_ns']) != parents.get(v['parent_id'])
            or v['seed_pair'] != {'rng_seed': 42, 'latency_seed': 59}
            or any(not isinstance(v[k], str) or len(v[k]) != 64
                   or any(c not in '0123456789abcdef' for c in v[k])
                   for k in ('input_manifest_id', 'release_identity'))):
        raise ValueError('mechanism12 identity differs from fixed contract')
    return v

def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(4*1024*1024), b''):
            h.update(block)
    return h.hexdigest()

def simulate_slice(prepared, params, predictions, journal, checkpoint=None, cut=None, initialization_lock=None):
    """The maintained engine owns all account/order/RNG state, never another arm."""
    from models.backtest_tick import simulate_prepared_inputs
    actual = deepcopy(params)
    actual['_l2_journal'] = journal
    lock = None
    try:
        if initialization_lock is not None:
            lock = Path(initialization_lock).open('a')
            fcntl.flock(lock, fcntl.LOCK_EX)
            original = actual.get('_replay_progress_callback')
            released = False
            # Bound a resumed slice's wait for its first operational callback.
            # This changes observer cadence only, never simulated compute/time.
            actual['_replay_progress_interval_events'] = 1000
            def entered_loop(value):
                # Operational callback is excluded by the existing checkpoint
                # binding and stripped at save; no business parameter changes.
                nonlocal released
                if not released:
                    fcntl.flock(lock, fcntl.LOCK_UN)
                    released = True
                if original is not None: original(value)
            actual['_replay_progress_callback'] = entered_loop
        return simulate_prepared_inputs(prepared, actual, prepared_predictions=predictions,
            resume_checkpoint=checkpoint, consume_resume_checkpoint=checkpoint is not None,
            checkpoint_at_ts_ms=cut)
    finally:
        if lock is not None:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()

def diagnostics(result, journal, *, full_calendar=False):
    """Keep full event rows for paired opportunity joins; no invented denominators."""
    from execution.chunked_parquet_journal import iter_chunked_parquet_journal
    from collections import Counter
    counts = Counter(); missing = Counter(); decisions = 0
    for row in iter_chunked_parquet_journal(journal['manifest'], verify_checksum=True):
        kind = row['event_type']; counts[kind] += 1
        if kind == 'decision':
            decisions += 1; p = row['record']
            for key in ('quote_snapshot_market_generation', 'market_readiness',
                        'action', 'base_price', 'final_price', 'base_size', 'final_size'):
                if key not in p: missing[key] += 1
    if missing or not journal['independent_delivery_verified']:
        raise ValueError('incomplete decision-to-order diagnostic delivery')
    risk_names = ('max_drawdown', 'max_inventory', 'abs_inventory_time_s',
                  'sq_inventory_time_s', 'inventory_lifecycle_count',
                  'inventory_lifecycle_closed_count', 'inventory_lifecycle_open_count',
                  'inventory_lifecycle_max_duration_s', 'inventory_lifecycle_max_adverse_excursion')
    return {'schema': 'mechanism407_diagnostics.v1' if full_calendar else 'mechanism12_diagnostics.v1', 'events': dict(counts),
            'decisions': decisions, 'missing_fields': dict(missing),
            'risk': {k: result.get(k) for k in risk_names},
            'risk_missing': [k for k in risk_names if k not in result],
            'common_opportunity_key': 'side + quote_snapshot_market_generation; aggregate repeated own-arm decisions, retain union/missing/intersection, never align own order IDs across arms',
            'opportunity_scope': 'instrumented side callbacks after original warmup; not every external event is an eligible decision',
            'censoring': 'open lifecycle at account_end is administratively censored; no forced liquidation',
            'tail_contract': ('all 204 original parents including the 24h tail; frozen paired block diagnostics, previous-use retained; no equivalence claim'
                              if full_calendar else 'report all 12 paired parent outcomes, minimum and empirical lower decile; no significance/equivalence claim'),
            'production': journal['production']}

def validate_cell(output, spec):
    return validate_bound_cell(output, identity(spec), validate_effective_params)


def validate_bound_cell(output, ident, validate_params):
    """Shared payload/accounting checks after an owner validates its identity."""
    output = Path(output); receipt = _load(output/'completion.json')
    if receipt.get('identity') != ident or receipt.get('economic_complete') is not True:
        raise ValueError('mechanism completion identity differs')
    required = {'result.json', 'accounting.json', 'metrics.json', 'effective_params.json',
                'daily-ledger.json', 'diagnostics.json'}
    payloads = receipt['payload_sha256']
    if not required.issubset(payloads) or not any(x.endswith('/manifest.json') for x in payloads):
        raise ValueError('missing mechanism payloads')
    for name, digest in payloads.items():
        p = output/name
        if not p.resolve().is_relative_to(output.resolve()) or p.is_symlink() or sha(p) != digest:
            raise ValueError('mechanism payload changed: '+name)
    accounting = _load(output/'accounting.json'); ledger = _load(output/'daily-ledger.json')
    metrics = _load(output/'metrics.json'); result = _load(output/'result.json')
    params = _load(output/'effective_params.json')
    validate_params(params, ident['arm_id'])
    if (accounting.get('economic_complete') is not True or ledger['accounting'] != accounting
            or accounting['account_start_ns'] != ident['start_ns']
            or accounting['account_end_ns'] != ident['end_ns']
            or result['public_input_contract']['input_manifest_id'] != ident['input_manifest_id']
            or receipt['durable_cut_ms']*10**6 != ident['end_ns']
            or params['account_start_ns'] != ident['start_ns']
            or params['rng_seed'] != 42 or params['latency_seed'] != 59
            or ledger['input_manifest_id'] != ident['input_manifest_id']
            or ledger['account_start_ns'] != ident['start_ns']
            or ledger['account_end_ns'] != ident['end_ns']
            or metrics['net_pnl'] != accounting['all_in_net_pnl']
            or len(ledger['rows']) != (ident['end_ns']-ident['start_ns'])//DAY_NS
            or ledger['account_reset'] is not False):
        raise ValueError('mechanism accounting binding differs')
    for i,row in enumerate(ledger['rows']):
        if (row['start_ns'],row['end_ns']) != (ident['start_ns']+i*DAY_NS, ident['start_ns']+(i+1)*DAY_NS):
            raise ValueError('daily ledger hole')
    for key in ('net_pnl','volume_btc','notional_usdc'):
        if not math.isfinite(metrics[key]) or not math.isclose(math.fsum(x[key] for x in ledger['rows']),metrics[key],rel_tol=1e-10,abs_tol=1e-8):
            raise ValueError('ledger totals differ')
    if not math.isclose(accounting['terminal_equity']-accounting['initial_capital'],metrics['net_pnl'],rel_tol=0,abs_tol=1e-8):
        raise ValueError('equity identity differs')
    for key in ('fees', 'funding_cashflow'):
        if not math.isclose(math.fsum(row[key] for row in ledger['rows']), accounting[key], rel_tol=1e-10, abs_tol=1e-8):
            raise ValueError('daily fee/funding totals differ')
    if not math.isclose(accounting['pnl_before_funding']+accounting['funding_cashflow'],metrics['net_pnl'],rel_tol=0,abs_tol=1e-8):
        raise ValueError('funding accounting identity differs')
    return receipt

def validate_effective_params(params, arm):
    if (arm not in ARMS or params.get('exit_urgency_strength') != 0.0
            or params.get('asym_strength') != (0.0 if arm == 'DIRECT_DIRECTION_ASYM_OFF' else 0.1)
            or params.get('markout_spread_scale') != 0.2
            or params.get('response_observe_only') is not True
            or params.get('response_update_branch_enabled') is not True
            or params.get('_research_generic_markout_policy_enabled', True) is not (arm != 'GENERIC_MARKOUT_POLICY_OFF')):
        raise ValueError('mechanism effective arm parameters differ')

def execute_cell(prepared, *, spec, common_params, prepared_predictions,
                 initial_capital, max_mark_age_ns, funding, output, progress=None,
                 initialization_lock=None):
    ident = identity(spec)
    return execute_bound_cell(prepared, ident=ident,
        params=arm_parameters(common_params, ident['arm_id']),
        prepared_predictions=prepared_predictions, initial_capital=initial_capital,
        max_mark_age_ns=max_mark_age_ns, funding=funding, output=output,
        progress=progress, initialization_lock=initialization_lock,
        validate_output=lambda path: validate_cell(path, spec),
        full_calendar=ident['experiment_id'] == FULL_EXPERIMENT)


def execute_bound_cell(prepared, *, ident, params, prepared_predictions,
                       initial_capital, max_mark_age_ns, funding, output,
                       validate_output, full_calendar, progress=None,
                       initialization_lock=None, checkpoint_key='mechanism_cell',
                       diagnostic_builder=diagnostics, retain_completed_checkpoint=False,
                       incremental_journal=False):
    """Unchanged replay/accounting primitive, with an explicit owner boundary.

    The caller must validate identity and full effective parameters first. This
    interface is not a scheduler or an alternative arm registry.
    """
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from models.replay.public_accounting import settle_public_replay, public_equity_ledger
    from models.replay.l2_journal import ReplayL2Journal
    plan = prepared.inputs['bundle'].manifest['plan']
    if (prepared.manifest_id != ident['input_manifest_id'] or plan['end_ns'] != ident['end_ns']
            or params['account_start_ns'] != ident['start_ns']
            or params.get('initial_live_state') or params.get('replay_initial_state_mode','fresh_start') != 'fresh_start'):
        raise ValueError('mechanism input/account binding differs')
    output = Path(output)
    with cell_lock(output):
        if (output/'identity.json').exists() and _load(output/'identity.json') != ident:
            raise ValueError('foreign mechanism owner')
        _atomic(output/'identity.json',ident)
        if (output/'completion.json').exists():
            validate_output(output)
            if _load(output/'effective_params.json') != json.loads(json.dumps(params,default=_numeric)):
                raise ValueError('completed mechanism parameters differ')
            return {'status':'ALREADY_COMPLETE','identity':ident}
        checkpoint_path = output/'latest.checkpoint'
        checkpoint = load_trusted_runtime_checkpoint(checkpoint_path) if checkpoint_path.exists() else None
        if checkpoint is not None and checkpoint.get(checkpoint_key) != ident:
            raise ValueError('foreign mechanism checkpoint')
        if (incremental_journal and checkpoint is not None
                and not checkpoint.get('l2_prefix', {}).get('manifest_sha256')):
            raise ValueError('legacy checkpoint requires its original journal release; no implicit migration')
        # This is one independently owned account, not a branching replay.
        # Reuse its append-only sink between hourly cuts; economic state and
        # durable checkpoint cadence remain entirely unchanged. Other owners
        # retain the ordinary branching journal unless they explicitly opt in.
        journal = None
        if incremental_journal:
            from models.replay.continuous_l2_journal import ContinuousReplayL2Journal
            journal = ContinuousReplayL2Journal(output/'journal'/uuid.uuid4().hex, identity=ident)
        cuts = list(range(ident['start_ns']//10**6+HOUR_MS,ident['end_ns']//10**6,HOUR_MS))+[None]
        for cut in cuts:
            if cut is not None and checkpoint is not None and cut <= checkpoint['cut_ts_ms']:
                continue
            old_manifest = Path(checkpoint['l2_prefix']['manifest']) if checkpoint else None
            old_prefix = old_manifest.parent if old_manifest else None
            if not incremental_journal:
                journal = ReplayL2Journal(output/'journal'/uuid.uuid4().hex,identity=ident)
            slice_started = time.monotonic()
            result = simulate_slice(prepared,params,prepared_predictions,journal,checkpoint,cut,initialization_lock)
            slice_seconds = time.monotonic()-slice_started
            if cut is not None:
                checkpoint = result['_replay_checkpoint'];checkpoint[checkpoint_key]=ident
                if checkpoint['cut_ts_ms'] != cut:raise ValueError('unexpected durable cut')
                save_started = time.monotonic()
                save_runtime_checkpoint(checkpoint_path,checkpoint)
                status={'status':'PARTIAL','identity':ident,'durable_cut_ms':cut,
                        'slice_seconds_including_initialization_wait':slice_seconds,
                        'checkpoint_save_seconds':time.monotonic()-save_started,
                        'journal_rows':journal.writer.row_count,
                        'journal_mode':'single_owner_incremental' if incremental_journal else 'branching_prefix_copy'}
                _atomic(output/'progress.json',status)
                if old_prefix and old_prefix.is_relative_to(output/'journal'):
                    if old_prefix != journal.writer.output_dir:
                        shutil.rmtree(old_prefix)
                    elif old_manifest != Path(checkpoint['l2_prefix']['manifest']):
                        # Replacement is already durable. Never remove the
                        # current sink's parts or sole recoverable prefix.
                        if old_manifest.name.startswith('prefix-'):
                            old_manifest.unlink()
                if progress:progress(status)
        accounting=settle_public_replay(prepared.inputs['bundle'].root,result,
            initial_capital=initial_capital,max_mark_age_ns=max_mark_age_ns,funding=deepcopy(funding))
        ledger=public_equity_ledger(prepared.inputs['bundle'].root,result,
            range(ident['start_ns']+DAY_NS,ident['end_ns']+1,DAY_NS),initial_capital=initial_capital,
            max_mark_age_ns=max_mark_age_ns,funding=deepcopy(funding))
        fills=result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']
        metrics={'net_pnl':accounting['all_in_net_pnl'],
                 'volume_btc':math.fsum(x['fill_qty'] for x in fills),
                 'notional_usdc':math.fsum(x['fill_qty']*x['quote_px'] for x in fills)}
        values={'result':result,'accounting':accounting,'daily-ledger':ledger,
                'effective_params':params,'metrics':metrics,'diagnostics':diagnostic_builder(result,result['_l2_journal'],
                    full_calendar=full_calendar)}
        for name,value in values.items():_atomic(output/(name+'.json'),value)
        files=[output/(name+'.json') for name in values]
        files += [p for p in journal.writer.output_dir.rglob('*') if p.is_file()]
        receipt={'identity':ident,'economic_complete':True,'durable_cut_ms':ident['end_ns']//10**6,
                 'payload_sha256':{str(p.relative_to(output)):sha(p) for p in files}}
        _atomic(output/'completion.json',receipt);validate_output(output)
        if not retain_completed_checkpoint:
            checkpoint_path.unlink(missing_ok=True)
            if (old_prefix and old_prefix != journal.writer.output_dir
                    and old_prefix.is_relative_to(output/'journal')):
                shutil.rmtree(old_prefix)
        return {'status':'COMPLETE','identity':ident}
