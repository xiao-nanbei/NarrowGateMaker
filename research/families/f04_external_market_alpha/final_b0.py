"""New-B0 Final107 M0/M1 evaluation; original first-batch contracts stay frozen.

This module does not train, schedule, select a winner, or activate live trading.
Only the direction head differs; every arm owns its economic account state.
"""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import math
from pathlib import Path
import shutil
import uuid

from data.feature_cursor import FeatureCursor
from data.runtime import ConsumerBundle, ObservationProfile, bundle_paths, derive_inputs
from research.families.f01_fixed_parameter_racing.defense_urgency_execution import (
    _atomic, _load, cell_lock,
)
from research.families.f01_fixed_parameter_racing.mechanism_ablation import arm_parameters
from research.families.f01_fixed_parameter_racing.mechanism_execution import diagnostics, simulate_slice
from research.families.f04_external_market_alpha.tardis_direction import (
    DirectionModel, EXECUTION_MARKET, REFERENCE_MARKET, TardisDirectionSignalEngine,
)

EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_F04_FINAL107_20261013'
BASELINE = 'B0_DEFENSE_URGENCY_OFF_20261011'
DAY_NS = 86400 * 10**9
START_NS = 1779926400000000000
END_NS = 1789171200000000000
ARMS = ('M0', 'M1')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(4*1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def identity(spec, *, experiment=EXPERIMENT, arms=ARMS):
    keys = ('experiment_id', 'baseline_id', 'parent_id', 'arm_id', 'start_ns',
            'end_ns', 'input_manifest_id', 'model_identity', 'release_identity', 'seed_pair')
    value = {k: deepcopy(spec[k]) for k in keys}
    parents = {f'final-{i:03d}-{date(2026, 5, 28)+timedelta(days=2*i)}':
               (START_NS+2*i*DAY_NS, min(START_NS+(2*i+2)*DAY_NS, END_NS))
               for i in range(54)}
    if (value['experiment_id'] != experiment or value['baseline_id'] != BASELINE
            or value['arm_id'] not in arms
            or parents.get(value['parent_id']) != (value['start_ns'], value['end_ns'])
            or value['seed_pair'] != {'rng_seed': 42, 'latency_seed': 59}
            or any(not isinstance(value[k], str) or len(value[k]) != 64
                   or set(value[k])-set('0123456789abcdef')
                   for k in ('input_manifest_id', 'model_identity', 'release_identity'))):
        raise ValueError('F04 Final identity outside authorized fixed calendar')
    return value


def parameters(original_parent, arm, *, arms=ARMS):
    if arm not in arms:
        raise ValueError('arm is outside the selected fixed Final contract')
    # Complete inherited B0, not live configuration or the old F04 config.
    return arm_parameters(original_parent, 'B0')


def reference_plan(execution_root, facts_root, model_plan, *, contract_sha, parent_id):
    execution = ConsumerBundle(execution_root)
    ep = execution.manifest['plan']
    if ep['market_id'] != EXECUTION_MARKET:
        raise ValueError('execution market must remain BTCUSDC')
    days = [Path(x['path']).name for x in execution.manifest['source_bundles']]
    if not days or days != sorted(set(days)):
        raise ValueError('ordered original source days required')
    paths = bundle_paths(facts_root, start=days[0], end=days[-1], relocated=True)
    if [p.name for p in paths] != days:
        raise ValueError('reference support days differ')
    declared = model_plan['observation']['reference']
    profile = dict(ep['observation_profile'])
    for key in ('profile_id', 'clock_policy', 'market_delay_ns', 'processing_ns',
                'measured_latency_path', 'measured_latency_sha256', 'measured_latency_market_id'):
        profile[key] = declared[key]
    ObservationProfile(**profile).require_market(REFERENCE_MARKET)
    for p in paths:
        if {row['symbol'] for row in _load(p/'manifest.json')['files']} != {'BTCUSDT'}:
            raise ValueError('reference facts are not BTCUSDT')
    return dict(source_bundles=[{'path': str(p), 'sha256': sha(p/'manifest.json')} for p in paths],
                market_id=REFERENCE_MARKET, observation_profile=profile,
                observation_scenario={'schema': 'data.observation_scenario.v1',
                    'market_id': REFERENCE_MARKET, 'classification': 'simulated_not_measured',
                    'parameter_basis': declared['origin'], 'provider_local_timestamp_as_receive': False,
                    'native_observation_parity': 'not_proven'},
                start_ns=ep['start_ns'], end_ns=ep['end_ns'], source_start_day=days[0],
                source_end_day=days[-1], include_outcome_bars=False,
                execution_parent_input_manifest_id=execution.input_manifest_id,
                frozen_contract_sha256=contract_sha, frozen_unit=parent_id)


def prepare_reference(plan, output):
    output = Path(output)
    if not output.exists():
        derive_inputs(plan, output)
    bundle = ConsumerBundle(output)
    if bundle.manifest['plan'] != plan or set(bundle.manifest['files']) != {'features', 'bars', 'depth'}:
        raise ValueError('reference input identity differs')
    bundle.source_paths()
    for name in bundle.manifest['files']:
        for _ in bundle.batches(name):
            pass
    return bundle


class FinalDirectionSignalEngine(TardisDirectionSignalEngine):
    """Reuse the exact learned heads and established feature/prediction consumer."""

    def __init__(self, execution_root, reference_root, frozen_base_model_root,
                 direction_model: DirectionModel, *, expected_input_id,
                 expected_reference_plan=None):
        self.execution = ConsumerBundle(execution_root)
        if (self.execution.input_manifest_id != expected_input_id
                or self.execution.manifest['plan']['market_id'] != EXECUTION_MARKET):
            raise ValueError('F04 Final execution input differs')
        self.model = direction_model
        self.plan = direction_model.plan
        if sha(Path(frozen_base_model_root)/'public_input_model.json') != self.plan['prediction']['frozen_F03_inf_model_manifest_sha256']:
            raise ValueError('common F03 heads changed')
        from strategy.signal import SignalEngine
        self.base = SignalEngine.from_public_models(frozen_base_model_root, symbol='BTCUSDC', ret_demean_halflife=0)
        self.reference = None
        if direction_model.arm == 'M1':
            if reference_root is None or expected_reference_plan is None:
                raise ValueError('M1 requires an admitted reference')
            self.reference = FeatureCursor(reference_root)
            rp = self.reference.bundle.manifest['plan']
            ep = self.execution.manifest['plan']
            if (rp != expected_reference_plan or rp['market_id'] != REFERENCE_MARKET
                    or rp['execution_parent_input_manifest_id'] != expected_input_id
                    or (rp['start_ns'], rp['end_ns']) != (ep['start_ns'], ep['end_ns'])):
                raise ValueError('reference binding/clock differs')
        elif direction_model.arm != 'M0' or reference_root is not None or expected_reference_plan is not None:
            raise ValueError('M0 must not consume reference inputs')
        self.reference_context_missing = self.reference_values_missing = self.decisions = 0


def validate_cell(output, spec, *, experiment=EXPERIMENT, arms=ARMS):
    output = Path(output)
    ident = identity(spec, experiment=experiment, arms=arms)
    c = _load(output/'completion.json')
    if c.get('identity') != ident or not c.get('economic_complete') or c['durable_cut_ms']*10**6 != ident['end_ns']:
        raise ValueError('F04 completion identity differs')
    required = {'result.json', 'accounting.json', 'metrics.json', 'effective_params.json',
                'daily-ledger.json', 'diagnostics.json', 'signal-diagnostics.json'}
    if not required <= set(c['payload_sha256']):
        raise ValueError('F04 result payload missing')
    for name, digest in c['payload_sha256'].items():
        p = output/name
        if p.is_symlink() or not p.resolve().is_relative_to(output.resolve()) or sha(p) != digest:
            raise ValueError('F04 result changed')
    acc, ledger, met, params, result = (_load(output/(n+'.json')) for n in
        ('accounting', 'daily-ledger', 'metrics', 'effective_params', 'result'))
    if (not acc['economic_complete'] or ledger['accounting'] != acc
            or acc['account_start_ns'] != ident['start_ns'] or acc['account_end_ns'] != ident['end_ns']
            or result['public_input_contract']['input_manifest_id'] != ident['input_manifest_id']
            or ledger['input_manifest_id'] != ident['input_manifest_id']
            or met['net_pnl'] != acc['all_in_net_pnl'] or params['exit_urgency_strength'] != 0
            or params['asym_strength'] != .1 or params['markout_spread_scale'] != .2
            or not params.get('_research_generic_markout_policy_enabled', True)
            or params['rng_seed'] != 42 or params['latency_seed'] != 59
            or ledger['account_reset'] is not False):
        raise ValueError('F04 accounting/parameter binding differs')
    if len(ledger['rows']) != (ident['end_ns']-ident['start_ns'])//DAY_NS:
        raise ValueError('F04 daily coverage differs')
    for i, row in enumerate(ledger['rows']):
        if (row['start_ns'], row['end_ns']) != (ident['start_ns']+i*DAY_NS, ident['start_ns']+(i+1)*DAY_NS):
            raise ValueError('F04 ledger hole')
    for key in ('net_pnl', 'volume_btc', 'notional_usdc'):
        if not math.isclose(math.fsum(x[key] for x in ledger['rows']), met[key], abs_tol=1e-8, rel_tol=1e-10):
            raise ValueError('F04 ledger totals differ')
    for key in ('fees', 'funding_cashflow'):
        if not math.isclose(math.fsum(x[key] for x in ledger['rows']), acc[key], abs_tol=1e-8, rel_tol=1e-10):
            raise ValueError('F04 fee/funding totals differ')
    if not math.isclose(acc['terminal_equity']-acc['initial_capital'], met['net_pnl'], abs_tol=1e-8, rel_tol=0):
        raise ValueError('F04 equity identity differs')
    return c


def execute_cell(prepared, *, spec, common_params, prepared_predictions,
                 initial_capital, max_mark_age_ns, funding, output, signal_diagnostics,
                 progress=None, initialization_lock=None, experiment=EXPERIMENT, arms=ARMS):
    from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
    from models.replay.public_accounting import settle_public_replay, public_equity_ledger
    from models.replay.l2_journal import ReplayL2Journal
    ident = identity(spec, experiment=experiment, arms=arms)
    params = parameters(common_params, ident['arm_id'], arms=arms)
    if (prepared.manifest_id != ident['input_manifest_id']
            or prepared.inputs['bundle'].manifest['plan']['end_ns'] != ident['end_ns']
            or params['account_start_ns'] != ident['start_ns'] or params.get('initial_live_state')
            or params.get('replay_initial_state_mode', 'fresh_start') != 'fresh_start'):
        raise ValueError('F04 input/account mismatch')
    output = Path(output)
    with cell_lock(output):
        if (output/'identity.json').exists() and _load(output/'identity.json') != ident:
            raise ValueError('foreign F04 owner')
        _atomic(output/'identity.json', ident)
        if (output/'completion.json').exists():
            return validate_cell(output, spec, experiment=experiment, arms=arms)
        cp_path = output/'latest.checkpoint'
        cp = load_trusted_runtime_checkpoint(cp_path) if cp_path.exists() else None
        if cp is not None and cp.get('f04_final_cell') != ident:
            raise ValueError('foreign F04 checkpoint')
        cuts = [*range(ident['start_ns']//10**6+3600000, ident['end_ns']//10**6, 3600000), None]
        for cut in cuts:
            if cut is not None and cp is not None and cut <= cp['cut_ts_ms']:
                continue
            old_prefix = Path(cp['l2_prefix']['manifest']).parent if cp else None
            journal = ReplayL2Journal(output/'journal'/uuid.uuid4().hex, identity=ident)
            result = simulate_slice(prepared, params, prepared_predictions, journal, cp, cut, initialization_lock)
            if cut is not None:
                cp = result['_replay_checkpoint']
                cp['f04_final_cell'] = ident
                if cp['cut_ts_ms'] != cut:
                    raise ValueError('unexpected F04 cut')
                save_runtime_checkpoint(cp_path, cp)
                status = {'status': 'PARTIAL', 'identity': ident, 'durable_cut_ms': cut}
                _atomic(output/'progress.json', status)
                if progress:
                    progress(status)
                if old_prefix and old_prefix.is_relative_to(output/'journal'):
                    shutil.rmtree(old_prefix)
        acc = settle_public_replay(prepared.inputs['bundle'].root, result, initial_capital=initial_capital,
                                   max_mark_age_ns=max_mark_age_ns, funding=deepcopy(funding))
        ledger = public_equity_ledger(prepared.inputs['bundle'].root, result,
            range(ident['start_ns']+DAY_NS, ident['end_ns']+1, DAY_NS), initial_capital=initial_capital,
            max_mark_age_ns=max_mark_age_ns, funding=deepcopy(funding))
        fills = result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']
        metrics = {'net_pnl': acc['all_in_net_pnl'], 'volume_btc': math.fsum(x['fill_qty'] for x in fills),
                   'notional_usdc': math.fsum(x['fill_qty']*x['quote_px'] for x in fills)}
        values = {'result': result, 'accounting': acc, 'daily-ledger': ledger, 'effective_params': params,
                  'metrics': metrics, 'diagnostics': diagnostics(result, result['_l2_journal'], full_calendar=True),
                  'signal-diagnostics': signal_diagnostics}
        for name, value in values.items():
            _atomic(output/(name+'.json'), value)
        files = [output/(name+'.json') for name in values]
        files += [p for p in journal.writer.output_dir.rglob('*') if p.is_file()]
        receipt = {'identity': ident, 'economic_complete': True, 'durable_cut_ms': ident['end_ns']//10**6,
                   'payload_sha256': {str(p.relative_to(output)): sha(p) for p in files}}
        _atomic(output/'completion.json', receipt)
        validate_cell(output, spec, experiment=experiment, arms=arms)
        cp_path.unlink(missing_ok=True)
        if old_prefix and old_prefix.is_relative_to(output/'journal'):
            shutil.rmtree(old_prefix)
        return receipt
