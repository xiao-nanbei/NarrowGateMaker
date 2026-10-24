"""Fixed Development300 BTCUSDC bundle extension; no economic dispatch.

This is a separate training identity. It does not widen the original F03
100-day producer, recalibrate P3, or change an active model/runtime.
"""
from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data.facts import save_private_json
from data.observation import CONTRACT, EXECUTION_FEATURE_NAMES, FEATURE_CONTRACT
from data.runtime import (
    ExchangeOutcomeBars, ObservationProfile, PublicInputStream,
    _require_market_source_bundles, _require_observation_scenario,
)
from data.tardis_input import CONTRACT as INPUT_CONTRACT
from research.families.f03_causal_13_head import ml_model
from research.families.f03_causal_13_head.public_input_panel import (
    FEATURE_COLUMNS, delivery_identity, label_frames, publish_model_contract,
)
from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS
from strategy.model_contract import absolute_price_variance_unit_contract

DAYS = tuple((date(2025, 8, 1) + timedelta(days=i)).isoformat() for i in range(300))
CANDIDATE = 'F03_BTCUSDC_TRAIN300_13HEAD_HINF_20261014'
SPLIT_ID = 'f03_development300_inner239_1_60_v1'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_plan(plan):
    expected = dict(schema='narrowgate.f03.development300.v1', candidate=CANDIDATE,
                    training_days=list(DAYS), fit_days=list(DAYS[:239]),
                    embargo_days=list(DAYS[239:240]), selection_days=list(DAYS[240:]),
                    feature_cols=list(FEATURE_COLUMNS), half_life_days='inf',
                    reference_market=None, recalibrate_p3=False)
    if any(plan.get(k) != v for k, v in expected.items()):
        raise ValueError('fixed Development300 training contract mismatch')
    for key in ('baseline_bundle', 'label_config', 'p3', 'original_panel'):
        item = plan[key]
        if sha(item['path']) != item['sha256']:
            raise ValueError('bound artifact changed: ' + key)
    p3 = json.loads(Path(plan['p3']['path']).read_text())
    meta = p3.get('metadata', {})
    if (p3.get('schema_version') != 'narrowgate_p3_touch_calibration.v4'
            or meta.get('fit_days') != list(TRAIN_DAYS)
            or meta.get('queue_included') is not False
            or meta.get('event_type') != 'touch'
            or meta.get('distance_unit') != 'USDC_per_BTC'
            or meta.get('quote_tick_size') != 0.1
            or meta.get('horizon_s') != 10.0):
        raise ValueError('original frozen P3 contract changed')
    if delivery_identity(meta['plan']['observation_profile']) != delivery_identity(plan['observation_profile']):
        raise ValueError('original P3/training delivery differs')
    if [x['day'] for x in plan['days']] != list(DAYS):
        raise ValueError('training sources must cover all 300 dates exactly once')
    for row in plan['days']:
        expected_days = [row['day']] if row['day'] == DAYS[0] else [
            (date.fromisoformat(row['day']) - timedelta(days=1)).isoformat(), row['day']]
        if [Path(x['path']).name for x in row['source_bundles']] != expected_days:
            raise ValueError('causal prior-day source context mismatch')
        for item in row['source_bundles']:
            if sha(Path(item['path']) / 'manifest.json') != item['sha256']:
                raise ValueError('fact manifest changed')
    return p3


def day_plan(plan, row):
    if row['day'] not in DAYS:
        raise ValueError('Final or undeclared day cannot enter training')
    start = pd.Timestamp(row['day'], tz='UTC').value
    return dict(market_id='binance_futures:perpetual:BTCUSDC',
                source_bundles=row['source_bundles'],
                observation_profile=plan['observation_profile'],
                start_ns=start - (120 * 10**9 if row['day'] != DAYS[0] else 0),
                end_ns=start + 86400 * 10**9)


def project_training_inputs(plan, output, *, progress=None, fast=True):
    """Project the unmodified stream without writing unused depth copies."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    bundles = []
    for item in plan['source_bundles']:
        path = Path(item['path'])
        if sha(path / 'manifest.json') != item['sha256'] or path in bundles:
            raise ValueError('source binding changed or duplicated')
        bundles.append(path)
    profile = ObservationProfile(**plan['observation_profile'])
    profile.require_market(plan['market_id'])
    _require_observation_scenario(plan, profile)
    _require_market_source_bundles(bundles, plan['market_id'])
    stream = PublicInputStream(bundles, profile=profile, start_ns=plan['start_ns'],
        end_ns=plan['end_ns'], market_id=plan['market_id'], input_contract_id=INPUT_CONTRACT,
        incremental_top_cache=fast, fast_fact_decode=fast)
    common = {b'input_contract_id': INPUT_CONTRACT.encode(),
              b'observation_contract_id': CONTRACT.encode(), b'feature_contract_id': FEATURE_CONTRACT.encode()}
    schemas = {
        'features': pa.schema([('cutoff_ns', pa.int64()), ('max_dependency_ready_ns', pa.int64())]
            + [(n, pa.float64()) for n in EXECUTION_FEATURE_NAMES]
            + [('validity_mask', pa.list_(pa.bool_())), ('source_ages_ns', pa.map_(pa.string(), pa.int64()))], metadata=common),
        'bars': pa.schema([(n, pa.int64()) for n in ('start_ns', 'end_ns', 'ready_ns', 'last_trade_ns')]
            + [(n, pa.string()) for n in ('coverage', 'open', 'high', 'low', 'close', 'volume', 'turnover',
                'buy_volume', 'sell_volume', 'buy_turnover', 'sell_turnover')]
            + [(n, pa.int64()) for n in ('individual_count', 'native_packet_count', 'buy_count', 'sell_count', 'observed_event_count')], metadata=common),
        'outcome_bars': pa.schema([('start_ns', pa.int64()), ('end_ns', pa.int64()), ('coverage', pa.string())]
            + [(n, pa.string()) for n in ('open', 'high', 'low', 'close', 'volume', 'turnover')]
            + [('individual_count', pa.int64())], metadata={**common,
                b'purpose': b'offline_outcome_only_not_strategy_visible', b'clock_policy': profile.clock_policy.encode()}),
    }
    buffers = {k: [] for k in schemas}
    counts = dict.fromkeys(schemas, 0)
    writers = {k: pq.ParquetWriter(output / (k + '.parquet'), v, compression='zstd') for k, v in schemas.items()}
    outcomes = ExchangeOutcomeBars(plan['start_ns'], profile.trade_coverage)

    def write(name, row):
        buffers[name].append({k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()})
        counts[name] += 1
        if len(buffers[name]) >= 4096:
            writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=schemas[name]))
            buffers[name].clear()

    try:
        for tick in stream:
            for row in outcomes.advance(tick.now_ns, tick.exchange_events):
                write('outcome_bars', row)
            for bar in tick.bars:
                write('bars', asdict(bar))
            f = tick.feature_frame
            if f is not None:
                write('features', dict(cutoff_ns=f.cutoff_ns, max_dependency_ready_ns=f.max_dependency_ready_ns,
                    **{k: float(v) if v is not None else None for k, v in f.values},
                    validity_mask=[v for _, v in f.validity_mask], source_ages_ns=list(f.source_ages_ns)))
                if progress and counts['features'] % 360 == 0:
                    progress(tick.now_ns, counts['features'])
        for row in outcomes.advance(plan['end_ns']):
            write('outcome_bars', row)
        for name in schemas:
            if buffers[name]:
                writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=schemas[name]))
    finally:
        for writer in writers.values():
            writer.close()
    result = dict(schema='narrowgate.f03.training_projection.v1', visibility='local_only_do_not_publish',
                  plan=plan, counts=counts, stats=stream.stats, depth_retained=False,
                  files={k: dict(file=k + '.parquet', sha256=sha(output / (k + '.parquet'))) for k in schemas})
    save_private_json(output / 'manifest.json', result)
    return result


def convert_original_labels(frame, metadata):
    """Explicit offline rename of retained labels; no numerical conversion."""
    mapping = {}
    for head, meta in metadata.items():
        if meta['representation_source']['index_values_changed'] or meta['representation_source']['sample_weights_changed']:
            raise ValueError('original label representation changed')
        old = meta['migration_source']['target_name']
        for prefix in ('label_', 'label_outcome_end_'):
            mapping[prefix + old] = prefix + head
    if not set(mapping) <= set(frame) or set(mapping.values()) & set(frame):
        raise ValueError('original schema must be converted exactly once')
    return frame.rename(columns=mapping).copy()


def purge_day(frame, day):
    if day not in DAYS or set(frame.index.strftime('%Y-%m-%d')) != {day}:
        raise ValueError('label day outside Development300')
    boundary = pd.Timestamp(day, tz='UTC') + pd.Timedelta(days=1)
    frame = frame.copy()
    for head, spec in ml_model.MODEL_SPECS.items():
        ends = pd.to_datetime(frame['label_outcome_end_' + head], utc=True)
        known = frame[spec[0]].notna()
        if (known & ends.notna() & (ends < frame.index)).any():
            raise ValueError('label outcome precedes decision')
        frame.loc[~(known & ends.notna() & (ends < boundary)), spec[0]] = np.nan
    return frame


def label_projection(root, day, config_path):
    root = Path(root)
    manifest = json.loads((root / 'manifest.json').read_text())
    for spec in manifest['files'].values():
        if sha(root / spec['file']) != spec['sha256']:
            raise ValueError('training projection payload changed')
    frames = pd.read_parquet(root / 'features.parquet')
    start = pd.Timestamp(day, tz='UTC').value
    frames = frames.loc[(frames.cutoff_ns >= start) & (frames.cutoff_ns < start + 86400 * 10**9)]
    visible = pd.read_parquet(root / 'bars.parquet')
    outcomes = pd.read_parquet(root / 'outcome_bars.parquet')
    outcomes.index = pd.to_datetime(outcomes.pop('start_ns'), unit='ns', utc=True)
    for column in ('open', 'high', 'low', 'close', 'volume', 'turnover'):
        outcomes[column] = pd.to_numeric(outcomes[column], errors='coerce')
    result = label_frames(frames, outcomes, config_path=config_path, visible_bars=visible)
    return purge_day(result, day)


def selection_contract(plan, panel_manifest, panel_path):
    validate_plan(plan)
    digest = sha(panel_path)
    policy = deepcopy(plan['baseline_head_metadata'][next(iter(ml_model.MODEL_SPECS))]['train_only_selection']['sample_weight_policy'])
    return ml_model.TrainOnlySelectionContract(
        schema_version='narrowgate_13_head_train_only_selection.v1', spec_path=str(panel_path), spec_sha256=digest,
        source_authority='tardis_public_input', fit_days=tuple(plan['fit_days']), embargo_days=tuple(plan['embargo_days']),
        selection_days=tuple(plan['selection_days']), refit_days=DAYS,
        feature_manifest_sha256=digest, feature_dag_sha256=panel_manifest['feature_dag_sha256'],
        source_manifest_sha256=digest, train_source_identity_sha256=panel_manifest['train_source_identity_sha256'],
        sample_weight_policy=policy, feature_cols=FEATURE_COLUMNS)


def fit_bundle(plan, panel_root, output, *, progress=None):
    """One fixed H=inf bundle, 13 selection fits and 13 full-300 refits."""
    panel_root, output = Path(panel_root), Path(output)
    manifest_path = panel_root / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    if manifest['training_days'] != list(DAYS) or sha(panel_root / 'dataset_train.parquet') != manifest['panel_sha256']:
        raise ValueError('complete bound Development300 panel required')
    if output.exists():
        raise FileExistsError('existing model bundle requires inspection, not refitting')
    contract = selection_contract(plan, manifest, manifest_path)
    full = pd.read_parquet(panel_root / 'dataset_train.parquet')
    fit, selection, refit = ml_model.split_train_only_selection(full, contract)
    identity = dict(feature_manifest_path=str(manifest_path), feature_manifest_sha256=sha(manifest_path),
        feature_dag_id=FEATURE_CONTRACT, feature_dag_sha256=manifest['feature_dag_sha256'], feature_semantics_version=1,
        feature_panel_split={'train': list(DAYS)}, feature_timestamp_semantics='feature_ready_index',
        feature_cutoff_semantics='feature_ready_index', feature_sampling_interval_ms=10000,
        input_contract_id=INPUT_CONTRACT, observation_contract_id=CONTRACT, feature_contract_id=FEATURE_CONTRACT,
        label_contract_id=manifest['label_contract_id'], split_manifest_id=SPLIT_ID,
        public_input_contract={'decision_time_semantics': 'feature_ready_index'},
        volatility_unit_contract=absolute_price_variance_unit_contract('BTCUSDC'),
        feature_label_quote_calibration=plan['p3'], execution_trade_count_unit='individual_execution',
        reference_trade_count_unit='unavailable', reference_trade_symbol=None)
    output.mkdir(parents=True, exist_ok=False)
    reports = {}
    for head, spec in ml_model.MODEL_SPECS.items():
        if progress:
            progress(head, len(reports))
        for phase, frame in [('fit', fit), ('selection', selection), ('refit', refit)]:
            x, y, _, _, report = ml_model.prepare_time_weighted_xy(frame, head, contract, phase=phase)
            if spec[3] and set(y.unique()) != {0., 1.}:
                raise ValueError('binary class support missing: ' + head + '/' + phase)
            if not np.isfinite(x.to_numpy()).any(axis=0).all():
                raise ValueError('unsupported predictor: ' + head + '/' + phase)
            del x, y
        params = deepcopy(plan['baseline_head_metadata'][head]['selection_fit_params'])
        params['n_jobs'] = plan['training_threads']
        model, meta = ml_model.train_one(head, fit, selection, params_override=params,
            refit_df=refit, selection_contract=contract, feature_identity=identity)
        meta.update(training_experiment_id=CANDIDATE, promotion_authority='research_only',
                    parent_model=plan['baseline_bundle'], previous_use=plan['previous_use'])
        model.booster_.save_model(str(output / (head + '.txt')))
        save_private_json(output / (head + '_meta.json'), meta)
        reports[head] = dict(best_iteration=meta['best_iteration'],
            selection_training_rows=meta['selection_training_rows'], selection_validation_rows=meta['selection_validation_rows'],
            final_training_rows=meta['final_training_rows'], per_head_time_weighting=meta['per_head_time_weighting'])
        del model
    bundle = publish_model_contract(output, identity, list(ml_model.MODEL_SPECS), selection_contract=contract.to_metadata())
    save_private_json(output / 'training-acceptance.json', dict(visibility='local_only_do_not_publish',
        status='FITTED_MODEL_RUNTIME_AND_ECONOMIC_ADMISSION_PENDING', heads=reports,
        selection_fits=13, refits=13, training_days=300, final_read_during_fit=False,
        bundle_sha256=sha(output / 'public_input_model.json')))
    return bundle
