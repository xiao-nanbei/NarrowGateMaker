"""F03-method two-market direction training; no scheduling or live activation.

Original labels are converted explicitly offline, never via a runtime alias.
Reference preparation projects the shared input stream without retaining depth
or bar tables. It does not change delivery, feature timers or feature formulas.
"""
from copy import deepcopy
from dataclasses import fields
import hashlib
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data.observation import CONTRACT, EXECUTION_FEATURE_NAMES, FEATURE_CONTRACT
from data.runtime import (
    ObservationProfile, PublicInputStream, _require_market_source_bundles,
    _require_observation_scenario,
)
from data.tardis_input import CONTRACT as INPUT_CONTRACT
from research.families.f03_causal_13_head.ml_model import (
    TrainOnlySelectionContract, prepare_time_weighted_xy, split_train_only_selection,
)
from research.families.f04_external_market_alpha.tardis_direction import (
    REFERENCE_FEATURE_PREFIX, REFERENCE_MARKET,
)

HEAD = 'touch_conditioned_up_probability_10000ms'
LABEL = 'label_' + HEAD
END = 'label_outcome_end_' + HEAD


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def feature_record(frame):
    return dict(cutoff_ns=frame.cutoff_ns,
                max_dependency_ready_ns=frame.max_dependency_ready_ns,
                **{k: float(v) if v is not None else None for k, v in frame.values},
                validity_mask=[v for _, v in frame.validity_mask],
                source_ages_ns=list(frame.source_ages_ns))


def project_reference(plan, output, *, progress=None, fast=False):
    """Create only a feature projection using the unmodified shared stream."""
    output = Path(output)
    if output.exists():
        raise FileExistsError('existing projection needs explicit receipt verification')
    if plan['market_id'] != REFERENCE_MARKET:
        raise ValueError('reference market must be BTCUSDT')
    bundles = []
    for item in plan['source_bundles']:
        p = Path(item['path'])
        if sha(p/'manifest.json') != item['sha256'] or p in bundles:
            raise ValueError('reference source identity changed')
        bundles.append(p)
    profile = ObservationProfile(**plan['observation_profile'])
    profile.require_market(REFERENCE_MARKET)
    _require_observation_scenario(plan, profile)
    _require_market_source_bundles(bundles, REFERENCE_MARKET)
    stream = PublicInputStream(bundles, profile=profile, start_ns=plan['start_ns'],
        end_ns=plan['end_ns'], market_id=REFERENCE_MARKET, input_contract_id=INPUT_CONTRACT,
        incremental_top_cache=fast, fast_fact_decode=fast)
    schema = pa.schema([('cutoff_ns', pa.int64()), ('max_dependency_ready_ns', pa.int64())]
        + [(n, pa.float64()) for n in EXECUTION_FEATURE_NAMES]
        + [('validity_mask', pa.list_(pa.bool_())), ('source_ages_ns', pa.map_(pa.string(), pa.int64()))],
        metadata={b'input_contract_id': INPUT_CONTRACT.encode(),
                  b'observation_contract_id': CONTRACT.encode(),
                  b'feature_contract_id': FEATURE_CONTRACT.encode()})
    output.parent.mkdir(parents=True, exist_ok=True)
    count, rows = 0, []
    with pq.ParquetWriter(output, schema, compression='zstd') as writer:
        for tick in stream:
            if tick.feature_frame is None:
                continue
            rows.append(feature_record(tick.feature_frame))
            count += 1
            if len(rows) >= 360:
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                rows.clear()
                if progress:
                    progress(tick.now_ns, count)
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    return dict(schema='narrowgate.f04.reference_feature_projection.v1',
        visibility='local_only_do_not_publish', plan=plan, rows=count,
        file=output.name, size=output.stat().st_size, sha256=sha(output), stats=stream.stats,
        economic_replay=False, depth_and_bars_retained=False, accelerated_decode=fast)


def causal_reference_rows(reference, decisions, columns, max_age_ns):
    """Vectorized exact backwards as-of with per-value validity; never fill ahead."""
    clocks = reference['cutoff_ns'].to_numpy(dtype=np.int64)
    if len(clocks) == 0 or np.any(np.diff(clocks) <= 0):
        raise ValueError('ordered nonempty reference clocks required')
    decisions = np.asarray(decisions, dtype=np.int64)
    if len(decisions) and np.any(np.diff(decisions) <= 0):
        raise ValueError('ordered unique execution decisions required')
    if type(max_age_ns) is not int or max_age_ns < 0:
        raise ValueError('nonnegative integer reference age required')
    deps = reference['max_dependency_ready_ns']
    if (deps.notna() & (deps > reference['cutoff_ns'])).any():
        raise ValueError('future reference dependency')
    masks = np.asarray(reference['validity_mask'].tolist(), dtype=bool)
    if masks.shape != (len(reference), len(EXECUTION_FEATURE_NAMES)):
        raise ValueError('reference validity schema differs')
    indexes = np.searchsorted(clocks, decisions, side='right') - 1
    safe = np.maximum(indexes, 0)
    present = (indexes >= 0) & (decisions - clocks[safe] <= max_age_ns)
    data = {}
    for name in columns:
        position = EXECUTION_FEATURE_NAMES.index(name)
        values = reference[name].to_numpy(dtype=float)[safe].copy()
        values[~present | ~masks[safe, position]] = np.nan
        if np.isinf(values).any():
            raise ValueError('nonfinite nonmissing reference values')
        data[REFERENCE_FEATURE_PREFIX + name] = values
    report = dict(decisions=len(decisions), missing_context=int((~present).sum()),
        finite_per_feature={k: int(np.isfinite(v).sum()) for k, v in data.items()},
        future_selected=0, no_favorable_row_filter=True)
    return pd.DataFrame(data, index=pd.to_datetime(decisions, utc=True)), report


def convert_original_direction(frame, metadata):
    """One explicit immutable-source panel conversion; not an app compatibility API."""
    if (metadata['migration_source']['target_name'] != 'dir_10s'
            or metadata['name'] != HEAD
            or metadata['representation_source']['index_values_changed']
            or metadata['representation_source']['sample_weights_changed']):
        raise ValueError('unsupported original direction representation')
    names = {'label_dir_10s': LABEL, 'label_outcome_end_dir_10s': END}
    if not set(names) <= set(frame) or set(names.values()) & set(frame):
        raise ValueError('expected original label schema exactly once')
    return frame.rename(columns=names).copy()


def training_contract(metadata, feature_cols):
    value = deepcopy(metadata['train_only_selection'])
    value = {f.name: value[f.name] for f in fields(TrainOnlySelectionContract)}
    for key in ('fit_days', 'embargo_days', 'selection_days', 'refit_days'):
        value[key] = tuple(value[key])
    value['feature_cols'] = tuple(feature_cols)
    return TrainOnlySelectionContract(**value)


def prepared_phases(frame, metadata, feature_cols):
    contract = training_contract(metadata, feature_cols)
    parts = split_train_only_selection(frame, contract)
    out = {}
    expected = dict(fit=metadata['selection_training_rows'],
                    selection=metadata['selection_validation_rows'],
                    refit=metadata['final_training_rows'])
    for phase, part in zip(('fit', 'selection', 'refit'), parts, strict=True):
        item = prepare_time_weighted_xy(part, HEAD, contract, phase=phase)
        if len(item[0]) != expected[phase]:
            raise ValueError('baseline eligible support changed: ' + phase)
        if set(item[1].unique()) != {0., 1.}:
            raise ValueError('binary class support missing: ' + phase)
        out[phase] = item
    return out


def fit_direction(frame, metadata, feature_cols, output, *, provenance):
    """One baseline-method selection fit and one refit; no Final read or search."""
    output = Path(output)
    if output.exists():
        raise FileExistsError('candidate model already exists; do not refit implicitly')
    phases = prepared_phases(frame, metadata, feature_cols)
    for name in feature_cols:
        if not np.isfinite(phases['refit'][0][name]).any():
            raise ValueError('zero supported feature: ' + name)
    params = deepcopy(metadata['selection_fit_params'])
    train, selection, refit = (phases[x] for x in ('fit', 'selection', 'refit'))
    candidate = lgb.LGBMClassifier(**params)
    candidate.fit(train[0], train[1], sample_weight=train[2],
        eval_set=[(selection[0], selection[1])], eval_sample_weight=[selection[2]],
        callbacks=[lgb.log_evaluation(0), lgb.early_stopping(100, verbose=False)])
    iteration = candidate.best_iteration_ or params['n_estimators']
    final_params = dict(params, n_estimators=iteration)
    model = lgb.LGBMClassifier(**final_params)
    model.fit(refit[0], refit[1], sample_weight=refit[2])
    output.mkdir(parents=True, exist_ok=False)
    target = output/(HEAD+'.txt')
    model.booster_.save_model(str(target))
    result = dict(schema='narrowgate.f04.f03_method_direction.v1',
        visibility='local_only_do_not_publish', name=HEAD, feature_cols=list(feature_cols),
        best_iteration=int(iteration), model_sha256=sha(target),
        selection_fit_params=params, refit_fit_params=final_params,
        baseline_method_metadata=metadata, provenance=provenance,
        support={k: v[4] for k, v in phases.items()},
        selection_auc=float(candidate.best_score_['valid_0']['auc']),
        feature_availability={k: float(np.isfinite(refit[0][k]).mean()) for k in feature_cols},
        new_fits=1, refits=1, external_panel_read=False, live_authority=False)
    (output/'manifest.json').write_text(json.dumps(result, indent=2, sort_keys=True)+'\n')
    return result
