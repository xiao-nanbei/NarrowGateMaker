"""Explicit multi-account admission of audited observed outcomes.

Engineering artifacts stay immutable. A frozen experiment binds each parent,
the behavior policy, label semantics and half-open account support before a
new training view is built. Collection identity never replaces row lineage.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from .logged_outcomes import TARGET
from .public_input import build_opportunity_panel, train_opportunity_models
from .quote_ev import QuoteEVModel


def digest(path):
    with Path(path).open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def canonical_id(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def account_support(contract):
    """Validate chronological support without reading any evaluation outcomes."""
    if contract['target'] != TARGET or contract['post_fill_horizons_ms'] != [1000, 5000, 30000]:
        raise ValueError('observed lifecycle/post-fill target required')
    if not contract['evaluation']:
        raise ValueError('nonempty evaluation support required')
    evaluation_start = min(pd.Timestamp(r['start_utc']).value for r in contract['evaluation'])
    evaluation_ids = {r['index'] for r in contract['evaluation']}
    if len(evaluation_ids) != len(contract['evaluation']):
        raise ValueError('unique evaluation accounts required')
    intervals, seen = [], set()
    for row in contract['training']:
        start = pd.Timestamp(row['start_utc']).value
        end = pd.Timestamp(row['end_exclusive_utc']).value
        if (start >= end or end > evaluation_start or row['index'] in seen
                or (intervals and start < intervals[-1][1])):
            raise ValueError('unique ordered training accounts before evaluation required')
        seen.add(row['index'])
        intervals.append([start, end])
    if not intervals or seen & evaluation_ids:
        raise ValueError('disjoint nonempty training/evaluation support required')
    return intervals, evaluation_start


def admit_account(root, artifacts, contract_path, index):
    """Build a new view from exact hash-bound raw artifacts, not a flag edit."""
    from models.replay.l2_journal import audit_l2_delivery

    root, artifacts, contract_path = map(Path, (root, artifacts, contract_path))
    contract = json.loads(contract_path.read_text())
    intervals, boundary = account_support(contract)
    rows = contract['training']
    position = next((i for i, row in enumerate(rows) if row['index'] == index), None)
    if position is None:
        raise ValueError('account is outside frozen training support')
    source = rows[position]
    launch = json.loads((artifacts/'launch.json').read_text())
    complete = json.loads((artifacts/'complete.json').read_text())
    if (complete['status'] != 'support_produced_not_fitted'
            or launch['contract_sha256'] != digest(contract_path)
            or launch['source_commit'] != contract['source_commit']
            or launch['model_manifest_sha256'] != contract['baseline']['model_manifest_sha256']
            or launch['input_manifest_sha256'] != source['input_manifest_sha256']
            or digest(root/'manifest.json') != source['input_manifest_sha256']
            or launch['shard']['shard_id'] != source['shard']):
        raise ValueError('frozen training parent/behavior identity mismatch')
    start, end = intervals[position]
    if (launch['shard']['start_ts_ms']*1_000_000 != start
            or launch['shard']['end_ts_ms_exclusive']*1_000_000 != end):
        raise ValueError('account boundary differs from training support')
    journal = json.loads((artifacts/'journal-receipt.json').read_text())
    audit_l2_delivery(journal['manifest'], journal['production'])
    for name in ('opportunities.parquet', 'raw-outcomes.parquet', 'non-quote-actions.parquet'):
        if digest(artifacts/name) != complete['files'][name]:
            raise ValueError('raw outcome artifact identity mismatch')
    opportunities = pd.read_parquet(artifacts/'opportunities.parquet')
    outcomes = pd.read_parquet(artifacts/'raw-outcomes.parquet')
    if (not outcomes.target.eq(TARGET).all()
            or not outcomes.input_manifest_id.eq(source['input_manifest_sha256']).all()
            or not opportunities.input_manifest_id.eq(source['input_manifest_sha256']).all()
            or not opportunities.decision_ns.between(start, end-1).all()):
        raise ValueError('target, source or opportunity time drift')
    if set(outcomes.horizon_ns.unique()) != {1_000_000_000, 5_000_000_000, 30_000_000_000}:
        raise ValueError('all declared horizons required')
    manifest = json.loads((root/'manifest.json').read_text())
    # Authorization follows all identity/target/support checks above. The
    # immutable raw file is never rewritten. Existing per-head purge remains.
    admitted = outcomes.copy()
    admitted['training_admitted'] = True
    admitted['training_contract_id'] = digest(contract_path)
    panel = build_opportunity_panel(root, opportunities, admitted,
        feature_columns=contract['feature_columns'], missing_policy=contract['missing_policy'],
        max_age_ns=manifest['plan']['observation_profile']['feature_period_ns'],
        training_boundary_ns=boundary, training_intervals_ns=[intervals[position]])
    panel['source_opportunity_id'] = panel.opportunity_id
    panel['opportunity_id'] = source['input_manifest_sha256'] + ':' + panel.opportunity_id.astype(str)
    panel.attrs['training_contract_id'] = digest(contract_path)
    panel.attrs['label_contract_id'] = canonical_id({'target': TARGET,
        'horizons_ms': contract['post_fill_horizons_ms'], 'contract': digest(contract_path)})
    panel.attrs['parent_receipt_sha256'] = digest(artifacts/'complete.json')
    return panel


def combine_training_panels(panels, contract_path):
    contract = json.loads(Path(contract_path).read_text())
    intervals, _ = account_support(contract)
    expected = [r['input_manifest_sha256'] for r in contract['training']]
    if len(set(expected)) != len(expected):
        raise ValueError('training accounts must have distinct parent manifests')
    if len(panels) != len(expected):
        raise ValueError('all frozen training accounts required')
    keys = ('input_contract_id', 'observation_contract_id', 'feature_contract_id',
            'feature_columns', 'missing_policy', 'training_contract_id', 'label_contract_id')
    for position, (panel, parent) in enumerate(zip(panels, expected, strict=True)):
        start, end = intervals[position]
        if (panel.attrs.get('training_admitted') is not True
                or panel.attrs.get('training_contract_id') != digest(contract_path)
                or panel.attrs.get('input_manifest_id') != parent
                or not panel.input_manifest_id.eq(parent).all()
                or panel.attrs.get('training_intervals_ns') != [[start, end]]
                or not panel.decision_ns.between(start, end-1).all()
                or not panel.actual_outcome_end_ns.between(start, end-1).all()
                or any(panel.attrs.get(k) != panels[0].attrs.get(k) for k in keys)):
            raise ValueError('training collection contains foreign or incompatible parent')
    attrs = {k: panels[0].attrs[k] for k in keys}
    result = pd.concat([p.copy().rename_axis(None) for p in panels], ignore_index=True)
    if result.duplicated(['opportunity_id', 'horizon_ns']).any():
        raise ValueError('duplicate training opportunity identity')
    parents = [{'input_manifest_id': p.attrs['input_manifest_id'],
                'parent_receipt_sha256': p.attrs['parent_receipt_sha256'],
                'outcome_support_counts': p.attrs['outcome_support_counts']} for p in panels]
    support_counts = {}
    for parent in parents:
        for key, counts in parent['outcome_support_counts'].items():
            total = support_counts.setdefault(key, {})
            for field, count in counts.items():
                total[field] = total.get(field, 0) + count
    result.attrs = {**attrs, 'input_manifest_id': canonical_id(parents),
        'parent_manifests': parents, 'training_intervals_ns': intervals,
        'outcome_support_counts': support_counts,
        'training_admitted': True, 'support_role': 'training',
        'label_units': 'maker_signed_bps_per_opportunity_not_net_pnl'}
    return result


def training_only_calibration(panel, contract):
    """Fit bucket representatives and the adverse cutoff on admitted rows only."""
    declared = contract['buckets']
    if declared['method'] != 'training_only_pooled_filled_markout_quantiles':
        raise ValueError('unsupported frozen bucket fit')
    observed = panel[(panel.fill_label == 1)
        & np.isfinite(panel.conditional_markout_bps)]
    values = observed.conditional_markout_bps.to_numpy(dtype=float)
    if not len(values) or panel.attrs.get('training_admitted') is not True:
        raise ValueError('nonempty admitted observed conditional marks required')
    quantiles = declared['edge_quantiles']
    if quantiles != [0.1, 0.3, 0.5, 0.7, 0.9]:
        raise ValueError('frozen five-edge calibration required')
    edges = np.quantile(values, quantiles).tolist()
    if any(a >= b for a, b in zip(edges, edges[1:], strict=False)):
        raise ValueError('nonunique markout edges; support expansion required')
    bins = np.digitize(values, edges)
    counts = np.bincount(bins, minlength=len(edges)+1)
    if (counts == 0).any():
        raise ValueError('empty markout bin; support expansion required')
    representatives = [float(np.mean(values[bins == index])) for index in range(len(counts))]
    adverse = panel[(panel.fill_label == 1) & (panel.horizon_ns == 30_000_000_000)
        & np.isfinite(panel.conditional_markout_bps)].conditional_markout_bps.to_numpy(dtype=float)
    if not len(adverse):
        raise ValueError('missing observed 30000ms markout for adverse threshold')
    threshold = float(np.quantile(adverse, 0.1))
    if not all(math.isfinite(v) for v in [*edges, *representatives, threshold]):
        raise ValueError('nonfinite training-only calibration')
    return dict(bucket_edges=edges, bucket_values=representatives,
                bucket_counts=counts.tolist(), adverse_threshold_bps=threshold,
                observed_conditional_marks=len(values), observed_30000ms_marks=len(adverse))


def preflight_head_support(panel, calibration):
    """Fail before any fit if any of the frozen ten targets has one class."""
    reports = {}
    for side in ('bid', 'ask'):
        rows = panel[panel.side == side]
        end = rows[rows.horizon_ns == 30_000_000_000]
        heads = [(f'{side}_fill_prob', end.fill_label.to_numpy())]
        for horizon in (1, 5, 30):
            part = rows[(rows.horizon_ns == horizon*1_000_000_000)
                & (rows.fill_label == 1)
                & np.isfinite(rows.conditional_markout_bps)]
            heads.append((f'{side}_fill_markout_bucket_{horizon}s', np.digitize(
                part.conditional_markout_bps.to_numpy(dtype=float), calibration['bucket_edges'])))
        observed_end = end[(end.fill_label == 1)
            & np.isfinite(end.conditional_markout_bps)]
        heads.append((f'{side}_extreme_adverse_given_fill',
            (observed_end.conditional_markout_bps.to_numpy(dtype=float)
             <= calibration['adverse_threshold_bps']).astype(int)))
        for name, target in heads:
            classes, counts = np.unique(target, return_counts=True)
            reports[name] = {'fit_rows': len(target),
                'classes': {str(cls): int(count) for cls, count in zip(classes, counts, strict=True)}}
            if len(classes) < 2:
                raise ValueError(f'{name} lacks two observed training classes')
    return reports


def fit_bound_training(panel, contract_path, output):
    """Fit and load exactly the frozen two-side, ten-head bundle create-only."""
    contract_path, output = Path(contract_path), Path(output)
    contract = json.loads(contract_path.read_text())
    intervals, boundary = account_support(contract)
    if (panel.attrs.get('training_contract_id') != digest(contract_path)
            or panel.attrs.get('training_intervals_ns') != intervals
            or len(panel.attrs.get('parent_manifests', [])) != len(intervals)
            or panel.input_manifest_id.isna().any()
            or set(panel.input_manifest_id.unique()) != {
                row['input_manifest_sha256'] for row in contract['training']}
            or contract['fit']['scheme'] != 'fixed_once_no_inner_selection_no_refit'
            or contract['fit']['heads_per_side'] != 5
            or contract['budget']['model_fit_calls'] != 10):
        raise ValueError('training panel or fixed ten-head budget differs from frozen contract')
    if output.exists():
        raise FileExistsError(output)
    calibration = training_only_calibration(panel, contract)
    head_support = preflight_head_support(panel, calibration)
    identity = {key: panel.attrs[key] for key in
        ('input_contract_id', 'observation_contract_id', 'feature_contract_id',
         'training_contract_id', 'label_contract_id')}
    identity['source_manifest_sha256'] = panel.attrs['input_manifest_id']
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.f05-ten-head-', dir=output.parent))
    receipts = {}
    for side in ('bid', 'ask'):
        receipts[side] = train_opportunity_models(panel, stage/side,
            input_identity=identity, feature_columns=contract['feature_columns'], side=side,
            missing_policy=contract['missing_policy'], training_boundary_ns=boundary,
            bucket_edges=calibration['bucket_edges'], bucket_values=calibration['bucket_values'],
            adverse_threshold_bps=calibration['adverse_threshold_bps'],
            parameters=contract['fit']['parameters'],
            num_boost_round=contract['fit']['num_boost_round'],
            training_intervals_ns=intervals)
        QuoteEVModel.load(stage/side, side=side, input_identity=identity)
        receipts[side]['model_directory'] = str(output/side)
    if sum(len(item['heads']) for item in receipts.values()) != 10:
        raise ValueError('incomplete F05 head set')
    receipt = dict(contract_sha256=digest(contract_path), input_identity=identity,
                   parents=panel.attrs['parent_manifests'],
                   calibration=calibration, head_support=head_support, sides=receipts,
                   model_files={str(p.relative_to(stage)): digest(p)
                                for p in stage.rglob('*') if p.is_file()},
                   economic_evaluation='not_run', previous_use=contract['previous_use'])
    (stage/'fit-receipt.json').write_text(json.dumps(receipt, indent=2, allow_nan=False))
    if output.exists():
        raise FileExistsError(output)
    os.rename(stage, output)
    return receipt
