"""One frozen F05-risk input to the existing quote-widen execution action.

This is a research adapter, not an action-value estimator or live policy.
The model estimates observed baseline-policy risk; widening is a fixed existing
mechanic with its own full-path economics, not a counterfactual prediction.
"""

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from data.feature_cursor import FeatureCursor
from .quote_ev import QuoteEVModel


def _digest(path):
    with Path(path).open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def freeze_training_risk_thresholds(panel, model_dir, fit_receipt, *, quantile=0.9):
    """Fix side cutoffs solely from admitted training-support predictions."""
    if (quantile != 0.9 or panel.attrs.get('training_admitted') is not True
            or panel.attrs.get('support_role') != 'training'
            or panel.attrs.get('training_contract_id') != fit_receipt['contract_sha256']
            or panel.attrs.get('parent_manifests') != fit_receipt['parents']):
        raise ValueError('frozen training-only upper-decile risk rule required')
    identity = fit_receipt['input_identity']
    if panel.attrs['input_manifest_id'] != identity['source_manifest_sha256']:
        raise ValueError('risk calibration is not from the fitted training collection')
    intervals = panel.attrs.get('training_intervals_ns')
    if (not intervals or panel.empty or not {'decision_ns', 'actual_outcome_end_ns',
                                              'input_manifest_id'}.issubset(panel.columns)):
        raise ValueError('risk calibration lacks exact training-time support')
    expected_parents = {row['input_manifest_id'] for row in fit_receipt['parents']}
    if set(panel.input_manifest_id) != expected_parents:
        raise ValueError('risk calibration includes foreign or missing parents')
    if len(intervals) != len(fit_receipt['parents']):
        raise ValueError('risk calibration parent/interval cardinality mismatch')
    eligible_time = np.zeros(len(panel), dtype=bool)
    for parent, (start, end) in zip(fit_receipt['parents'], intervals, strict=True):
        eligible_time |= (panel.input_manifest_id.to_numpy() == parent['input_manifest_id']) & (
            panel.decision_ns.to_numpy() >= start) & (panel.decision_ns.to_numpy() < end) & (
            panel.actual_outcome_end_ns.to_numpy() >= start) & (panel.actual_outcome_end_ns.to_numpy() < end)
    if not eligible_time.all():
        raise ValueError('risk calibration contains out-of-training outcome support')
    cutoffs, counts = {}, {}
    for side in ('bid', 'ask'):
        model = QuoteEVModel.load(Path(model_dir)/side, side=side, input_identity=identity)
        rows = panel[(panel.side == side) & (panel.horizon_ns == 30_000_000_000)]
        columns = model.fill_prob_features
        if columns != model.extreme_adverse_features:
            raise ValueError('risk heads must share the frozen feature matrix')
        matrix = rows[columns].to_numpy(dtype=float)
        if len(matrix) < 100 or np.isinf(matrix).any():
            raise ValueError('insufficient finite training opportunities for risk calibration')
        pfill = np.asarray(model.fill_prob_model.predict(matrix)).reshape(-1)
        padverse = np.asarray(model.extreme_adverse_model.predict(matrix)).reshape(-1)
        risks = pfill * padverse
        if (not np.isfinite(risks).all() or (risks < 0).any() or (risks > 1).any()
                or len(np.unique(risks)) < 2):
            raise ValueError('unusable training-only risk distribution')
        action_side = 'BUY' if side == 'bid' else 'SELL'
        cutoffs[action_side] = float(np.quantile(risks, quantile))
        counts[action_side] = {'training_opportunities': len(risks),
            'at_or_above_cutoff': int((risks >= cutoffs[action_side]).sum()),
            'distinct_scores': int(len(np.unique(risks)))}
    return cutoffs, counts


class F05RiskWidenPolicy:
    """Duck-compatible replay consumer: fixed upper-decile risk -> existing widen.

    It runs inside the original executor, so positions, orders, queue and RNG
    are independently regenerated. Its predictions never observe future fills.
    """

    def __init__(self, root, model_dir, contract_path):
        self.cursor = FeatureCursor(root)
        self.model_dir = Path(model_dir)
        self.contract_path = Path(contract_path)
        self.contract = json.loads(self.contract_path.read_text())
        c = self.contract
        if (c.get('schema') != 'f05_existing_quote_widen_candidate.v1'
                or c.get('action') != 'existing_public_quote_widen'
                or c.get('risk_score') != 'p_fill_times_p_extreme_adverse_given_fill_30000ms'
                or c.get('threshold_source') != 'training_predicted_risk_q90'
                or c.get('widen_mult') != 1.25
                or c.get('input_manifest_id') != self.cursor.input_manifest_id
                or set(c.get('side_thresholds', {})) != {'BUY', 'SELL'}
                or type(c.get('max_age_ns')) is not int or c['max_age_ns'] <= 0
                or type(c.get('trace_limit')) is not int or c['trace_limit'] < 0):
            raise ValueError('explicit frozen F05/existing-widen evaluation contract required')
        if any(not isinstance(v, (float, int)) or not 0 < v <= 1 or not math.isfinite(v)
               for v in c['side_thresholds'].values()):
            raise ValueError('training-only risk cutoffs must be finite probabilities')
        fit_path = self.model_dir/'fit-receipt.json'
        if _digest(fit_path) != c['fit_receipt_sha256']:
            raise ValueError('fitted bundle receipt identity mismatch')
        fit = json.loads(fit_path.read_text())
        if fit['contract_sha256'] != c['training_contract_sha256']:
            raise ValueError('candidate is not bound to the frozen training contract')
        for relative, expected in fit['model_files'].items():
            if _digest(self.model_dir/relative) != expected:
                raise ValueError('fitted model file identity mismatch')
        self.models = {('BUY' if side == 'bid' else 'SELL'): QuoteEVModel.load(self.model_dir/side,
            side=side, input_identity=fit['input_identity']) for side in ('bid', 'ask')}
        for model in self.models.values():
            if (model.input_identity['input_contract_id'] != self.cursor.bundle.manifest['input_contract_id']
                    or model.input_identity['observation_contract_id']
                       != self.cursor.bundle.manifest['observation_contract_id']
                    or model.input_identity['feature_contract_id']
                       != self.cursor.bundle.manifest['feature_contract_id']):
                raise ValueError('evaluation feature/observation contract differs from fitted model')
        self.started = False
        self.last_ns = None
        self.current = None
        self.current_risks = None
        self.current_cutoff = None
        self.cached_frame_cutoff = None
        self.cached_risks = None
        self.receipts = []
        self.counts = {'decisions': 0, 'evaluated_sides': 0, 'missing_sides': 0,
                       'widen_requested': 0, 'resolved_sides': 0, 'trace_dropped': 0,
                       'model_frame_predictions': 0}

    def start(self, manifest_id):
        if self.started or manifest_id != self.cursor.input_manifest_id:
            raise ValueError('fresh F05 policy instance and matching input required')
        self.started = True

    def decide(self, decision_ns):
        if not self.started or (self.last_ns is not None and decision_ns < self.last_ns):
            raise ValueError('F05 policy unstarted or decision clock regressed')
        if decision_ns == self.last_ns:
            return self.current
        actions = {side: {'action': 'default', 'spread_mult': 1.0} for side in ('BUY', 'SELL')}
        try:
            frame = self.cursor.at(decision_ns, max_age_ns=self.contract['max_age_ns'])
        except ValueError as exc:
            if 'missing or stale causal feature context' not in str(exc):
                raise
            self.counts['missing_sides'] += 2
            self.counts['decisions'] += 1
            self.last_ns, self.current = decision_ns, actions
            self.current_risks, self.current_cutoff = {'BUY': None, 'SELL': None}, None
            return actions
        if frame.cutoff_ns == self.cached_frame_cutoff:
            risks = dict(self.cached_risks)
        else:
            risks = {}
            for side, model in self.models.items():
                self.counts['model_frame_predictions'] += 1
                try:
                    prediction = model.predict_frame(frame, decision_ns=decision_ns)
                except ValueError as exc:
                    if 'nonfinite quote EV' not in str(exc):
                        raise
                    risks[side] = None
                    continue
                risk = float(prediction.fill_and_extreme_adverse_probability_30000ms)
                if risk is not None and not math.isfinite(risk):
                    risk = None
                if risk is not None and not 0 <= risk <= 1:
                    raise ValueError('F05 risk is not a probability')
                risks[side] = risk
            self.cached_frame_cutoff, self.cached_risks = frame.cutoff_ns, dict(risks)
        for side, risk in risks.items():
            if risk is None:
                self.counts['missing_sides'] += 1
                continue
            self.counts['evaluated_sides'] += 1
            if risk >= self.contract['side_thresholds'][side]:
                actions[side] = {'action': 'widen', 'spread_mult': 1.25}
                self.counts['widen_requested'] += 1
        self.counts['decisions'] += 1
        self.last_ns, self.current = decision_ns, actions
        self.current_risks, self.current_cutoff = risks, frame.cutoff_ns
        return actions

    @staticmethod
    def continuation(action, *, enabled, updated, has_order, force_update):
        # This candidate only requests widening; no fabricated keep/cancel.
        if action not in ('default', 'widen'):
            raise ValueError('F05 candidate cannot change order-continuation semantics')
        return enabled, updated

    def resolved(self, decision_ns, side, *, action, price, quantity, route_due):
        self.counts['resolved_sides'] += 1
        if len(self.receipts) >= self.contract['trace_limit']:
            self.counts['trace_dropped'] += 1
            return
        self.receipts.append(dict(decision_ns=decision_ns, feature_cutoff_ns=self.current_cutoff,
            side=side, predicted_extreme_risk=self.current_risks[side],
            requested=deepcopy(self.current[side]), resolved_intent=action,
            price=price, quantity=quantity, route_due=route_due))

    def report(self):
        return dict(contract=deepcopy(self.contract), counts=dict(self.counts),
            decisions=deepcopy(self.receipts),
            receipt_semantics='resolved_intent_not_ack_or_fill', native_queue_parity='not_proven')
