"""Live E/C adapter: immutable visible inputs, shared scoring, no transport."""
from collections import Counter
import json
from pathlib import Path

from strategy.risk_selection import (
    PREFILL_FEATURE_UNITS, PendingExposure, RiskSelectionCandidate,
    RiskSelectionObservation, RiskSelectionPolicy, evaluate_risk_selection,
    prefill_features,
)


class LivePrefillSelection:
    def __init__(self, *, mode, scope, policy=None):
        if mode not in {'off', 'shadow', 'enforce'} or scope not in {'E', 'C', 'EC'}:
            raise ValueError('explicit prefill mode and E/C scope required')
        if mode != 'off':
            if policy is None or policy.selection_scope != 'visible_inventory':
                raise ValueError('prefill requires visible-inventory policy')
            required = {f'{k}:{s}' for k in scope for s in ('BUY', 'SELL')}
            if not required <= policy.models.keys():
                raise ValueError('prefill required model surface missing')
            if any(k not in PREFILL_FEATURE_UNITS or unit != PREFILL_FEATURE_UNITS[k]
                   for k, (unit, _, _) in policy.features.items()):
                raise ValueError('prefill feature units do not match live support')
            if {'side_trade_imbalance_5000ms', 'adverse_mid_change_5000ms_bps'} & policy.features.keys():
                raise ValueError('exact 5000ms live feature support is not bound')
        self.mode, self.scope, self.policy = mode, scope, policy
        self.counts = Counter()
        self.last_decisions = ()

    @classmethod
    def from_config(cls, cfg):
        mode = cfg.strategy.prefill_ec_mode
        if mode == 'off':
            return cls(mode=mode, scope=cfg.strategy.prefill_ec_scope)
        payload = json.loads(Path(cfg.strategy.prefill_ec_policy_path).read_text())
        if payload.get('training_label_contract') != 'prefill_ec_accounting_30000ms.v1':
            raise ValueError('prefill requires current bounded authoritative labels')
        expected = {k: getattr(cfg.strategy, k) for k in (
            'eta_inventory', 'a_spread', 'risk_per_order', 'inventory_reference_qty')}
        expected['asym_strength'] = cfg.ml.asym_strength
        if payload.get('baseline_coefficients') != expected:
            raise ValueError('prefill policy baseline coefficients differ')
        return cls(mode=mode, scope=cfg.strategy.prefill_ec_scope,
                   policy=RiskSelectionPolicy.from_dict(payload))

    def evaluate(self, *, decision_ns, market_ready_ns, inventory, orders, intents,
                 tick_size, lot_size, best_bid, best_ask, bid_quantity, ask_quantity,
                 variance):
        if self.mode == 'off':
            return {}
        pending = tuple(PendingExposure(o['id'], o['side'], o['remaining']) for o in orders)
        observation = RiskSelectionObservation(decision_ns, market_ready_ns, inventory,
            pending, selection_scope='visible_inventory')
        candidates = []
        for row in intents:
            if row['kind'] not in self.scope:
                continue
            other = [o for o in orders if o['id'] != row['order_id']]
            features = prefill_features(kind=row['kind'], side=row['side'],
                decision_ns=decision_ns, market_ready_ns=market_ready_ns,
                tick_size=tick_size, lot_size=lot_size, best_bid=best_bid, best_ask=best_ask,
                bid_quantity=bid_quantity, ask_quantity=ask_quantity, order_price=row['price'],
                visible_inventory=inventory,
                other_same_side_pending=sum(o['remaining'] for o in other if o['side'] == row['side']),
                opposite_side_pending=sum(o['remaining'] for o in other if o['side'] != row['side']),
                absolute_price_variance_rate=variance, local_submit_ns=row['submit_ns'])
            candidates.append(RiskSelectionCandidate(row['id'], row['kind'], row['side'],
                row['quantity'], 'POST' if row['kind'] == 'E' else 'KEEP',
                order_id=row['order_id'], features=features))
        self.last_decisions = evaluate_risk_selection(observation, candidates, self.policy)
        for decision in self.last_decisions:
            self.counts[f'{decision.kind}:{decision.side}:{decision.action}'] += 1
            if decision.out_of_scope:
                self.counts[f'fallback:{decision.reason}'] += 1
        return ({d.side: d.action for d in self.last_decisions}
                if self.mode == 'enforce' else {})
