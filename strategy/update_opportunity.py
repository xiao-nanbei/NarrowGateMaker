"""Output-only pre-price-throttle observation. Never changes an order intent.

Full action eligibility also needs downstream risk, pending and quantity checks;
the local qualification here is deliberately not called an executable fork.
"""
from collections import Counter
from decimal import Decimal, InvalidOperation


def old_price_support(cursor, side, price, active, frame):
    """Visible local evidence only; no exchange queue or economic inventory."""
    state = cursor.state
    reasons = []
    geometry = None
    if not active:
        return ['no_active_old_order'], geometry
    try:
        p = Decimal(str(price))
        valid_price = p.is_finite() and p > 0
    except InvalidOperation:
        valid_price = False
    if not valid_price:
        return ['missing_or_invalid_old_price'], geometry
    view = state.book
    if view is None:
        return ['no_visible_book'], geometry
    if not view.valid or not view.bids or not view.asks:
        return ['invalid_book'], geometry
    levels = view.bids if side == 'BUY' else view.asks
    best, worst = levels[0][0], levels[-1][0]
    geometry = dict(best=str(best), worst=str(worst), levels=len(levels),
                    old_minus_best=str(p-best), old_minus_worst=str(p-worst))
    if not min(best, worst) <= p <= max(best, worst):
        reasons.append('outside_visible_price_interval')
        geometry['position'] = ('better_than_touch' if (p > best) == (side == 'BUY')
                                else 'worse_than_last_visible')
    if state.clock-state.source_ns > state.max_age:
        reasons.append('stale_book')
    if state.clock-state.started_ns < 10_000_000_000:
        reasons.append('response_warmup')
    if state.trade_coverage != 'observed':
        reasons.append('trade_coverage_unknown')
    if frame is not None and dict(frame.values).get('order_price_delta_qty') is None:
        reasons.append('fixed_price_history_unavailable')
    return reasons, geometry


class UpdateOpportunityObserver:
    def __init__(self, cursor, *, sample_limit=64):
        self.cursor = cursor
        self.counts = Counter()
        self.samples = []
        self.sample_limit = sample_limit
        self.sequence = 0
        self.coverage = Counter()
        self.downstream = Counter()
        self.current = {}
        self.pending_requests = {}

    def observe(self, *, side, now_ms, call_sequence, inventory, target_price,
                order_price, order_id, order_active, needs_update, force_update,
                quantity_changed, pending, age_ms, interval_ms, price_delta_ticks,
                fixed_ticks, route_allowed, queue_keep, compute, emit_row=True):
        if side not in ('BUY', 'SELL'):
            raise ValueError('invalid order side')
        read_ns = int(now_ms) * 1_000_000
        self.cursor.advance(read_ns)
        self.sequence += 1
        role = ('opener' if inventory == 0 else
                'add' if (side == 'BUY') == (inventory > 0) else 'reducing')
        reasons = [name for name, blocked in (
            ('no_active_old_order', not order_active), ('original_needs_update_false', not needs_update),
            ('force', force_update), ('quantity_changed', quantity_changed), ('pending', pending),
            ('time', age_ms < interval_ms), ('sub_tick', price_delta_ticks < 1),
            ('route', not route_allowed), ('queue_keep', queue_keep)) if blocked]
        frame = (self.cursor.frame('bid' if side == 'BUY' else 'ask', order_price=order_price)
                 if order_price is not None and order_price > 0 else None)
        available = frame is not None and frame.valid
        base_action = 'UPDATE' if needs_update and price_delta_ticks + 1e-9 >= fixed_ticks else 'KEEP'
        group = f'{side}:{role}'
        self.counts[group + ':observed'] += 1
        self.counts[group + ':locally_qualified'] += int(not reasons)
        self.counts[group + ':micro_available'] += int(available)
        for reason in reasons:
            self.counts[group + ':blocked:' + reason] += 1
        support_reasons, geometry = old_price_support(self.cursor, side, order_price, order_active, frame)
        coverage = self.coverage
        coverage[group + ':observed'] += 1
        coverage[group + ':valid_active_old_order'] += int(order_active)
        coverage[group + ':distinct_candidate_price'] += int(order_active and price_delta_ticks >= 1)
        coverage[group + ':eligibility_unknown'] += int(order_active)
        # These counters share this exact call/side/order/read identity. Neither
        # local checks nor frame.valid are promoted to full execution eligibility.
        if order_active:
            coverage[f'{group}:local_micro:{int(not reasons)}:{int(available)}'] += 1
            for reason in support_reasons:
                coverage[group + ':missing:' + reason] += 1
            if frame is not None:
                for name, value in frame.values:
                    coverage[group + ':field_available:' + name] += int(value is not None)
                coverage[group + ':recovery_anchor_absent'] += int(frame.recovery_target is None)
        token = dict(sequence=self.sequence, call_sequence=call_sequence, side=side,
                     order_id=order_id, group=group, row=None)
        self.current[side] = token
        if not emit_row and len(self.samples) >= self.sample_limit:
            return None
        row = dict(sequence=self.sequence, call_sequence=call_sequence, side=side, role=role,
                   read_ns=read_ns, observation_sequence=self.cursor.sequence,
                   latest_observation_ready_ns=self.cursor.last_ready_ns,
                   order_id=order_id, old_price=order_price, target_price=target_price,
                   local_inventory=inventory, price_delta_ticks=price_delta_ticks,
                   baseline_price_gate_action=base_action, local_block_reasons=reasons,
                   micro_available=available, micro_missing_reason=(frame.reason if frame else 'price_or_book_unavailable'),
                   features=dict(frame.values) if frame else None,
                   source_asof_ns=frame.source_asof_ns if frame else None,
                   book_version=frame.book_version if frame else None,
                   compute=dict(compute), complete_action_eligibility_verified=False,
                   eligibility_status='unknown' if order_active else 'not_applicable',
                   eligibility_missing_checks=['downstream_pending_risk_quantity_and_request_link'] if order_active else [],
                   primary_reason=support_reasons[0] if support_reasons else None,
                   support_reasons=support_reasons, visible_price_geometry=geometry,
                   action_timing_status='unknown', counterfactual_effective_ns=None)
        if len(self.samples) < self.sample_limit:
            self.samples.append(row)
        token['row'] = row
        return row

    def token(self, side, call_sequence):
        token = self.current.get(side)
        return token if token is not None and token['call_sequence'] == call_sequence else None

    def resolved(self, token, **facts):
        """Actual final intent, not eligibility of an unexecuted alternative."""
        if token is None:
            return
        self.downstream[token['group'] + ':final_intent:' + facts['action']] += 1
        if token['row'] is not None:
            token['row']['downstream_intent'] = facts
            token['row']['eligibility_missing_checks'] = [
                'unexecuted_alternative_checks', 'request_admission_and_effectiveness']

    def blocked(self, token, reason, now_ms):
        if token is None:
            return
        self.downstream[token['group'] + ':execution_blocked:' + reason] += 1
        if token['row'] is not None:
            token['row'].setdefault('execution_blocks', []).append(
                dict(reason=reason, logical_ns=int(now_ms)*1_000_000))

    def checked(self, token, stage, **facts):
        if token is None:
            return
        self.downstream[token['group'] + ':checked:' + stage] += 1
        if token['row'] is not None:
            token['row'].setdefault('actual_checks', {})[stage] = facts

    def bind_request(self, token, kind, order_id, scheduled_ms):
        """Bind an actual reserved request; reservation is not submission."""
        if token is None:
            return
        key = (kind, str(order_id))
        if key in self.pending_requests:
            raise ValueError('duplicate pending observation request identity')
        self.pending_requests[key] = (token, int(scheduled_ms)*1_000_000)
        self.downstream[token['group'] + ':request_reserved:' + kind] += 1

    def submitted(self, kind, order_id, now_ms, *, request_sequence=None):
        linked = self.pending_requests.pop((kind, str(order_id)), None)
        if linked is None:
            return
        token, scheduled_ns = linked
        self.downstream[token['group'] + ':request_submitted:' + kind] += 1
        if token['row'] is not None:
            token['row'].setdefault('submitted_requests', []).append(dict(
                kind=kind, order_id=str(order_id), scheduled_request_ns=scheduled_ns,
                request_ns=int(now_ms)*1_000_000,
                gateway_request_sequence=request_sequence,
                observation_sequence=token['sequence']))
            # This says nothing about an alternative that B0 never requested.
            token['row']['actual_request_observed'] = True

    def snapshot(self):
        return dict(sequence=self.sequence, counts=dict(self.counts), samples=list(self.samples),
                    downstream=dict(self.downstream), unsubmitted_request_links=len(self.pending_requests),
                    coverage=dict(self.coverage), required_feature_contract='not_yet_frozen',
                    complete_eligibility_coverage=None,
                    observation_sequence=self.cursor.sequence, read_ns=self.cursor.read_ns,
                    scope='pre_price_throttle_observation_not_action_admission')
