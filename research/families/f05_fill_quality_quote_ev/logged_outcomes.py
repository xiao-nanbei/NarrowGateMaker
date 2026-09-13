"""Observed lifecycle outcomes, never hypothetical POST outcomes for WAIT.

Engineering adapter for the full replay journal. The observation target is
the existing order-lifecycle aggregate with 1/5/30-second *post-fill* price
diagnostics. Delayed first-price observations are not fixed-time prices. A
scientific training/split contract must explicitly admit this target; this
module neither chooses one nor grants fitting permission.
"""
from collections import defaultdict
import math

import pandas as pd

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.replay.l2_journal import audit_l2_delivery

TARGET = 'observed_order_lifecycle_quantity_weighted_post_fill_markout.v1'


def produce_logged_outcomes(manifest, *, production, input_manifest_id, horizons_s=(1, 5, 30)):
    """Keep the complete callback denominator and exact creation foreign keys.

    Complete rejected orders are reported separately, not used as maker
    no-fill observations. Partial/cutoff lifecycles retain their observed fill
    quantity without claiming their final aggregate or hypothetical fills.
    """
    if not input_manifest_id or any(h not in (1, 5, 30) for h in horizons_s):
        raise ValueError('bound input identity and supported post-fill horizons required')
    if not horizons_s or len(set(horizons_s)) != len(horizons_s):
        raise ValueError('nonempty unique horizons required')
    audit_l2_delivery(manifest, production)
    opportunities, created, events, fills = {}, {}, defaultdict(list), defaultdict(list)
    revisions, cutoffs = defaultdict(list), {}
    for event in iter_chunked_parquet_journal(manifest):
        kind, row = event['event_type'], event['record']
        sequence = event['production_event']['sequence']
        order_id = row.get('order_id')
        if kind in ('decision', 'decision_route_excluded'):
            opportunities[sequence] = dict(opportunity_id=sequence, decision_ns=event['event_ts_ns'],
                side={'BUY': 'bid', 'SELL': 'ask'}[event['side']],
                input_manifest_id=input_manifest_id, action=row['action'],
                route_allowed=kind == 'decision', source_event_sequence=event['sequence'],
                managed_order_ids=row.get('managed_order_ids', []),
                block_reasons=row.get('block_reasons', []))
        elif kind == 'order_created':
            if order_id in created:
                raise ValueError('duplicate logical order creation')
            created[order_id] = event
        elif kind == 'decision_action_revision':
            revisions[row['opportunity_event_id']].append(event)
        if order_id is not None:
            events[order_id].append(event)
            if kind == 'fill':
                fills[order_id].append(event)
            elif kind == 'order_observation_cutoff':
                cutoffs[order_id] = event
    linked = defaultdict(list)
    non_quote = []
    for order_id, event in created.items():
        row = event['record']
        parent = row['creation_opportunity_event_id']
        if parent is None:
            non_quote.append(dict(order_id=order_id, creation_source=row['creation_source'],
                                  source_event_sequence=event['sequence'],
                                  observed_filled_quantity=sum(float(x['record']['fill_qty']) for x in fills[order_id]),
                                  opportunity_kind='non_quote_action_not_fabricated_quote',
                                  reason=row.get('cause_kind'), unresolved=row['creation_source'] == 'quote_decision'))
        elif parent not in opportunities:
            raise ValueError('order refers to missing creation opportunity')
        else:
            if opportunities[parent]['side'] != {'BUY': 'bid', 'SELL': 'ask'}[event['side']]:
                raise ValueError('order/opportunity side mismatch')
            linked[parent].append(order_id)
    rows = []
    for key, opportunity in opportunities.items():
        orders = linked[key]
        opportunity['created_order_ids'] = orders
        opportunity['actual_submitted_order_ids'] = [order_id for order_id in orders
            if any(x['event_type'] == 'order_submit' for x in events[order_id])]
        opportunity['action_revision_sequences'] = [x['sequence'] for x in revisions[key]]
        opportunity['resolved_action'] = revisions[key][-1]['record']['action'] if revisions[key] else opportunity['action']
        for horizon in horizons_s:
            end, censored, status = None, True, 'not_submitted_no_counterfactual_label'
            quantity, markout, price_delta, amount = math.nan, math.nan, math.nan, math.nan
            observed_quantity, requested_ends, actual_ends, methods = 0., [], [], set()
            dependencies, fill_rows = [], []
            terminal = bool(orders)
            rejected = False
            for order_id in orders:
                history = events[order_id]
                terminals = [x for x in history if (
                    x['event_type'] in ('cancel_terminal_notification', 'order_reject') or
                    (x['event_type'] == 'order_outcome' and x['record']['outcome'] in ('cancel', 'reject')) or
                    (x['event_type'] == 'order_outcome' and x['record']['outcome'] == 'fill'
                     and x['record']['remaining'] <= 0))]
                # Account cutoff is not an exchange terminal. Keep any local
                # pending notification conservative rather than fabricate one.
                terminal = terminal and bool(terminals) and order_id not in cutoffs
                rejected = rejected or any(x['event_type'] == 'order_reject' for x in history)
                dependencies += [x['event_ts_ns'] for x in terminals]
                fill_rows += [x['record'] for x in fills[order_id]]
            observed_quantity = sum(float(x['fill_qty']) for x in fill_rows)
            if orders:
                status = 'rejected' if rejected else 'administrative_censor_or_missing_terminal'
                if terminal and not rejected:
                    censored, quantity = False, observed_quantity
                    end = max(dependencies)
                    status = 'complete_no_fill' if quantity == 0 else 'filled_conditional_unknown'
                    known = bool(fill_rows)
                    for fill in fill_rows:
                        prefix = f'markout_{horizon}s'
                        requested = fill.get(prefix + '_requested_end_ms')
                        actual = fill.get(prefix + '_actual_end_ms')
                        method = fill.get(prefix + '_method')
                        requested_ends.append(requested)
                        actual_ends.append(actual)
                        methods.add(method)
                        valid = (fill.get('markout_unit') == 'USDC/BTC'
                            and method == 'first_replay_price_at_or_after_target'
                            and fill.get(prefix + '_status') in ('exact', 'delayed')
                            and actual is not None and requested is not None and actual >= requested
                            and fill.get(prefix) is not None and math.isfinite(float(fill[prefix]))
                            and float(fill['quote_px']) > 0)
                        known = known and valid
                        if valid:
                            end = max(end, int(actual) * 1_000_000)
                    if known:
                        # Each partial fill has its own execution price and
                        # future dependency. Weights are BTC, never fill counts.
                        price_delta = sum(x['fill_qty'] * x[f'markout_{horizon}s'] for x in fill_rows) / quantity
                        markout = sum(x['fill_qty'] * x[f'markout_{horizon}s'] / x['quote_px'] * 10000
                                      for x in fill_rows) / quantity
                        amount = price_delta * quantity
                        status = 'filled_conditional_known'
            rows.append(dict(opportunity_id=key, horizon_ns=horizon*1_000_000_000,
                actual_outcome_end_ns=end, right_censored=censored, filled_quantity=quantity,
                observed_filled_quantity=observed_quantity, markout_bps=markout,
                markout_price_delta_usdc_per_btc=price_delta, markout_amount_usdc=amount,
                status=status, requested_end_ms=requested_ends, observed_price_end_ms=actual_ends,
                price_methods=sorted(m for m in methods if m is not None),
                fixed_horizon_price=bool(requested_ends) and requested_ends == actual_ends,
                price_source='replay_trade_price_array_may_include_clock_rows',
                input_manifest_id=input_manifest_id, target=TARGET,
                training_admitted=False))
    opportunity_frame, outcomes = pd.DataFrame(opportunities.values()), pd.DataFrame(rows)
    if outcomes.empty:
        raise ValueError('no decision denominator')
    outcomes['actual_outcome_end_ns'] = pd.array([row['actual_outcome_end_ns'] for row in rows], dtype='Int64')
    outcomes['horizon_ns'] = pd.array(outcomes.horizon_ns, dtype='int64')
    opportunity_frame['decision_ns'] = pd.array(opportunity_frame.decision_ns, dtype='int64')
    return opportunity_frame, outcomes, pd.DataFrame(non_quote)
