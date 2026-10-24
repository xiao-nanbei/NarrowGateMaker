"""Suffix wealth labels from already settled optional-update branches.

This does not advance an executor, liquidate inventory, or reinterpret local
notifications as economic matches. The caller must first settle both complete
branch outputs with public_accounting; that validates their full ledgers.
"""
import math

from models.replay.continuous_accounting import funding_cashflow_usdc


def economic_facts(result):
    return result['_economic_fill_trace'] if result.get('private_fill_visibility_enabled') else result['_fill_trace']


def wealth_at(result, funding, boundary_ns, mark):
    """Boundary before equal-time fills, after equal-time funding, as settled."""
    facts = economic_facts(result)
    if any(a['fill_ts'] > b['fill_ts'] for a, b in zip(facts, facts[1:], strict=False)):
        raise ValueError('economic fill order regressed')
    events = [(r['fill_ts']*1_000_000, 1, r) for r in facts
              if r['fill_ts']*1_000_000 < boundary_ns]
    events.extend((r['settlement_ns'], 0, r) for r in funding['events']
                  if r['settlement_ns'] <= boundary_ns)
    cash = q = fees = payments = 0.
    for _, kind, row in sorted(events, key=lambda item: (item[0], item[1])):
        if kind == 0:
            payment = funding_cashflow_usdc(q, row['mark_price'], row['rate'])
            payments += payment
        else:
            dq = row['fill_qty']*(1 if row['side'] == 'BUY' else -1)
            cash -= dq*row['quote_px']+row['fill_fee_usdc']
            fees += row['fill_fee_usdc']
            q += dq
            if abs(q) < 1e-12:
                q = 0.
    if q != 0 and (mark is None or not math.isfinite(mark) or mark <= 0):
        value = None
    else:
        value = cash+payments+(q*mark if q else 0.)
    return dict(wealth_usdc=value, inventory_btc=q, fill_cash_usdc=cash,
                fees_usdc=fees, funding_usdc=payments)


def wealth_at_many(result, funding, boundaries_and_marks):
    """One ordered ledger traversal, preserving scalar floating-point order."""
    facts = economic_facts(result)
    if any(a['fill_ts'] > b['fill_ts'] for a, b in zip(facts, facts[1:], strict=False)):
        raise ValueError('economic fill order regressed')
    points = sorted(boundaries_and_marks)
    if not points:
        return {}
    events = [(r['fill_ts']*1_000_000, 1, r) for r in facts
              if r['fill_ts']*1_000_000 < points[-1]]
    events.extend((r['settlement_ns'], 0, r) for r in funding['events']
                  if r['settlement_ns'] <= points[-1])
    events.sort(key=lambda item: (item[0], item[1]))
    cash = q = fees = payments = 0.
    cursor = 0
    states = {}
    for boundary in points:
        while cursor < len(events):
            timestamp, kind, row = events[cursor]
            if timestamp > boundary or (timestamp == boundary and kind == 1):
                break
            if kind == 0:
                payments += funding_cashflow_usdc(q, row['mark_price'], row['rate'])
            else:
                dq = row['fill_qty']*(1 if row['side'] == 'BUY' else -1)
                cash -= dq*row['quote_px']+row['fill_fee_usdc']
                fees += row['fill_fee_usdc']
                q += dq
                if abs(q) < 1e-12:
                    q = 0.
            cursor += 1
        mark = boundaries_and_marks[boundary]
        value = (None if q != 0 and (mark is None or not math.isfinite(mark) or mark <= 0)
                 else cash+payments+(q*mark if q else 0.))
        states[boundary] = dict(wealth_usdc=value, inventory_btc=q, fill_cash_usdc=cash,
                                fees_usdc=fees, funding_usdc=payments)
    return states


def paired_labels(keep, update, accounts, funding, request, marks, *, day_end_ns, account_end_ns):
    """One pair, three fixed endpoints; incomplete diagnostics stay censored."""
    start = request.decision_ns
    end = request.execution_end_ns
    if start+30_000_000_000 >= min(day_end_ns, account_end_ns):
        raise ValueError('main label crosses its legal day/account')
    for result, account, action in zip((keep, update), accounts,
                                      ('KEEP_EXISTING', 'UPDATE_TO_B0_TARGET'), strict=True):
        if not account['economic_complete'] or account['account_end_ns'] != end:
            raise ValueError('branches require complete original-contract settlement')
        evidence = result['response_update_fork']
        if evidence['action'] != action or evidence['decision_ns'] != start:
            raise ValueError('branch action/decision binding mismatch')
    points = {start: marks[start], end: marks[end]}
    points.update({start+h*1_000_000_000: marks[start+h*1_000_000_000]
                   for h in (5, 30, 120) if start+h*1_000_000_000 <= end
                   and start+h*1_000_000_000 < min(day_end_ns, account_end_ns)})
    snapshots = [wealth_at_many(result, funding, points) for result in (keep, update)]
    for account, snapshot in zip(accounts, snapshots, strict=True):
        state = snapshot[end]
        if state['wealth_usdc'] is None or not math.isclose(state['wealth_usdc'], account['all_in_net_pnl'],
                                                          rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError('label ledger differs from settled branch')
    prefix = [[r for r in economic_facts(result) if r['fill_ts']*1_000_000 < start]
              for result in (keep, update)]
    if prefix[0] != prefix[1]:
        raise ValueError('branch economic prefix differs before intervention')
    initial = snapshots[0][start]
    labels = {}
    for horizon in (5, 30, 120):
        boundary = start+horizon*1_000_000_000
        if boundary > end or boundary >= min(day_end_ns, account_end_ns):
            labels[str(horizon)] = dict(valid=False, reason='endpoint_censored')
            continue
        states = [snapshot[boundary] for snapshot in snapshots]
        if initial['wealth_usdc'] is None or any(s['wealth_usdc'] is None for s in states):
            labels[str(horizon)] = dict(valid=False, reason='missing_valid_boundary_mark')
            continue
        ys = [s['wealth_usdc']-initial['wealth_usdc'] for s in states]
        labels[str(horizon)] = dict(valid=True, keep_usdc=ys[0], update_usdc=ys[1],
                                    delta_usdc=ys[1]-ys[0], terminal=states)
    return dict(initial=initial, labels=labels, unit='USDC_per_assigned_action',
                terminal_liquidation=False, common_inputs='frozen_exogenous_observations',
                execution_draws='path_dependent_not_request_index_matched')
