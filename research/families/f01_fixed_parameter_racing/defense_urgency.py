"""Bounded offline D/U experiment contracts; no scheduler or live activation.

The conditional rule returns removed contributions, never an allow-post override.
Caller wiring and backend parity must be admitted separately before dispatch.
"""
from copy import deepcopy
from dataclasses import dataclass
import math

BASELINE = 'B0_RESPONSE_20261007'
RESPONSE_PATH = 'TRADE_BOOK_RESPONSE_VALUE'
PATCHES = {
    'B0': {},
    'DEFENSE_OFF__URGENCY_ON': {'defense_guard_enabled': False},
    'DEFENSE_ON__URGENCY_OFF': {'exit_urgency_strength': 0.0},
    'DEFENSE_OFF__URGENCY_OFF': {
        'defense_guard_enabled': False, 'exit_urgency_strength': 0.0},
    'DEFENSE_NO_PAUSE__URGENCY_ON': {'defense_pause': False},
    'DEFENSE_NO_PAUSE__URGENCY_OFF': {
        'defense_pause': False, 'exit_urgency_strength': 0.0},
    'DEFENSE_PAUSE_RELEASE_ON_LOSS': {},
    'DEFENSE_ALL_RELEASE_ON_LOSS': {},
    'URGENCY_OFF_WHILE_DEFENSE_PAUSES': {},
}
CONDITIONAL = tuple(PATCHES)[6:]


def arm_parameters(parent_params, arm_id):
    """Copy the actual parent's parameters without mutating shared model state."""
    if arm_id not in PATCHES:
        raise ValueError('undeclared arm')
    policy = parent_params.get('response_action_policy', {})
    if policy.get('path_id') != RESPONSE_PATH:
        raise ValueError('new B0 response policy required')
    # Invoke the maintained inference schema check; identity hashing is owned by
    # the release admission, not recreated per opportunity.
    from strategy.response_action_value import ResponseActionValue
    ResponseActionValue.from_fitted(policy['artifact'], RESPONSE_PATH)
    model = policy['artifact']['models'][RESPONSE_PATH]
    cost = policy.get('compute', {})
    if (len(model['features']) != 47 or model.get('alpha') != 10
            or cost.get('schema') != 'response_compute.v1'
            or cost.get('event_ns') != 109169 or cost.get('call_ns') != 195875
            or cost.get('common_to_both_arms') is not True):
        raise ValueError('response model/cost contract changed')
    if (parent_params.get('defense_guard_enabled') is not True
            or parent_params.get('defense_pause') is not True
            or parent_params.get('exit_urgency_strength') != 0.5):
        raise ValueError('expected full new B0 parent')
    result = deepcopy(parent_params)
    result.update(PATCHES[arm_id])
    # Kept outside params until actual quote/native consumers are integrated.
    return result, arm_id if arm_id in CONDITIONAL else 'B0'


@dataclass(frozen=True)
class ContributionRemoval:
    defense_all: bool = False
    defense_pause: bool = False
    urgency: bool = False


def conditional_removal(mode, *, potential_active, potential_pause,
                        pure_reducing, visible_state_consistent, visible_pnl):
    """Consume upstream role/quantity admission, never infer it from new quotes.

    Unknown state/role falls back to the complete baseline. The caller must use
    the existing legal pre-intervention quantity and visible, not economic, PnL.
    """
    if mode not in ('B0', *CONDITIONAL):
        raise ValueError('undeclared conditional mode')
    none = ContributionRemoval()
    if (mode == 'B0' or pure_reducing is not True
            or visible_state_consistent is not True):
        return none
    if (not isinstance(visible_pnl, (int, float))
            or isinstance(visible_pnl, bool) or not math.isfinite(visible_pnl)):
        return none
    if mode == 'URGENCY_OFF_WHILE_DEFENSE_PAUSES':
        return ContributionRemoval(urgency=potential_active is True and potential_pause is True)
    if potential_active is not True or visible_pnl >= 0:
        return none
    if mode == 'DEFENSE_PAUSE_RELEASE_ON_LOSS':
        return ContributionRemoval(defense_pause=True)
    return ContributionRemoval(defense_all=True)


def aggregate(rows):
    """Sum economic numerators/denominators, never mean account ratios."""
    rows = list(rows)
    for row in rows:
        for key in ('net_pnl', 'volume_btc', 'notional_usdc'):
            if not math.isfinite(row[key]):
                raise ValueError('nonfinite economics')
        if row['volume_btc'] < 0 or row['notional_usdc'] < 0:
            raise ValueError('negative absolute turnover')
    sums = {key: math.fsum(row[key] for row in rows)
            for key in ('net_pnl', 'volume_btc', 'notional_usdc')}
    if any(not math.isfinite(v) for v in sums.values()):
        raise ValueError('nonfinite economics')
    if sums['volume_btc'] < 0 or sums['notional_usdc'] < 0:
        raise ValueError('negative absolute turnover')
    p, q, n = (sums[k] for k in ('net_pnl', 'volume_btc', 'notional_usdc'))
    return dict(sums, eta_Q=p/q if q else None,
                eta_N=10000*p/n if n else None, eta_001=.001*p/q if q else None)


def baseline_completion_gate(calendar, cells):
    """Fail closed on anything except all 204 verified canonical new-B0 cells.

    Cells are produced by account/identity admission, not scheduler COMPLETE.
    In-flight cells must remain registered and are never resubmitted here.
    """
    parents = calendar['groups']['Development'] + calendar['groups']['Final']
    expected = {p['shard_id'] for p in parents}
    if len(parents) != 204 or len(expected) != 204:
        raise ValueError('invalid parent universe')
    ids = [c['parent_id'] for c in cells]
    if len(ids) != len(set(ids)) or set(ids) - expected:
        raise ValueError('duplicate or foreign baseline cell')
    valid = {c['parent_id'] for c in cells
             if c.get('baseline_id') == BASELINE
             and c.get('source_path_id') == RESPONSE_PATH
             and c.get('economics_status') == 'COMPLETE'
             and c.get('binding_verified') is True
             and c.get('terminal_cut_verified') is True
             and c.get('accounting_verified') is True
             and c.get('source_result_ref')}
    missing = sorted(expected - valid)
    return {'ready': not missing, 'verified': len(valid), 'required': 204,
            'missing_or_unverified': missing}


def choose_from_b(calendar, cells):
    """Only accept the complete B view; reject C/Final rows at this boundary."""
    expected = set(calendar['analysis_views']['B_SELECTION_PARENTS']['parent_ids'])
    if len(expected) != 25:
        raise ValueError('expected 25 B parents')
    keys = [(c['parent_id'], c['arm_id']) for c in cells]
    if len(set(keys)) != len(keys) or set(keys) != {(p, a) for p in expected for a in PATCHES}:
        raise ValueError('choice requires exactly B25 x nine, no other rewards')
    if any(c.get('economics_status') != 'COMPLETE' for c in cells):
        raise ValueError('incomplete selection cells')
    scores = {a: aggregate([c for c in cells if c['arm_id'] == a]) for a in PATCHES}
    def rank(a):
        mechanisms = (0 if a.startswith('DEFENSE_OFF__') else 1) + (
            0 if a.endswith('__URGENCY_OFF') else 1)
        return (-scores[a]['net_pnl'], int(a in CONDITIONAL), mechanisms, a)
    order = sorted(PATCHES, key=rank)
    challenger = next(a for a in order if a != 'B0')
    base = scores['B0']
    quality = [a for a in order if a != 'B0' and all(
        scores[a][k] is not None and base[k] is not None and scores[a][k] > base[k]
        for k in ('net_pnl', 'eta_Q', 'eta_N')) and all(
            c.get('hard_risk_contract_unchanged') is True for c in cells if c['arm_id'] == a)]
    return {'B_raw_winner': order[0], 'frozen_challenger': challenger,
            'B_quality_winner': quality[0] if quality else None, 'scores': scores}
