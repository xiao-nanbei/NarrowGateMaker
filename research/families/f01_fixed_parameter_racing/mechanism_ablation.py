"""Preparation-only contracts for the fixed 12-parent, three-arm follow-up.

No scheduler, replay, source activation or result reuse is performed here.
The historical D/U parent remains unchanged; construct each arm from its own
original parent parameters, never from a live config or a sample parent's clock.
"""
from copy import deepcopy
from datetime import datetime, timedelta

from .defense_urgency import arm_parameters as historical_arm_parameters

BASELINE = 'B0_DEFENSE_URGENCY_OFF_20261011'
EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_MECHANISM12'
FULL_EXPERIMENT = 'B0_DEFENSE_URGENCY_OFF_20261011_MECHANISM_FULL407'
MARKOUT_SWITCH = '_research_generic_markout_policy_enabled'
ARMS = ('B0', 'GENERIC_MARKOUT_POLICY_OFF', 'DIRECT_DIRECTION_ASYM_OFF')
PARENT_INDICES = tuple(((2 * k + 1) * 150) // 24 for k in range(12))
MAX_ACCOUNT_PATHS = 36


def select_full_parents(calendar):
    """Bind all original IDs in calendar order; never invent a missing parent."""
    parents = sorted([*calendar['groups']['Development'], *calendar['groups']['Final']],
                     key=lambda p: p['account_start_utc'])
    if len(parents) != 204 or len({p['shard_id'] for p in parents}) != 204:
        raise ValueError('complete original 204-parent calendar required')
    cursor = datetime(2025, 8, 1).date()
    for i, parent in enumerate(parents):
        start = datetime.fromisoformat(parent['account_start_utc'].replace('Z', '+00:00')).date()
        end = datetime.fromisoformat(parent['account_end_utc_exclusive'].replace('Z', '+00:00')).date()
        days = 1 if i == 203 else 2
        group, index = ('development', i) if i < 150 else ('final', i-150)
        if (start != cursor or end != start+timedelta(days=days)
                or parent['duration_hours'] != 24*days
                or parent['original_parent_index'] != index
                or parent['account_start_utc'] != f'{start}T00:00:00Z'
                or parent['account_end_utc_exclusive'] not in (str(end), f'{end}T00:00:00Z')
                or parent['shard_id'] != f'{group}-{index:03d}-{start}'
                or parent['calendar_days'] != [(start+timedelta(days=j)).isoformat() for j in range(days)]):
            raise ValueError('original full-calendar identity/coverage differs')
        cursor = end
    if cursor != datetime(2026, 9, 12).date():
        raise ValueError('full calendar endpoint differs')
    return deepcopy(parents)


def arm_parameters(original_parent_params, arm):
    """Deep-copy the full promoted inheritance; patch exactly one contribution."""
    if arm not in ARMS:
        raise ValueError('only the three declared arms are allowed')
    if MARKOUT_SWITCH in original_parent_params:
        raise ValueError('historical parent must not contain the new research switch')
    baseline, _ = historical_arm_parameters(original_parent_params, 'DEFENSE_ON__URGENCY_OFF')
    if (baseline.get('asym_strength') != 0.1
            or baseline.get('markout_spread_scale') != 0.2
            or baseline.get('response_observe_only') is not True
            or baseline.get('response_update_branch_enabled') is not True
            or baseline.get('rng_seed') != 42 or baseline.get('latency_seed') != 59):
        raise ValueError('inherited parameter/response/seed contract differs')
    if arm == ARMS[1]:
        baseline[MARKOUT_SWITCH] = False
    elif arm == ARMS[2]:
        baseline['asym_strength'] = 0.0
    return baseline


def select_parents(calendar):
    """Deterministic date-only selection, rejecting gaps in the original 150."""
    parents = sorted(calendar['groups']['Development'], key=lambda p: p['account_start_utc'])
    if len(parents) != 150 or len({p['shard_id'] for p in parents}) != 150:
        raise ValueError('the complete original Development150 is required')
    previous_end = None
    for index, parent in enumerate(parents):
        start = datetime.fromisoformat(parent['account_start_utc'].replace('Z', '+00:00')).date()
        end = datetime.fromisoformat(parent['account_end_utc_exclusive']).date()
        if (parent['original_parent_index'] != index or parent['duration_hours'] != 48
                or end != start + timedelta(days=2)
                or parent['calendar_days'] != [start.isoformat(), (start + timedelta(days=1)).isoformat()]
                or (previous_end is not None and start != previous_end)):
            raise ValueError('original parent dates/order/coverage differ')
        previous_end = end
    return deepcopy([parents[i] for i in PARENT_INDICES])
