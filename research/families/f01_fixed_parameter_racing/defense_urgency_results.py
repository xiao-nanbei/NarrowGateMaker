"""Reward-read boundary for the fixed D/U panel, not an execution scheduler.

The index contains identities/locations only. Selection reads exactly B25 x nine;
non-selection rewards stay unopened until that complete selection is frozen.
"""
import json
from pathlib import Path

from .defense_urgency import BASELINE, PATCHES, choose_from_b


def read_selection_cells(calendar, index, read_cell):
    parents = set(calendar['analysis_views']['B_SELECTION_PARENTS']['parent_ids'])
    wanted = {(p, a) for p in parents for a in PATCHES}
    locations = {}
    for entry in index:
        key = (entry['parent_id'], entry['arm_id'])
        if key not in wanted:
            continue
        if key in locations:
            raise ValueError('duplicate selection cell in index')
        locations[key] = entry
    if locations.keys() != wanted:
        raise ValueError('all B25 identities must exist before reading selection rewards')
    cells = []
    for key in sorted(wanted):
        cell = read_cell(locations[key])
        if (cell['parent_id'], cell['arm_id']) != key:
            raise ValueError('selection result identity differs from its index')
        if (cell.get('baseline_id') != BASELINE
                or cell.get('economics_status') != 'COMPLETE'
                or cell.get('binding_verified') is not True
                or cell.get('terminal_cut_verified') is not True
                or cell.get('accounting_verified') is not True):
            raise ValueError('selection requires admitted complete economics')
        cells.append(cell)
    choose_from_b(calendar, cells)
    return cells


def freeze_selection(path, calendar, cells, *, run_identity):
    """Create once; an existing, different selection may never be overwritten."""
    if not run_identity:
        raise ValueError('frozen executable run identity required')
    if any(c.get('baseline_id') != BASELINE or any(c.get(k) is not True for k in
           ('binding_verified', 'terminal_cut_verified', 'accounting_verified')) for c in cells):
        raise ValueError('selection cells must have passed actual receipt admission')
    selection = dict(schema='defense_urgency_selection.v1', baseline_id=BASELINE,
        run_identity=run_identity,
        parent_ids=sorted(calendar['analysis_views']['B_SELECTION_PARENTS']['parent_ids']),
        **choose_from_b(calendar, cells))
    path = Path(path)
    # Exclusive creation is intentionally not an upsert. A crash leaves an
    # unreadable document which fails closed and requires explicit recovery.
    try:
        with path.open('x') as target:
            json.dump(selection, target, indent=2, allow_nan=False)
            target.write('\n')
    except FileExistsError:
        if json.loads(path.read_text()) != selection:
            raise ValueError('existing frozen selection differs') from None
    return selection


def load_frozen_selection(selection_path, *, calendar, run_identity):
    selection = json.loads(Path(selection_path).read_text())
    if (selection.get('schema') != 'defense_urgency_selection.v1'
            or selection.get('baseline_id') != BASELINE
            or selection.get('run_identity') != run_identity
            or selection.get('parent_ids') != sorted(
                calendar['analysis_views']['B_SELECTION_PARENTS']['parent_ids'])
            or selection.get('frozen_challenger') not in set(PATCHES)-{'B0'}
            or set(selection.get('scores', {})) != set(PATCHES)):
        raise ValueError('missing or mismatched frozen B selection')
    return selection


def read_after_selection(entry, read_cell, *, selection_path, calendar, run_identity):
    load_frozen_selection(selection_path, calendar=calendar, run_identity=run_identity)
    cell = read_cell(entry)
    if (cell['parent_id'], cell['arm_id']) != (entry['parent_id'], entry['arm_id']):
        raise ValueError('result identity differs from its index')
    return cell
