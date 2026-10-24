"""Outcome-blind optional-update slots on the existing non-Final calendar."""
from datetime import date
from itertools import product
from functools import lru_cache
from bisect import bisect_right

from research.families.f03_causal_13_head.time_weighted_evaluation import SPLITS
from strategy.response_action_features import extract

DAY_NS = 86_400_000_000_000
SURFACES = tuple(product(('BUY', 'SELL'), ('opener', 'add', 'reducing'), ('KEEP', 'UPDATE')))


def slots():
    # Return fresh dictionaries; never expose the cached calendar's ownership.
    return tuple(dict(row) for row in _calendar(tuple((g, tuple(SPLITS[g])) for g in ('T', 'A', 'B', 'C'))))


@lru_cache(maxsize=4)
def _calendar(groups):
    groups = dict(groups)
    result = []
    for group, limit in (('T', 4096), ('A', 64), ('B', 128), ('C', 64)):
        days = groups[group]
        ordinal = 0
        for rank, day in enumerate(days):
            count = limit*(rank+1)//len(days)-limit*rank//len(days)
            start = (date.fromisoformat(day)-date(1970, 1, 1)).days*DAY_NS
            for index in range(count):
                side, role, baseline = SURFACES[ordinal % len(SURFACES)]
                result.append(dict(day=day, split=group, index=index, side=side,
                    role=role, baseline_action=baseline,
                    start_ns=start+DAY_NS*index//count,
                    end_ns=start+DAY_NS*(index+1)//count,
                    label_day_end_ns=start+DAY_NS))
                ordinal += 1
    return tuple(tuple(row.items()) for row in result)


class OptionalUpdateSampler:
    """First admitted, supported observation in each predeclared slot."""
    def __init__(self, selected_slots, account_end_ns):
        selected_slots = tuple(selected_slots)
        allowed = {tuple(sorted(s.items())) for s in slots()}
        if any(tuple(sorted(s.items())) not in allowed for s in selected_slots):
            raise ValueError('sampling slot is not in the frozen calendar contract')
        self.slots = tuple(dict(s) for s in selected_slots)
        self.account_end_ns = account_end_ns
        self.selected = {}
        self.eligible = {}
        self.qualified = 0
        self.unsupported = 0
        self._index_slots()

    def _index_slots(self):
        surfaces = {}
        for index, slot in enumerate(self.slots):
            key = (slot['side'], slot['role'], slot['baseline_action'])
            surfaces.setdefault(key, []).append((slot['start_ns'], index))
        self._surface_index = {key: tuple(sorted(rows)) for key, rows in surfaces.items()}

    def consider(self, row):
        if not row['optional_update_admission']['decision_admissible']:
            return
        self.qualified += 1
        if not row['micro_available']:
            self.unsupported += 1
            return
        # No substitution of touch history for the actual order price.
        if row['features'].get('order_price_delta_qty') is None:
            self.unsupported += 1
            return
        if extract(row, materialize=False) is None:
            self.unsupported += 1
            return
        now = row['read_ns']
        if not hasattr(self, '_surface_index'):
            self._index_slots()  # Old disk checkpoints retain their original slots.
        candidates = self._surface_index.get((row['side'], row['role'], row['baseline_price_gate_action']), ())
        stop = bisect_right(candidates, (now, len(self.slots)))
        for index in sorted(index for _, index in candidates[:stop]):
            slot = self.slots[index]
            if not slot['start_ns'] <= now < slot['end_ns']:
                continue
            if (row['side'], row['role'], row['baseline_price_gate_action']) != (
                    slot['side'], slot['role'], slot['baseline_action']):
                continue
            key = f"{slot['day']}:{slot['index']}"
            if now+30_000_000_000 >= min(slot['label_day_end_ns'], self.account_end_ns):
                continue
            self.eligible[key] = self.eligible.get(key, 0)+1
            if key not in self.selected:
                self.selected[key] = dict(row, sampling=dict(slot), model_features=extract(row))
