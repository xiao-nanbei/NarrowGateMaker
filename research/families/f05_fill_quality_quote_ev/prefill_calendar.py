"""Fixed E/C sampling revision, derived from the maintained F03 calendar."""
from datetime import date, datetime, timezone
from functools import lru_cache

from research.families.f03_causal_13_head.time_weighted_evaluation import SPLITS, build_shard_specs

REVISION = 'f03-407-t100-final107-v2'
DAY_NS = 86_400_000_000_000
HORIZON_NS = 30_000_000_000
SURFACES = ('E:BUY', 'E:SELL', 'C:BUY', 'C:SELL')


def day_start(day):
    return (date.fromisoformat(day) - date(1970, 1, 1)).days * DAY_NS


@lru_cache(maxsize=1)
def account_specs():
    return (*build_shard_specs(initial_capital_usdc=10000, phase='development'),
            *build_shard_specs(initial_capital_usdc=10000, phase='final'))


@lru_cache(maxsize=1)
def slots():
    """Immutable (day, split, surface, index, left_ns, right_ns) records."""
    result = []
    for j, day in enumerate(SPLITS['T']):
        quota = 512 * (j + 1) // 100 - 512 * j // 100
        start = day_start(day)
        for surface in SURFACES:
            for index in range(quota):
                result.append((day, 'T', surface, index,
                               start + DAY_NS * index // quota,
                               start + DAY_NS * (index + 1) // quota))
    for group, count in (('A', 16), ('B', 32), ('C', 16)):
        days = SPLITS[group]
        for j in range(count):
            day = days[(2*j+1)*len(days)//(2*count)]
            start = day_start(day)
            for s, surface in enumerate(SURFACES):
                index = (j+s) % 4
                result.append((day, group, surface, index,
                               start + index*DAY_NS//4, start + (index+1)*DAY_NS//4))
    return tuple(result)


def sampling_identity(slot, account_id):
    day, split, surface, index, left, right = slot
    return dict(calendar_revision=REVISION, parent_account_id=account_id,
                day=day, split=split, surface=surface, slot_index=index,
                slot_start_ns=left, slot_end_ns=right)


def account_slots(account_id, end_ns):
    spec = next((s for s in account_specs() if s.shard_id == account_id), None)
    if spec is None or end_ns != spec.end_ts_ms_exclusive*1_000_000:
        raise ValueError('calendar account/end binding mismatch')
    return tuple(s for s in slots() if s[0] in spec.calendar_days)


def label_group(row):
    """Validate actual clocks, account and immutable slot identity, not a split tag."""
    evidence = row.get('sampling', {})
    account = evidence.get('parent_account_id')
    spec = next((s for s in account_specs() if s.shard_id == account), None)
    if spec is None:
        raise ValueError('calendar label requires a bound parent account')
    ts = row['decision_ts_ns']
    end = row['terminal_mark_ts_ms']*1_000_000
    day = datetime.fromtimestamp(ts//1_000_000_000, timezone.utc).date().isoformat()
    if (end != ts+HORIZON_NS or not day_start(day) <= ts < end
            or end >= min(day_start(day)+DAY_NS, spec.end_ts_ms_exclusive*1_000_000)
            or row['replay_start_ts_ms'] != spec.start_ts_ms):
        raise ValueError('calendar label outcome leaves its UTC day/account')
    surface = f"{row['kind']}:{row['side']}"
    matches = [s for s in account_slots(account, spec.end_ts_ms_exclusive*1_000_000)
               if s[0] == day and s[2] == surface and s[4] <= ts < s[5]]
    if len(matches) != 1 or evidence != sampling_identity(matches[0], account):
        raise ValueError('calendar label slot/use does not match actual decision')
    return matches[0][1]


def plan():
    specs = account_specs()
    by_day = {day: s.shard_id for s in specs for day in s.calendar_days}
    train = sorted({by_day[s[0]] for s in slots() if s[1] == 'T'})
    diagnostic = sorted({by_day[s[0]] for s in slots() if s[1] != 'T'})
    return dict(calendar_revision=REVISION, timezone='UTC',
                splits={k: list(v) for k, v in SPLITS.items()},
                day_to_account=by_day, shards=[s.to_metadata() for s in specs],
                slots=[sampling_identity(s, by_day[s[0]]) for s in slots()],
                train_parent_ids=train, diagnostic_parent_ids=diagnostic,
                unique_label_parent_ids=sorted(set(train)|set(diagnostic)),
                coverage_account_arm_units=2040, coverage_strategy_days=4070)
