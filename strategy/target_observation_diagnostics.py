"""Offline causal grids over actual publications, never a trading scheduler."""
from itertools import groupby


def fixed_grid(records, *, period_ns, phase_ns=0):
    """One side, nondecreasing observation times; equal-time input order wins.

    Grid phase is explicit. All publications at t are applied before a grid at
    t. Carrying a price preserves its publication time and source age. Stop at
    the final publication, without extrapolating an unseen suffix.
    """
    if type(period_ns) is not int or period_ns <= 0 or type(phase_ns) is not int:
        raise ValueError('invalid grid')
    last = None
    grid = None
    previous = None
    version = 0
    for stamp, batch in groupby(records, key=lambda r:r['observation_ns']):
        if previous is not None and stamp < previous:
            raise ValueError('publication time regression')
        if grid is None:
            grid = phase_ns + ((stamp-phase_ns+period_ns-1)//period_ns)*period_ns
        while grid < stamp:
            if last is not None:
                version += 1
                yield dict(last, observation_ns=grid, planned_observation_seq=version,
                           source_publication_ns=last['observation_ns'],
                           target_age_ns=grid-last['observation_ns'])
            grid += period_ns
        for row in batch:
            last = row
        if grid == stamp:
            version += 1
            yield dict(last, planned_observation_seq=version,
                       source_publication_ns=stamp,target_age_ns=0)
            grid += period_ns
        previous = stamp
