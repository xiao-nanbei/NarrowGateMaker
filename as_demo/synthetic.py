"""Seeded fictional events, not a fit to any instrument or strategy."""
from dataclasses import dataclass
from decimal import Decimal
from random import Random


@dataclass(frozen=True)
class Event:
    time: float
    mid: Decimal
    trade_price: Decimal
    aggressor: str
    volume: Decimal


def events(count=200, seed=7):
    if isinstance(count, bool) or not isinstance(count, int) or count < 2:
        raise ValueError('count must be an integer >= 2')
    rng = Random(seed)
    mid_ticks = 10000
    for time in range(count):
        mid_ticks = max(1000, mid_ticks + rng.randint(-25, 25))
        aggressor = rng.choice(('buy', 'sell'))
        distance = rng.randint(0, 250)
        trade_ticks = mid_ticks + (distance if aggressor == 'buy' else -distance)
        yield Event(time, Decimal(mid_ticks) / 100, Decimal(trade_ticks) / 100,
                    aggressor, Decimal(rng.randint(1, 4)) / 4)
