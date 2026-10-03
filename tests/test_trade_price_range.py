import random
import pickle

from features.trade_price_range import TradePriceRangeIndex


def test_one_hundred_thousand_exact_interval_queries():
    rng = random.Random(479)
    index = TradePriceRangeIndex()
    rows = []
    now = 0
    for i in range(100_000):
        now += rng.randrange(3)
        price = rng.randrange(100000) / 100
        rows.append((now, price))
        index.append(now, price)
        cutoff = now - 1000
        rows = [(t, p) for t, p in rows if t > cutoff]
        index.expire(cutoff)
        start = rng.randint(max(0, cutoff), now)
        end = rng.randint(start, now)
        selected = [p for t, p in rows if start < t <= end]
        expected = (min(selected), max(selected)) if selected else None
        assert index.range_minmax(start, end) == expected
        if i % 10000 == 0:
            index = pickle.loads(pickle.dumps(index))
    index.clear()
    assert index.range_minmax(0, now) is None
