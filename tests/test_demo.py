"""Synthetic teaching assertions only; no recorded research outputs."""
from dataclasses import replace
from decimal import Decimal as D
from math import log1p
import unittest

from as_demo.model import quotes
from as_demo.replay import replay
from as_demo.synthetic import Event, events


CONFIG = dict(horizon=3, risk_aversion=0.2, volatility=0, liquidity=2)


def event(time, price='100', side='sell', volume='1', mid='100'):
    return Event(time, D(mid), D(price), side, D(volume))


class ModelTests(unittest.TestCase):
    def test_flat_formula(self):
        bid, ask = quotes(100, 0, risk_aversion=0.2, volatility=0.8,
                          liquidity=2, remaining=1)
        half = 0.2 * 0.8**2 / 2 + log1p(0.2 / 2) / 0.2
        self.assertAlmostEqual(bid, 100-half)
        self.assertAlmostEqual(ask, 100+half)

    def test_inventory_direction_and_symmetry(self):
        config = dict(risk_aversion=0.2, volatility=0.8, liquidity=2, remaining=1)
        flat, long, short = [quotes(100, q, **config) for q in (0, 1, -1)]
        self.assertLess(long[0], flat[0])
        self.assertGreater(short[1], flat[1])
        self.assertAlmostEqual(flat[0]-long[0], short[0]-flat[0])

    def test_zero_horizon(self):
        config = dict(risk_aversion=0.2, volatility=0.8, liquidity=2, remaining=0)
        self.assertEqual(quotes(100, 0, **config), quotes(100, 4, **config))

    def test_invalid_values(self):
        base = dict(mid=100, inventory=0, risk_aversion=0.2, volatility=0.8,
                    liquidity=2, remaining=1)
        for key in base:
            with self.subTest(key=key), self.assertRaises(ValueError):
                quotes(**(base | {key: float('nan')}))
        for key in ('mid', 'risk_aversion', 'liquidity', 'volatility', 'remaining'):
            with self.subTest(key=key), self.assertRaises(ValueError):
                quotes(**(base | {key: -1}))


class ReplayTests(unittest.TestCase):
    def test_no_same_event_fill(self):
        self.assertEqual(replay([event(0, '1')], **CONFIG).fills, ())

    def test_bid_partial_at_old_quote(self):
        result = replay([event(0), event(1, '90', volume='0.25')], **CONFIG)
        self.assertEqual(result.inventory, D('0.25'))
        self.assertEqual(result.fills[0].price, D('99.52'))
        self.assertEqual(result.cash, -D('0.25') * D('99.52'))

    def test_ask_fill_and_size_cap(self):
        result = replay([event(0), event(1, '110', 'buy', '3')], **CONFIG)
        self.assertEqual(result.inventory, D('-1'))
        self.assertEqual(result.fills[0].price, D('100.48'))

    def test_touch_inclusive(self):
        result = replay([event(0), event(1, '99.52')], **CONFIG)
        self.assertEqual(len(result.fills), 1)

    def test_wrong_aggressor_cannot_fill_other_side(self):
        result = replay([event(0), event(1, '90', 'buy'), event(2, '110', 'sell')], **CONFIG)
        self.assertEqual(result.fills, ())

    def test_changed_mid_does_not_reprice_existing_fill(self):
        result = replay([event(0), event(1, '90', mid='80')], **CONFIG)
        self.assertEqual(result.fills[0].price, D('99.52'))
        self.assertEqual(result.final_mid, D('80'))

    def test_fee_roundtrip_and_mark(self):
        result = replay([event(0), event(1, '90'), event(2, '110', 'buy')],
                        fee_rate=D('0.002'), **CONFIG)
        self.assertEqual(result.inventory, 0)
        self.assertEqual(result.fees, D('0.4'))
        self.assertEqual(result.cash, D('0.56'))
        self.assertEqual(result.equity, result.cash)

    def test_open_inventory_marked_not_liquidated(self):
        result = replay([event(0), event(1, '90', mid='102')], **CONFIG)
        self.assertEqual(result.inventory, 1)
        self.assertEqual(result.equity, result.cash + D('102'))
        self.assertEqual(len(result.fills), 1)

    def test_bad_time(self):
        for times in ((1, 0), (0, 0), (0, 4), (0, float('inf')), (-1, 0)):
            with self.subTest(times=times), self.assertRaises(ValueError):
                replay([event(t) for t in times], **CONFIG)

    def test_bad_event(self):
        for change in ({'aggressor': 'unknown'}, {'volume': D('0')},
                       {'mid': D('NaN')}, {'trade_price': D('-1')}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replay([replace(event(0), **change)], **CONFIG)

    def test_empty_and_invalid_config(self):
        with self.assertRaises(ValueError):
            replay([], **CONFIG)
        for key in ('size', 'tick'):
            with self.subTest(key=key), self.assertRaises(ValueError):
                replay([event(0)], **CONFIG, **{key: D('0')})
        with self.assertRaises(ValueError):
            replay([event(0)], **CONFIG, fee_rate=D('-1'))

    def test_prefix_causality(self):
        prefix = [event(0), event(1, '90')]
        short = replay(prefix, **CONFIG)
        extended = replay(prefix + [event(2, '110', 'buy')], **CONFIG)
        self.assertEqual(short.fills, extended.fills[:len(short.fills)])

    def test_deterministic_synthetic_input(self):
        self.assertEqual(list(events(seed=7)), list(events(seed=7)))
        self.assertNotEqual(list(events(seed=7)), list(events(seed=8)))
        stream = list(events(count=4))
        self.assertEqual(replay(stream, **CONFIG), replay(stream, **CONFIG))

    def test_bad_count(self):
        for count in (-1, 0, 1, True, 2.5):
            with self.subTest(count=count), self.assertRaises(ValueError):
                list(events(count))


if __name__ == '__main__':
    unittest.main()
