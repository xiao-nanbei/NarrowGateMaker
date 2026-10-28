"""Causal toy trade-through replay; not an exchange simulator."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from math import isfinite

from .model import quotes


@dataclass(frozen=True)
class Fill:
    time: float
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal


@dataclass(frozen=True)
class Result:
    cash: Decimal
    inventory: Decimal
    fees: Decimal
    final_mid: Decimal
    fills: tuple[Fill, ...]

    @property
    def equity(self):
        return self.cash + self.inventory * self.final_mid


def replay(stream, *, horizon, risk_aversion, volatility, liquidity,
           size=Decimal('1'), tick=Decimal('0.01'), fee_rate=Decimal('0')):
    """Start flat; match old quotes before observing/requoting each event."""
    if not isfinite(horizon) or horizon <= 0:
        raise ValueError('horizon must be positive and finite')
    for name, value in (('size', size), ('tick', tick), ('fee_rate', fee_rate)):
        if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
            raise ValueError(f'{name} must be a nonnegative finite Decimal')
    if size == 0 or tick == 0:
        raise ValueError('size and tick must be positive')
    quotes(100, 0, risk_aversion=risk_aversion, volatility=volatility,
           liquidity=liquidity, remaining=horizon)
    cash = inventory = fees = Decimal('0')
    fills = []
    previous = None
    resting = None
    final_mid = None
    for event in stream:
        if (not isfinite(event.time) or event.time < 0 or event.time > horizon
                or (previous is not None and event.time <= previous)):
            raise ValueError('events must have strictly increasing times within horizon')
        if (event.aggressor not in ('buy', 'sell')
                or any(not isinstance(v, Decimal) or not v.is_finite() or v <= 0
                       for v in (event.mid, event.trade_price, event.volume))):
            raise ValueError('invalid market event')
        previous = event.time
        if resting is not None:
            bid, ask = resting
            side = ('buy' if event.aggressor == 'sell' and event.trade_price <= bid else
                    'sell' if event.aggressor == 'buy' and event.trade_price >= ask else None)
            if side is not None:
                price = bid if side == 'buy' else ask
                quantity = min(size, event.volume)
                fee = quantity * price * fee_rate
                signed_quantity = quantity if side == 'buy' else -quantity
                inventory += signed_quantity
                cash -= signed_quantity * price + fee
                fees += fee
                fills.append(Fill(event.time, side, price, quantity, fee))
        final_mid = event.mid
        # Every row cancels/replaces the previous pair after its matching step.
        bid, ask = quotes(float(event.mid), float(inventory),
                          risk_aversion=risk_aversion, volatility=volatility,
                          liquidity=liquidity, remaining=horizon-event.time)
        bid = (Decimal(str(bid)) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        ask = (Decimal(str(ask)) / tick).to_integral_value(rounding=ROUND_CEILING) * tick
        if bid <= 0 or ask <= bid:
            raise ValueError('tick rounding produced invalid quotes')
        resting = bid, ask
    if final_mid is None:
        raise ValueError('empty stream has no terminal mark')
    return Result(cash, inventory, fees, final_mid, tuple(fills))
