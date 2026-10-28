"""Finite-horizon textbook AS approximation, in fictional units."""
from math import isfinite, log1p


def quotes(mid, inventory, *, risk_aversion, volatility, liquidity, remaining):
    """Return continuous bid/ask prices; volatility is price/sqrt(time)."""
    values = (mid, inventory, risk_aversion, volatility, liquidity, remaining)
    if not all(isfinite(v) for v in values):
        raise ValueError('all inputs must be finite')
    if mid <= 0 or risk_aversion <= 0 or liquidity <= 0 or volatility < 0 or remaining < 0:
        raise ValueError('invalid AS inputs')
    variance_term = risk_aversion * volatility**2 * remaining
    center = mid - inventory * variance_term
    half_spread = variance_term / 2 + log1p(risk_aversion / liquidity) / risk_aversion
    bid, ask = center - half_spread, center + half_spread
    if not isfinite(bid) or not isfinite(ask) or bid <= 0 or ask <= bid:
        raise ValueError('quotes outside the supported positive-price domain')
    return bid, ask
