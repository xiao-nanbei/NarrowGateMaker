"""Read-only spatial queries for actual, already-decided B0 order prices.

No price selection, model scoring or action eligibility is performed here.
Coverage is not the complete C0/CZ information-support test.
"""

from decimal import Decimal
import math

from data.depth_observation import DepthObservation


def canonical_order_price(raw, tick_size):
    """Recover only a binary-float tick representation, never reprice an order."""
    price, tick = Decimal(str(raw)), Decimal(str(tick_size))
    if not price.is_finite() or price <= 0 or not tick.is_finite() or tick <= 0:
        raise ValueError('invalid order price or tick')
    nearest = (price/tick).to_integral_value()*tick
    if price != nearest:
        if not isinstance(raw, float) or abs(price-nearest) > Decimal(str(4*math.ulp(raw))):
            raise ValueError('order price is not tick aligned')
    return nearest


def quote_regions(observation: DepthObservation, *, side, read_ns, max_age_ns,
                  old_price, target_price, tick_size, neighbor_ticks,
                  original_levels=10):
    """Query old/new prices separately using one legally delivered snapshot.

    Prices must be the caller's actual post-rounding candidates, not prices
    reconstructed from future fills. Missing old orders remain not applicable.
    Returned decimal strings preserve price/quantity values for later joins.
    """
    if side not in {"BUY", "SELL"}:
        raise ValueError("invalid order side")
    if read_ns < observation.original.ready_ns:
        raise ValueError("research book not ready at decision")
    if type(max_age_ns) is not int or max_age_ns < 0:
        raise ValueError("invalid source-age limit")
    if type(original_levels) is not int or not 1 <= original_levels <= 20:
        raise ValueError("original projection must remain within original Top20")
    if type(neighbor_ticks) is not int or neighbor_ticks < 0:
        raise ValueError("invalid neighborhood")
    tick = Decimal(str(tick_size))
    if not tick.is_finite() or tick <= 0:
        raise ValueError("positive tick size required")
    book_side = "bid" if side == "BUY" else "ask"
    snap = observation.snapshot
    points = getattr(snap.book, book_side + "s")
    original = points[:original_levels]
    fresh = read_ns - observation.original.source_asof_ns <= max_age_ns
    usable = bool(snap.book.valid and fresh and points)

    def region(low, high):
        covered = usable and snap.covers(book_side, low, high)
        original_covered = bool(covered and original and
                                min(original[0][0], original[-1][0]) <= low <= high <=
                                max(original[0][0], original[-1][0]))
        selected = [(p, q) for p, q in points if low <= p <= high]
        return dict(low=str(low), high=str(high), expanded_region_covered=covered,
                    original_region_covered=original_covered,
                    quantity=str(sum((q for _, q in selected), Decimal(0))) if covered else None,
                    occupied_levels=len(selected) if covered else None,
                    missing_reason=None if covered else (
                        "stale_book" if not fresh else "invalid_book" if not snap.book.valid
                        else "outside_initialized_projection"))

    result = dict(side=side, read_ns=read_ns, event_id=observation.original.event_id,
                  ready_ns=observation.original.ready_ns,
                  source_asof_ns=observation.original.source_asof_ns,
                  source_ordinal=snap.source_ordinal, book_version=snap.book.version,
                  snapshot_version=snap.snapshot_version, original_levels=original_levels,
                  requested_levels=snap.requested_levels,
                  complete_information_support="not_evaluated",
                  model_score_support="not_evaluated", action_eligibility="not_evaluated")
    for name, raw in (("old", old_price), ("target", target_price)):
        if raw is None:
            result[name] = dict(status="not_applicable", price=None)
            continue
        price = Decimal(str(raw))
        if not price.is_finite() or price <= 0 or price % tick:
            raise ValueError("order price must be positive and already tick aligned")
        if not points:
            result[name] = dict(status="missing_book", price=str(price))
            continue
        touch = points[0][0]
        quantity = snap.quantity(book_side, price) if usable else None
        result[name] = dict(
            status="observed" if usable else "unavailable", price=str(price), bbo=str(touch),
            signed_distance_bps=str((price-touch)/touch*Decimal(10000)),
            price_rank=next((i for i, (p, _) in enumerate(points, 1) if p == price), None),
            point_quantity=None if quantity is None else str(quantity),
            path=region(min(price, touch), max(price, touch)),
            neighborhood=region(price-tick*neighbor_ticks, price+tick*neighbor_ticks))
    return result
