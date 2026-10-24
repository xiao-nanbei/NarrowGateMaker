"""Pure fill accounting shared by live inventory and replay adapters.

Adapters own identity, clocks, state mutation and lifecycle transitions. This
module owns position/entry/open-fee accounting across additions, reductions and
zero-inventory boundaries; a rebate is negative, not a clamped accounting path.
"""

from dataclasses import dataclass
from enum import Enum


class PositionAccountingBasis(Enum):
    """Preserve the two existing numeric representations, not two fill rules.

    Live retains its accumulated cost basis; the replay risk ledger retains
    average entry only. Their historical fee grouping and dust/flip arithmetic
    are explicit here so extraction does not silently change rounding or old
    checkpoint semantics. Cash, funding and exchange-reported realized PnL are
    separate ledgers and are not replaced by this position cost calculation.
    """

    ACCUMULATED_COST = "accumulated_cost"
    AVERAGE_ENTRY = "average_entry"


@dataclass(frozen=True, slots=True)
class PositionFillState:
    inventory: float
    average_entry: float
    cost_basis: float
    opening_fee: float
    round_trip_pnl: float


@dataclass(frozen=True, slots=True)
class PositionFillTransition:
    state: PositionFillState
    realized_pnl: float = 0.0
    reduced_position: bool = False
    closed_round_trip: bool = False
    closed_round_trip_pnl: float = 0.0
    opened_position: bool = False


def apply_position_fill(
    state: PositionFillState,
    *,
    side: str,
    quantity: float,
    price: float,
    commission: float,
    basis: PositionAccountingBasis,
) -> PositionFillTransition:
    """Apply an already validated, identity-reconciled fill without mutation.

    Quantities are positive base units, price is quote/base, and commission is
    signed quote units. Callers validate finite values and BUY/SELL, deduplicate
    fills and own time/risk counters. This result allocates opening fees when a
    position is reduced; its realized PnL is not an additional cash debit.
    """
    if not isinstance(basis, PositionAccountingBasis):
        raise ValueError("unsupported position accounting basis")
    accumulated_cost = basis is PositionAccountingBasis.ACCUMULATED_COST
    q0 = state.inventory
    signed_qty = quantity if side == "BUY" else -quantity
    flat = q0 == 0.0 if accumulated_cost else abs(q0) <= 1e-10
    if flat:
        return PositionFillTransition(
            PositionFillState(signed_qty, price, price * abs(signed_qty), commission, 0.0),
            opened_position=True,
        )
    if (q0 > 0.0 and signed_qty > 0.0) or (q0 < 0.0 and signed_qty < 0.0):
        total_qty = abs(q0) + quantity
        cost = (
            state.cost_basis if accumulated_cost else state.average_entry * abs(q0)
        ) + price * quantity
        return PositionFillTransition(
            PositionFillState(
                q0 + signed_qty, cost / total_qty, cost,
                state.opening_fee + commission, state.round_trip_pnl,
            )
        )

    close_qty = min(quantity, abs(q0))
    closing_fee, opening_fee = split_fill_fee(commission, close_qty, quantity)
    if abs(q0) > 1e-10:
        open_fee_share = (
            state.opening_fee * (close_qty / abs(q0))
            if accumulated_cost else state.opening_fee * close_qty / abs(q0)
        )
    else:
        open_fee_share = state.opening_fee
    gross = (
        (price - state.average_entry) * close_qty
        if q0 > 0.0 else (state.average_entry - price) * close_qty
    )
    realized = gross - closing_fee - open_fee_share
    remaining_opening_fee = state.opening_fee - open_fee_share
    round_trip_pnl = state.round_trip_pnl + realized
    if abs(q0) - close_qty < 1e-10:
        flip_qty = quantity - close_qty
        if flip_qty > 1e-10:
            q1 = (
                signed_qty + q0 if accumulated_cost
                else flip_qty if signed_qty > 0.0 else -flip_qty
            )
            after = PositionFillState(q1, price, price * abs(q1), opening_fee, 0.0)
        else:
            after = PositionFillState(0.0, 0.0, 0.0, 0.0, 0.0)
        return PositionFillTransition(
            after, realized, True, True, float(round_trip_pnl), flip_qty > 1e-10,
        )
    q1 = q0 + signed_qty
    return PositionFillTransition(
        PositionFillState(
            q1, state.average_entry, state.average_entry * abs(q1),
            remaining_opening_fee, round_trip_pnl,
        ),
        realized_pnl=realized,
        reduced_position=True,
    )


def split_fill_fee(fee: float, closing_quantity: float, fill_quantity: float) -> tuple[float, float]:
    """Return closing/opening fees for an already validated fill.

    Callers establish finite values, positive fill quantity, and a closing
    quantity in [0, fill_quantity]. Keep multiplication/division order and
    compute the opening residual exactly as the existing ledgers did. This
    allocates this fill's fee only, not an earlier opening-fee pool.
    """
    closing_fee = fee * closing_quantity / fill_quantity
    return closing_fee, fee - closing_fee
