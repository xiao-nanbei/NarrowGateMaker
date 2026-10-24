"""Pure preparation contracts; these functions do not run labels or economics."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal


def _ns(day):
    return int(datetime.combine(day, time(), timezone.utc).timestamp()) * 1_000_000_000


def two_day_slices(start="2025-08-01", end_inclusive="2026-09-11"):
    day, end = date.fromisoformat(start), date.fromisoformat(end_inclusive) + timedelta(days=1)
    if day >= end:
        raise ValueError("empty or inverted slice calendar")
    result = []
    while day < end:
        stop = min(day + timedelta(days=2), end)
        result.append({"start_ns": _ns(day), "end_ns": _ns(stop), "account_initialization": "independent_flat_no_orders",
                       "hours": (stop - day).days * 24, "tail": (stop - day).days < 2,
                       "market_warmup": "explicit_dependency_context_not_reset", "interval": "[start,end)"})
        day = stop
    return result


def label_before_boundary(*, decision_ns, actual_outcome_end_ns, boundary_ns, right_censored=False):
    ends = tuple(actual_outcome_end_ns)
    return bool(ends and not right_censored and decision_ns < boundary_ns
                and all(end is not None and decision_ns <= end < boundary_ns for end in ends))


@dataclass(frozen=True)
class TerminalAccount:
    realized_trading_pnl: Decimal
    terminal_inventory: Decimal
    terminal_unrealized_pnl: Decimal | None
    fees: Decimal
    funding_cashflow: Decimal | None
    valuation_price: Decimal | None
    valuation_time_ns: int | None
    valuation_age_ns: int | None

    def totals(self):
        before = None if self.terminal_unrealized_pnl is None else self.realized_trading_pnl + self.terminal_unrealized_pnl - self.fees
        if self.terminal_inventory and (self.valuation_price is None or self.valuation_time_ns is None
                                        or self.valuation_age_ns is None or self.valuation_age_ns < 0):
            before = None
        complete = before is not None and self.funding_cashflow is not None
        return {"pnl_before_funding": before,
                "all_in_net_pnl": before + self.funding_cashflow if complete else None,
                "economic_complete": complete, "mode": "MTM_not_real_liquidation"}
