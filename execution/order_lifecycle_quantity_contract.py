"""Frozen quantity tolerances for authoritative order lifecycles.

The terminal tolerance absorbs floating-point residue only. It is not an
exchange lot-size rule: a positive sub-lot remainder such as 0.0004 BTC is
still economically live and must never be relabelled as a full fill.
"""

from __future__ import annotations

import math

ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID = (
    "order_lifecycle_terminal_remainder_zero_abs_1e-12.v1"
)
TERMINAL_REMAINDER_ABS_TOLERANCE_BTC = 1e-12
QUANTITY_INCREASE_ABS_TOLERANCE_BTC = 1e-10
PARTIAL_FILL_PROGRESS_ABS_TOLERANCE_BTC = 1e-12
CANONICAL_TERMINAL_REMAINDER_BTC = 0.0


def finite_nonnegative_quantity(value: object, *, label: str) -> float:
    quantity = float(value)
    if not math.isfinite(quantity) or quantity < 0.0:
        raise ValueError(f"{label} must be finite and non-negative")
    return quantity


def is_terminal_zero(value: object) -> bool:
    """Return whether a source remainder is numerical zero under the contract."""

    quantity = finite_nonnegative_quantity(value, label="remaining quantity")
    return quantity <= TERMINAL_REMAINDER_ABS_TOLERANCE_BTC


def canonicalize_remaining_quantity(value: object) -> float:
    """Canonicalize numerical terminal zero while preserving every real remainder."""

    quantity = finite_nonnegative_quantity(value, label="remaining quantity")
    if quantity <= TERMINAL_REMAINDER_ABS_TOLERANCE_BTC:
        return CANONICAL_TERMINAL_REMAINDER_BTC
    return quantity


def validate_fill_terminal_claim(
    *,
    remaining_after: object,
    full_fill_claimed: bool,
) -> tuple[float, bool]:
    """Validate a source fill claim and return canonical remainder/classification.

    A zero remainder may infer a full fill even when the caller did not set the
    flag. A caller may not force a positive remainder into the terminal state.
    """

    canonical = canonicalize_remaining_quantity(remaining_after)
    inferred_full_fill = canonical == CANONICAL_TERMINAL_REMAINDER_BTC
    if bool(full_fill_claimed) and not inferred_full_fill:
        raise ValueError(
            "full fill claim has positive remaining quantity under the frozen "
            f"terminal tolerance: remaining_after={canonical:.17g}"
        )
    return canonical, inferred_full_fill


def persisted_terminal_remainder_is_zero(value: object) -> bool:
    """Formal journal rows use exact zero after source-side canonicalization."""

    quantity = finite_nonnegative_quantity(value, label="terminal remaining quantity")
    return quantity == CANONICAL_TERMINAL_REMAINDER_BTC
