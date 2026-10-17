"""One identified optional price decision, using the existing order lifecycle.

Decision admission is not a claim that a future request will succeed. No RNG,
queue state, future fills, price generation, or execution clock lives here.
"""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class OptionalUpdateFork:
    decision_ns: int
    decision_sequence: int
    side: str
    order_id: str
    old_price: float
    target_price: float
    quantity: float
    action: str
    execution_end_ns: int

    def __post_init__(self):
        if (type(self.decision_ns) is not int or self.decision_ns % 1_000_000
                or type(self.decision_sequence) is not int or self.decision_sequence < 0
                or self.side not in {'BUY', 'SELL'} or not self.order_id
                or self.action not in {'BASELINE', 'KEEP_EXISTING', 'UPDATE_TO_B0_TARGET'}
                or type(self.execution_end_ns) is not int
                or self.execution_end_ns-self.decision_ns not in {
                    5_000_000_000, 30_000_000_000, 120_000_000_000}
                or any(not math.isfinite(v) or v <= 0
                       for v in (self.old_price, self.target_price, self.quantity))):
            raise ValueError('invalid optional-update branch contract')

    def validate(self, checkpoint, *, account_end_ns):
        if (not checkpoint.get('public_binding')
                or checkpoint.get('consumed', False)
                or checkpoint['cut_ts_ms']*1_000_000 > self.decision_ns
                or self.execution_end_ns > account_end_ns
                or not getattr(checkpoint['runtime'], 'response_update_branch_enabled', False)
                or getattr(checkpoint['runtime'], 'response_update_branch_evidence', None) is not None):
            raise ValueError('optional-update branch requires an unchanged enabled parent checkpoint')

    def matches(self, *, now_ms, sequence, side, order_id):
        return (now_ms*1_000_000, sequence, side, str(order_id)) == (
            self.decision_ns, self.decision_sequence, self.side, self.order_id)

    def select(self, *, old_price, target_price, quantity, baseline, reasons):
        # Exact values belong to this identified decision, not a nearby order.
        if (old_price, target_price, quantity) != (
                self.old_price, self.target_price, self.quantity):
            raise ValueError('optional-update target state differs from parent observation')
        if reasons:
            raise ValueError('optional-update root is not admissible: '+','.join(reasons))
        return baseline if self.action == 'BASELINE' else self.action == 'UPDATE_TO_B0_TARGET'


def admission_reasons(*, local_reasons, order_count, pending, inventory_allowed,
                      forced_cancel, quantity, lot_size, capped_quantity,
                      budget_allowed):
    """Checks known at the original consumer; downstream races stay unknown."""
    reasons = list(local_reasons)
    for reason, blocked in (
        ('multiple_or_absent_orders', order_count != 1),
        ('pending', pending), ('inventory', not inventory_allowed),
        ('forced_cancel', forced_cancel), ('below_lot', quantity < lot_size),
        ('notional_quantity_change', capped_quantity != quantity),
        ('inventory_budget', not budget_allowed),
    ):
        if blocked and reason not in reasons:
            reasons.append(reason)
    return reasons
