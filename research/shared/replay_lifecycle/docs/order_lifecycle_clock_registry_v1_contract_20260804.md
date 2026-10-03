# Order Lifecycle Clock Registry v1

Last materially modified: 2026-08-04

## Purpose

This mechanics-only registry combines the authoritative `order_lifecycle_journal.v1` with a fully bound baseline epoch. It reports two clocks independently:

\[
T_{calendar}=t_{event}^{visible}-t_{activation}^{visible}
\]

\[
T_{risk}=\int 1\{order\in fill\ risk\ set\}\,dt.
\]

Orders not activated, already exchange-terminal, or outside the relevant repair risk set do not accrue risk time.

## Event Identity

The registry does not collapse lifecycle events into one generic terminal label. It preserves:

- partial fill and full fill;
- cancel request, cancel ACK, and cancel reject;
- reject, expiry, and shutdown;
- post-terminal recovery as a state transition outside the old order's fill risk set.

It emits separate first-fill and exchange-terminal Aalen-Johansen tables on a 100 ms grid. The 30 s limit is a right-censoring/reporting boundary, not a policy TTL or an estimand horizon.

## Epoch Rule

Every order is owned by the epoch in which it was submitted. An order crossing an epoch boundary fails closed. Curves are emitted per epoch; v1 cannot pool them. Drift and live/replay transport are evaluated only after epoch-specific estimation.

## Data Boundary

The input schema must be exactly `OrderLifecycleJournalRow`. Extra columns are rejected, which prevents hidden PnL, reward, markout, or label inputs. Both quantity-weighted exposure clocks are retained:

\[
E_q^{visible}=\int Q_{remaining}(t_{visible})dt,
\qquad
E_q^{exchange}=\int Q_{remaining}(t_{exchange})dt.
\]

The exchange-time value remains nullable with explicit validity/completeness fields. No C++ exposure authority is inferred.

## Current Status

The registry implementation and synthetic competing-risk tests pass. The current 360-hour baseline manifest has zero authorized epochs, so no live curve has been produced. This is a provenance blocker, not an outcome failure.
