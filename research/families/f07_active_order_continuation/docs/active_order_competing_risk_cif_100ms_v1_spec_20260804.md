# F07 Active-Order Competing-Risk CIF 100 ms v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: mechanics contract frozen; lifecycle-panel generation, training, execution, economics, and live authority remain closed.

## Scope

This identity freezes the probability accounting for one active-order remaining-quantity risk spell. It does not classify raw exchange events, train a model, evaluate PnL, or authorize q90 action.

The bound implementation is:

- Path: `research/families/f07_active_order_continuation/audit/active_order_competing_risk_cif.py`
- SHA256: `9c9c00902b2ff495895d76c119f3095b1213bd53954588fa186ff33ec80ffa72`
- Identity: `active_order_competing_risk_cif_100ms_v1`

## Probability Contract

The state grid is exactly 100 ms. Duplicate, reversed, or skipped grid edges fail closed; the kernel does not backfill a missed edge.

The canonical competing causes, in order, are:

1. `favorable_fill`
2. `adverse_fill`
3. `cancel_ack`
4. `other_terminal`

For cause-specific rates \(\lambda_k\) in events per second, the interval probabilities are jointly normalized:

\[
h_k
=
\frac{\lambda_k}{\sum_j\lambda_j}
\left(1-e^{-0.1\sum_j\lambda_j}\right).
\]

The no-event probability is \(e^{-0.1\sum_j\lambda_j}\). Survival plus all cause-specific cumulative incidences must equal one exactly at every accepted edge. Survival is non-increasing and every cause-specific CIF is non-decreasing. Treating the four causes as independently normalized binary hazards is prohibited.

## Lifecycle Contract

`ACTIVE`, `PARTIALLY_FILLED`, and `CANCEL_PENDING` are risk phases. `EXCHANGE_TERMINAL` is terminal and forbids all later hazard evaluation.

- A partial fill ends the current remaining-quantity risk spell. Positive remaining quantity starts a new spell with a new `spell_id`, the same last evaluated grid edge, and reset survival/CIF state. It is not order terminal.
- A cancel request moves the order into `CANCEL_PENDING`; it is a state transition, not a competing terminal cause.
- A cancel reject returns the order to `ACTIVE` or `PARTIALLY_FILLED`; it is a state transition, not a competing terminal cause.
- A cancel ACK terminates the risk set with cause `cancel_ack`.
- A full fill has zero remaining quantity and terminates with the frozen upstream classification `favorable_fill` or `adverse_fill`.
- Any other supported exchange-terminal event terminates with `other_terminal`.

The upstream `lifecycle_events_v2` adapter owns event classification and must preserve these distinctions. The probability kernel must not infer lifecycle causes from PnL, reward, or markout inputs.

## Checkpoint Contract

Checkpoint/restore must preserve identity, the 100 ms grid, spell ownership, phase, remaining quantity, last edge, survival, every cause-specific CIF, and terminal metadata. Exact round-trip state equality and resumed-event parity are required. Schema, identity, probability-mass, and edge-sequence mismatch all fail closed.

## Closed Gates

The authoritative `lifecycle_events_v2` tape does not yet exist. Accordingly:

- lifecycle-panel generation is not authorized;
- training and prediction evaluation are not authorized;
- execution replay is not authorized;
- PnL, reward, markout, campaign terminal value, Validation, and sealed holdout may not be read;
- q90 action remains OFF and its threshold may not change;
- no action experiment, live deployment, or baseline update is authorized.

After `lifecycle_events_v2` is admitted, a successor identity must freeze its training panel and model. Execution remains closed until a chronological 40-day Python/C++ event lockstep, checkpoint-resume lockstep, and AWS receive-time transport all pass. Mechanics evidence cannot grant economic or live authority. Any PnL read requires a separate, preregistered economic action identity.

The exact machine-readable permissions and gate order are frozen in `active_order_competing_risk_cif_100ms_v1_spec_20260804.json`.
