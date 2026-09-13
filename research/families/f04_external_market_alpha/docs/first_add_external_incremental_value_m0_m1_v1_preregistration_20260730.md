# First-Add External Incremental Value M0/M1 v1

Last materially modified: 2026-07-30

Status: `preregistered_blocked_on_30_distinct_valid_receive_time_utc_days`

Migration note (2026-08-11): exact transport-source amendments and host-bound rows are private operational evidence and are not distributed. This public preregistration is historical and makes no claim about a current host or collection route.

This is the only registered successor to the completed F10/F05 first-add evidence. It is prediction evidence only and cannot register an action or grant live authority.

## Question

F10 established that first exposure-increasing add decisions have negative average decision-to-campaign-terminal value on both sides. F05 did not identify a stable negative-value subset using campaign state and local causal microstructure. This audit asks whether real AWS Tokyo receive-time external state adds incremental identification:

\[
M0=\text{campaign state + local causal microstructure}
\]

\[
M1=M0+\text{Bitget/Bybit/OKX receive-time state}.
\]

The target remains:

\[
Y=V(T_{campaign})-V(t_{first\ add}),
\]

in USDC per first-add decision. Future direction, standalone markout, and fill-conditioned toxicity are not substitute targets.

## Data Gate

The audit remains blocked until the ledger contains at least 30 different UTC days with valid windows of at least 3,500 seconds. Multiple valid captures on one UTC day count once. The minimum chronology is 20 train days, one embargo day, five test days, and four late-panel days. Insufficient side, quality-panel, or lifecycle rows defer the audit; they do not relax its gates.

As of the last completed 2026-07-29 ledger check, the ledger contains 16 full windows over 15 distinct UTC days. The 2026-07-21 duplicate window does not increase the threshold denominator. The 2026-07-29 background capture was not yet admitted when this snapshot was written.

## Frozen Comparison

- BUY and SELL are fitted and reported separately.
- Grade A is primary; Grade B is sensitivity and cannot be pooled into Grade A.
- M0 and M1 use standardized Ridge with `alpha=1`; no hyperparameter search is allowed.
- M1 uses the existing `global_flow.v1` live ABI at 10/25/50/100/250/500ms. Those windows are engineering support inherited from the live feature ABI, not horizons selected from this target.
- Full M1 and each true leave-one-venue-out profile are rebuilt from raw venue tapes and score exactly the same rows.
- Every external feature must be visible under its recorded receive and feature-ready timestamps at the first-add decision.

For a side to pass, the day-clustered proper-score improvement lower bound must be positive, the simultaneous upper bound for the M1 high-risk subset must be below zero, all venue omissions must preserve direction, Grade B must not reverse Grade A, and the frozen late panel must not reverse the result.

Failure on both sides closes the current first-add classifier route and sends the project to baseline economic-structure redesign. A passing side still produces prediction evidence only; a separate F09 identity under the lineage outcome contract is required before any policy test.
