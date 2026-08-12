# BUY exposure adverse q90 live policy

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Frozen action identity

- Policy: `buy_exposure_adverse_q90_cancel_reenter_v1`
- Scope: BUY `opener/add` active orders only
- Score: `P(adverse fill within 100ms) - P(favorable fill within 100ms)`
- Entry: score `>= 0.005747997873332665`
- Action: cancel the active BUY order and retain its full-depth path
- Recovery: score falls below the same threshold
- Re-entry: return to the unchanged baseline quote path
- SELL and BUY reducing quotes: unchanged
- Size and inventory limit: unchanged
- Invalid state before entry: keep
- Invalid state after cancel: continue holding until a valid recovery state

The threshold is the Development OOF BUY opener/add empirical q90. Validation activation was 8.5944% and was not used to retune the threshold.

## Artifact identity

- Hazard model SHA256: `80743b0c737cd7485b4fe111363655e28e56f1d391ad6565ea5dcd46ca55d4f6`
- Action policy SHA256: `3bbb56e192cd92b2118e84c0dc0e23d9a9ea2d9018b5721f1f73921efa5a641a`

## Live feature path

The existing `depth20@100ms` partial-depth stream remains the strategy feature feed. A separate Binance USD-M `limit=1000` REST snapshot plus `@depth@100ms` diff stream maintains exact-price active-order queue, cancel/refill, recovery, and microprice-path features. Canceled policy orders retain this path until recovery so re-entry does not depend on a stale pre-cancel snapshot.

## Deployment verification

- Deployment UTC: 2026-07-24 22:38
- EC2 process: PID `1303470`, native profile
- Tests: `648 passed, 4 skipped`
- Capture state before deployment: no active or pending bounded capture; recording disabled
- First live cycle:
  - score `0.00577065`: cancel ACK, recovered after about 109ms, baseline re-entry
  - score `0.00717975`: cancel ACK, recovered after about 115ms, baseline re-entry
- Health after the cycle: 2 cancels, 2 re-entries, 197 keeps, zero retained paths
- No severe startup or action-path log errors

This was enabled by explicit user instruction. It is a bounded live policy trial, not evidence that DR action uplift or campaign-tail promotion gates have passed.
