# External Adverse Quote Edge Guard Exact-Opener Mechanics v2.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: hash-bound and eligible for prospective exact-opener tape collection; formal collection has not started. Economic outcomes, prediction authority, F09 action registration, and live authority remain closed.

## Purpose

This execution-only amendment hardens the independent P2 v2 exact-opener estimand before any prospective tape is admitted. It does not modify the frozen v2 Spec or registration. The successor is explicitly `mechanics_informed_successor`: P2 v1's opener concentration motivated the new denominator, but v2.1 is not the v1 estimand and excludes first-add/P1 rows.

## Frozen Collection Contract

- The input must contain exactly the frozen tape schema. The validator permits only the loader-internal `input_path` column in addition to that schema.
- `baseline_eligible=1` requires a final `place`, `replace`, or `keep` action.
- A candidate quote change requires `guard_valid=1`, adverse side equal to the quote side, and `0 < effective_outward_ticks <= requested_outward_ticks`.
- Stable decision, group, origin, trigger, and client-order identifiers cannot be blank, null, or NaN.
- Lifecycle sequence numbers must be positive, unique, and strictly increasing per order. Each order may have at most one terminal outcome.
- The formal support floor is 5% separately for BUY and SELL. The pooled opener rate is diagnostic only and cannot pass the gate for a failing side.

The journal reads partial/full fill, quantity, price, remaining quantity, and terminal events solely to verify operational lifecycle linkage and executed action support. The accurate disclosure is therefore:

```text
economic_outcomes_read=false
operational_lifecycle_outcomes_read=true
```

No external PnL, reward, markout, campaign-terminal label, or other economic outcome table may be joined.

## Identity And Verification

- Canonical amendment identity: `8e4e5e32aa6abc366a1087b71eff803909c8d2a36ea48d7cee250174467a9786`
- Amendment file SHA256: `4d477183c2b06a6ed93207d6998b59483b42d03ef9802f017f657c6d20f69765`
- Machine audit SHA256: `aa6b894615f18b5199374b53d8c6bcfa9e37975db4e2d8a9492615e25baca573`
- Targeted contracts: `33 passed`.
- Full repository suite: `1382 passed, 4 skipped, 1 warning`.

The producer remains disabled by default, live orders are unchanged, and this tape is not part of the bounded seven-tape capture. Passing a future prospective tape can establish opener mechanics support only; it cannot directly register an F09 action.
