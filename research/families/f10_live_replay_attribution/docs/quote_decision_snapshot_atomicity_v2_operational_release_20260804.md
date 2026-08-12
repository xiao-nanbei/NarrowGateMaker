# Quote Decision Snapshot Atomicity v2 Operational Release

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The immutable quote-decision snapshot is deployed as a baseline-integrity successor. It is not a strategy action and no PnL or reward was read for the deployment decision.

The live quote chain now uses one frozen depth generation from quote-core input through policy, C++ routing, spread caps, post-only validation, and final order routing. `use_bar_pricing=true` must use the bar midpoint frozen inside the same snapshot and remains subject to the same depth and routing invariants.

## Clock Contract

The live stale gate uses:

\[
age_{visible}=t_{capture}-t_{receive}
\]

Source transport is reported and gated separately:

\[
lag_{source}=t_{receive}-t_{exchange}
\]

The bookTicker BBO is eligible as a post-only guard only when its prices, exchange timestamp, receive timestamp, visible age, and source lag are valid. Otherwise routing falls back to the depth BBO frozen in the same snapshot and records the exact reason.

## Verification

The 10,000-iteration concurrent synthetic audit passed with zero midpoint, microprice, quote-identity, or routing violations.

The immutable EC2 engineering window ran from `2026-08-03T20:28:38.715153Z` through `2026-08-03T20:37:45.792717Z`:

- 85/85 rows had valid market and depth generations.
- Final tick mismatches, quote-identity violations, post-only violations, and snapshot blocks were all zero.
- Snapshot lock wait/hold p99 were 2.317 ms and 0.401 ms.
- One unavailable bookTicker observation explicitly fell back to frozen depth.
- Equal-window cancel rate changed from 638.30/hour to 361.92/hour; the patch did not add cancel churn in this window.
- Nine health rows contained zero severe errors, recorder drops, or deep-book buffer accumulation. The one startup depth gap was resynchronized.

The authoritative evidence is stored under:

`${NARROWGATE_DATA_ROOT}/reports/quote_decision_snapshot_atomicity_v2_operational_20260804`

All code, config, evidence, rollback, and implementation-contract hashes are bound in the companion JSON release record.

## Baseline State

The strategy baseline is otherwise unchanged: causal-v12 remains enabled, q90 remains shadow-only with action disabled, and BUY fill-selection remains shadow-only with action disabled. Short-window PnL is not a rollback gate for this integrity release.

The later `exit_urgency_strength=0.5` versus `0` study remains a separate campaign-level, carryover-safe action identity.
