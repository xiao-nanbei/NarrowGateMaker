# BUY Add Conditional Widen Causal v4 v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): the exact causal-v4 DR, fill, campaign and tail values below are withdrawn with their superseded data/replay identity. The conservative family-level conclusion remains unchanged: this exact `widen_1tick` action is closed and its locked panels must not be opened to rescue it. A successor needs a new family identity and current contracts.

## Decision

`buy_add_conditional_widen_causal_v4_v1` failed its Development gate and is closed. The 9-day Validation and 10-day sealed holdout were not replayed or read.

This closes the current **BUY one-tick quote-distance family** on the frozen causal-v4 evidence split. It does not close BUY selection research generally, and it does not authorize a different tree, threshold, feature subset, or fixed widen rule on the sealed dates.

## Frozen Design

| Item | Value |
|---|---|
| Surface | BUY exposure-increasing add |
| Behavior actions | baseline 50%, widen one tick 50% |
| SELL | baseline only |
| Intervention unit | at most one per campaign |
| Size / reducing / inventory limit | unchanged |
| External reference | excluded |
| Development | 100 previously inspected good days through 2026-06-23 |
| Embargo | 2026-06-24 |
| Validation | 2026-06-25 through 2026-07-03, locked |
| Embargo | 2026-07-04 |
| Sealed holdout | 10 good days from 2026-07-05 through 2026-07-15, locked |

The family spec froze a 35-feature local-only surface, action-specific Ridge nuisance models, past-only chronological nuisance folds, and a second chronological depth-2 honest treatment tree. The tree learned directly from cross-fitted doubly robust pseudo-outcomes for `widen - baseline`; it did not reuse the old good-fill score as its target.

Artifact identities:

- empirical P3 SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- queue-v3 q0.70 SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- AWS Tokyo latency tape SHA256: `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`
- live-like config SHA256: `0c13d1533fe28c4f3bbaafdd85f62ed0b6074314e177b000356ab4b529a4aa9a`

## Replay Integrity

The Development replay produced 4,387 unique BUY campaigns:

| Check | Result |
|---|---:|
| baseline / widen rows | 2,207 / 2,180 |
| widen assignment rate | 49.69% |
| selected propensity | 0.50 on every row |
| SELL interventions | 0 |
| causal path valid | 100% |
| censored campaigns | 46 / 4,387 |
| reward identity max error | `2.78e-17` USDC |

Relative to the no-randomization control, fills, placed actions, and campaigns were retained at 99.988%, 99.979%, and 99.945%. Inventory time was 1.0009x. The experiment therefore changed quote geometry without obtaining its result from a material activity collapse.

## Development OOF

The second policy layer had 28 future evaluation days, 1,267 rows, candidate rate 37.96%, and policy ESS 642.

| Outcome, higher is better | DR uplift per decision | 95% day-cluster interval |
|---|---:|---:|
| decision-to-terminal reward | -0.00742 | [-0.02657, +0.01304] |
| campaign-cost avoidance | -0.00707 | [-0.02800, +0.01410] |
| negative-terminal protection | -0.00575 | [-0.02275, +0.01041] |
| development-q10 shortfall protection | -0.00421 | [-0.01588, +0.00755] |
| repair probability | +0.00261 | [-0.00513, +0.00993] |
| restricted time-to-repair value | +4.95 s | [-60.61, +66.97] |
| intervention fill probability | -0.00658 | [-0.02208, +0.00787] |

All four value/downside gates failed. Support and overlap passed, so this is not an underpowered-action or propensity failure.

As a diagnostic only, applying widen to every nuisance-OOF row had reward uplift `+0.01087`, but its interval was `[-0.01934, +0.04151]`. The global point estimate is therefore still uncertain, while the shallow conditional tree did not discover a stable positive subset.

No Development campaign crossed the predeclared `terminal <= -5 USDC` diagnostic threshold. That event count is unsupported here and was not used to manufacture a tail pass.

## Interpretation

One tick of BUY widening is almost a no-op for participation at the aggregate level, and the raw randomized rows show a favorable unconditional point estimate. The conditional treatment effect, however, does not remain stable when the action rule is learned on earlier pseudo-outcomes and applied to future days. Shock, refill, recovery, queue, campaign state, and BUY markout state did not turn that weak average clue into a positive-lower-bound action.

The next action family should not rename or retune this BUY geometry test. Per the frozen roadmap, SELL should be studied separately with a repair-vs-trend-through competing-risk state and the action `skip one exposure-increasing add cycle`, while reducing BUY remains unchanged.
