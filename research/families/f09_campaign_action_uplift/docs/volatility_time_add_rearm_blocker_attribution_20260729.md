# Volatility-Time Add Rearm Blocker Attribution

Last materially modified: 2026-07-29

Status: Development/live-mechanics diagnostic only. This document does not reopen `volatility_time_add_rearm_feasibility_v1`, read Validation or sealed holdout, create an action experiment, or authorize live deployment.

## Question

Changing the fill-cooldown clock can change an order only when every other applicable policy and system gate permits the exposure-increasing quote. The relevant distinction is therefore:

\[
\text{mechanical timing effect}
=
\mathbf 1\{R_{QV}(t)\ne R_{85n}(t)\},
\]

versus:

\[
\text{action-effective timing effect}
=
\mathbf 1\{R_{QV}(t)\ne R_{85n}(t)\}
\mathbf 1\{G_{-fill\_cd}(t)=ready\}.
\]

The frozen v1 feasibility result measured the first quantity. It did not measure the second.

## Current-live diagnostic

The read-only audit used the last 48 hours ending at approximately 2026-07-28 21:57 UTC. Quote-decision counts use 58,164 side decisions. Stale book counts use 29,604 requote telemetry rows because stale-book handling returns before a quote-decision row is written.

| Observation | Count | Rate / interpretation |
|---|---:|---:|
| `fill_cd` side decisions | 9,294 | 15.98% |
| BUY / SELL `fill_cd` | 2,040 / 7,254 | SELL dominates current fill-cooldown occupancy |
| BUY q90 hazard hold | 3,346 | 5.75% of side decisions |
| `fill_cd` plus q90 hold | 91 | 0.98% of all `fill_cd`; 4.46% of BUY `fill_cd` |
| `fill_cd` plus any observable independent blocker | 95 | 1.02% of `fill_cd` rows |
| stale-book requote returns | 494 | 1.67% of requote telemetry |
| adverse exposure-blocked decisions | 24 | 0.04% of side decisions |
| consecutive-loss cooldown triggers | 26 | at most 780 seconds before overlap |
| adverse-markout pause triggers | 1 | one 127-second BUY latch |
| sync-adjust degrade triggers | 0 | no observed masking in this window |

The q90 action log contains 1,612 completed score-recovery holds in the sampled history. Hold age is strongly right-skewed: p50 213ms, p90 10.494s, p99 195.667s, and maximum 1,266.909s. It is therefore a material BUY-side concurrent state even though its overlap with the *existing* fill-cooldown rows is small.

These are observational co-occurrences under the 85-second baseline. They do not identify blocker overlap at counterfactual QV rearm times. In particular, an earlier QV release may land inside a hazard hold that did not overlap the baseline release, while a later release may avoid one.

## Policy interaction

Most `markout` reasons co-observed with `fill_cd` are price/size modifiers, not independent posting blocks. They should remain active and identical in both arms; their downstream response is part of the policy being evaluated.

The following mechanisms can suppress or alter the candidate effect:

| Mechanism | Effect on a variance-time study | Current replay status |
|---|---|---|
| fill cooldown | treatment clock itself | Python/C++ explicit reset-policy contract exists |
| adverse markout / defense | endogenous downstream state; may diverge after fills | represented in Python/C++ replay |
| stale book | exogenous operational support loss; no quote decision is emitted | represented approximately by replay book-age semantics |
| BUY q90 hazard hold | separate cancel/ACK/queue-reset action that can mask BUY rearm | deliberately rejected by formal replay; source v1 froze it off |
| consecutive-loss cooldown | global, reward-path-dependent state; can change after arm divergence | not replayed by the authoritative tick engines |
| sync-adjust degrade | live reconciliation state unavailable from historical market data | explicitly reported as not replayed |

The consecutive-loss cooldown cannot be treated as a fixed common random path in an action-value test: the candidate can change fills and realized losses, which can change whether the cooldown is triggered. Sync-adjust degrade is different; it is a system event and should be supplied by a frozen event tape, censored as unsupported operational time, or evaluated as a separate stress scenario.

## Consequence

Other gates do affect the proposed research, but current live evidence does not suggest that they make fill cooldown mechanically redundant. At existing baseline fill-cooldown timestamps, about 99% of rows have no simultaneously observable independent hard blocker. SELL has especially little overlap; BUY requires explicit treatment of the q90 hold and its long tail.

This does not rescue the frozen v1 identity. V1 already failed its upstream variance-clock availability gate, and adding blocker attribution can only reduce its effective action rate.

A future live-reproducible v2 feasibility identity must report, by side:

- baseline-ready, candidate-ready, earlier, later, and equal decisions;
- `unmasked_action_effective_rate` after removing fill cooldown from the gate conjunction;
- masking attribution for q90, markout/adverse, defense, stale book, inventory, campaign controls, global loss cooldown, and sync degrade;
- quote opportunity and would-fill opportunity affected by each timing change;
- Python/C++ parity for the exact gate order and reset state.

Before any randomized action identity is registered, the current BUY q90 cancel/ACK/re-entry lifecycle and the global consecutive-loss cooldown must be replayed, or the experiment must explicitly freeze them off and remain ineligible to authorize deployment on top of the current live policy.
