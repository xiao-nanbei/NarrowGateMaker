# Multiscale EMA Boolean Cooldown Duration Policy v1

Date: 2026-08-09

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Identity: `multiscale_ema_boolean_cooldown_duration_policy_v1`

Family: F05 Fill Quality / Quote EV

Status: frozen before Development duration labels or economic results

## Research Question

This is a new exploratory identity. At the moment an exposure-increasing fill creates a BUY or SELL cooldown lineage, it asks whether the complete causal multiscale EMA state can select the total cooldown duration:

\[
f_s(x_t)=\tau,\qquad s\in\{\mathrm{BUY},\mathrm{SELL}\}.
\]

The requested chain is:

\[
\text{fill-time causal EMA state}
\rightarrow
\text{AND/OR/NOT Boolean rule policy}
\rightarrow
\text{total cooldown duration}
\rightarrow
\text{complete-path terminal value}.
\]

It is not an explanation of the current 85-second mechanism, a market-direction model, a search for one EMA pair, a linear vote across pairs, or an `ADD NOW` versus `WAIT ONE EPOCH` classifier. The closed v1/v1.1/v1.2 ADD/WAIT identities do not close this duration-policy question.

## Fill-Time Action Contract

The policy is evaluated once when a strategy-visible exposure-increasing fill creates a same-side cooldown-lineage revision. Exposure role is classified from inventory immediately before the fill:

- BUY is exposure-increasing when pre-fill inventory is non-negative.
- SELL is exposure-increasing when pre-fill inventory is non-positive.
- The triggering fill may be an opener or an add.
- The chosen duration blocks only later same-side exposure-increasing add permission.
- Reducing quotes always bypass this policy.
- Duration expiry restores permission; it never forces an order submission.

Every later eligible exposure-increasing fill creates a new immutable lineage revision and evaluates the policy once again. The deadline cannot be shortened, extended, or recomputed on ordinary quote ticks. A same-side reducing fill with zero reducing cooldown does not create a duration action and must not erase an active add cooldown. Opposite-side fills retain the frozen `opposite_fill_only` reset semantics.

The control is `CONTROL_85N`:

\[
\tau_0=85\text{s}\times
\frac{\text{same-side filled quantity}}{0.001\text{ BTC}}.
\]

Candidate fixed durations are total durations from the target fill and are not multiplied by \(n\).

The outcome-blind freezer reads the authoritative configuration directly and locks `clock_mode=wall_time`, `fill_cooldown_s=85`, quantity units as `max(order_size, lot_size)=0.001 BTC`, `opposite_fill_only` reset semantics, adaptive add cooldown OFF, reducing cooldown `0`, q90 action OFF, and BUY fill selector action OFF. The bound configuration SHA256 is `62a6add8d46c2695205e278ecb41bcaa16dc8199e683ef9114c21f6118b04e18`.

## EMA State

The price basis is one canonical decision-visible local mid. Trade price, microprice, external fair price, and a later BBO generation cannot be mixed into this identity. All feature-ready clocks must be no later than fill visibility.

The half-life bank is:

`0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256 seconds`.

All 45 legal fast/slow pairs are available, including non-adjacent pairs. Each pair carries ordering, last-cross direction, cross age, arrangement persistence, distance, provider-normalized distance, convergence/expansion, and EMA slopes. The surface also carries full EMA ordering and curvature state.

The model consumes the 360 atomic predicates frozen in the [outcome-blind input artifact](./multiscale_ema_boolean_cooldown_duration_policy_v1_outcome_blind_inputs_20260809.json). Multiple crosses overlap when their current ordering, age, persistence, and magnitude predicates overlap. They do not need to occur in the same millisecond.

## Boolean Model

BUY and SELL are trained and evaluated separately. The model is an ordered sparse Boolean rule list:

\[
C_\ell(x)=
\bigwedge_{j\in P_\ell}a_j(x)
\land
\bigwedge_{j\in N_\ell}\neg a_j(x).
\]

Multiple clauses with the same consequence form an OR. The first matching rule wins and the default is `CONTROL_85N`. Every one of the 45 EMA pairs can appear in a positive or negated literal. Linear Ridge, independent pair scores, majority votes, and post-outcome predicate thresholds are forbidden.

Complexity is selected only in nested chronological inner folds. The frozen search permits 1/2/3/4/6 literals per clause and 2/4/8 clauses, with a deterministic beam width of 256. A non-control clause must have support on at least four UTC days, ten campaign-side clusters, and 1% of campaign weight. The one-standard-error rule selects the simplest statistically tied policy.

## Outcome-Blind Duration Set

The duration vocabulary was frozen without reading duration value or PnL. It uses daily-fresh-start, day-equal-weighted Kaplan-Meier p25/p50/p75/p90 values from two operational lifecycle clocks: the next inventory-state change and the time until the incremental inventory is undone. When either event is not observed, its duration is right-censored at that path's natural UTC day end.

This is a daily-fresh-start Development distribution. It is not a cross-day physical duration distribution and must not be described or deployed as one. The `CONTROL_85N` empirical distribution is reported separately from the KM candidate construction; its day-equal-weighted p25/p50/p75/p90 is `85s/170s/170s/255s` for both BUY and SELL.

The corrected outcome-blind artifact SHA256 is `965400c6fe5408a6f49dd4253c96d6673d4621451af561a2bc7921591c2d7035`.

BUY candidates:

`CONTROL_85N, 79s, 173s, 223s, 356s, 640s, 709s, 2048s`.

SELL candidates:

`CONTROL_85N, 79s, 166s, 211s, 349s, 660s, 686s, 1748s`.

No duration may be added, removed, or adjusted after its Development value is read.

## Source Separation

2025 provider-normalized data supplies only unsupervised EMA predicate scales and normalization. It contains 66 admitted days and 112,090,884 side rows on the full normalized 100ms source grid. It supplies no PnL, cooldown label, queue, fill, or lifecycle authority.

All economic labels come from the 40-day 2026 native full-path replay. This is a historical Development panel already consumed by other research, not an independent confirmation set. Validation and sealed holdout remain unread.

## Complete Development Denominator

The formal denominator is not sampled. It is the Cartesian product:

\[
\boxed{
8{,}600\ \text{legal exposure-increasing fill opportunities}
\times
8\ \text{side-specific durations}
=68{,}800\ \text{full-path forks}
}.
\]

Every legal opportunity must appear with every duration for its side. Per-day, per-side, per-role sampling is forbidden. Work may be partitioned and checkpointed by day, side, campaign, opportunity, or duration, but a partial partition cannot become the formal panel. Campaign total analysis weight is one; this weighting never permits label rows to be omitted. If the full execution is too expensive, the result is `execution_blocker`, not a smaller estimand.

## Single-Action Labels

Each label changes only the target lineage duration. Every later lineage uses the frozen `CONTROL_85N` continuation policy:

\[
Q_s^{\pi_0}(x_t,\tau)
=
E[Y_{t\rightarrow\text{washout}}\mid x_t,\tau,\pi_0].
\]

The fork rebuilds activation, GTX rejection, queue, partial/full fills, cancel-request/ACK races, later quotes, inventory, fees, and campaign state. No second research assignment is allowed before washout. Washout requires flat inventory, inactive campaign, terminal descendant orders, no pending lifecycle, and no queue or hazard owner. Boundary rows without washout are right-censored; there is no forced terminal or arbitrary maximum waiting time.

The single-action label identity is deliberately separate from the learned policy identity. A policy applies the learned rule at every future legal lineage; therefore it must receive a new full-path replay rather than inherit the one-action label result.

## Nested Chronological Development

Outer folds reuse the hash-bound 40-day split: 16 initial history days followed by four non-overlapping six-day tests. Within each outer fit set, the final nine admissible days form three consecutive three-day inner tests. Every inner and outer split uses a one-calendar-day embargo and purges any training label whose washout reaches the first test assignment.

Rule discovery, duration consequence selection, and complexity selection occur only in inner folds. Outer folds execute the already selected procedure once. Within a campaign-side cluster, opportunity weights sum to one. Uncertainty is clustered by UTC day and campaign-side. BUY and SELL cannot obtain pooled permission.

## Progression

The sequence is deliberately strict:

1. Freeze the complete opportunity manifest without economic outcomes.
2. Implement and pass Python/C++ opportunity, duration, deadline, permission, lifecycle, and terminal-path parity.
3. Materialize all 68,800 single-action forks with atomic checkpoint admission.
4. Run nested chronological Development separately for BUY and SELL.
5. Serialize an immutable side policy only for a side passing every value, risk, support, and accounting gate.
6. Run a new policy-level daily-fresh-start full-path A/B against `CONTROL_85N`.
7. Run restart-aware continuous confirmation with identical restart and gap schedules and continuous accounting.
8. Only then may a separate F09 identity request Validation, sealed holdout, canary, or live authority.

Daily fresh-start is Development screening only. It resets cash, inventory, campaign, orders, queue, cooldown, and EMA warmup state at each UTC day and cannot claim continuous-live PnL. Continuous confirmation preserves state across ordinary UTC midnight and follows one frozen production restart contract for both arms.

## Current Permission

At this freeze point no Development duration economics, Validation, or sealed holdout has been read. There is no F09 registration, action, or live authority. Python-only labels, prediction quality, or a daily fresh-start point estimate cannot change that boundary.
