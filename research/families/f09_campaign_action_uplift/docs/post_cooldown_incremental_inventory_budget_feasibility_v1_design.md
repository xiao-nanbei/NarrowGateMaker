# Post-Cooldown Incremental Inventory Budget Feasibility v1 - Design Draft

Last materially modified: 2026-08-01

Status: `single_day_mechanics_smoke_complete_not_registered`. This mutable design is not a frozen executable Spec, action identity, or permission to read economic outcomes. Frozen v1/v1.1 identities remain unchanged and require a new executable successor before another run.

## Latest Live Motivation And Scope Boundary

F10's diagnostic-only [`live_240h_loss_solution_routing_v1`](../../f10_live_replay_attribution/docs/live_240h_loss_solution_routing_v1_20260801.md) found that single-level SHORT campaigns were approximately flat in aggregate, while 257 multi-level SHORT campaigns contributed -11.1471 USDC. Every exact depth from two through seven units had negative aggregate PnL, with a sharply worse high-depth tail. This is observational motivation only: depth is endogenous and cannot select a budget, establish a treatment effect, or replace the mechanics-first gates below.

SELL opener weakness is outside this intervention's scope. A post-cooldown incremental inventory budget can only alter later exposure-increasing fills; it cannot improve the first order that opens a SHORT campaign. BUY and SELL support must remain separate, with SELL/SHORT as the preregistered primary mechanics slice only if that choice is frozen before reading replay outcomes.

The latest F10 q90 audit is a separate transport blocker, not part of the 240-hour loss attribution. Because q90 currently lacks a supported post-cancel recovery estimand, the next executable mechanics identity must either freeze q90 OFF in both arms or bind a separately passed recovery contract. It must not repair q90 and test an inventory budget in the same identity.

## Research Question

After the current cooldown releases normally, can the strategy preserve ordinary quote timing while limiting the additional exposure-increasing inventory accumulated by the same side?

The intervention object is an inventory budget:

\[
B_{s,\ell}
=
\text{maximum additional exposure-increasing fill units allowed after release}
\]

It is not another wall-clock, variance-clock, recovery threshold, or fixed number of blocked quote cycles. Reducing quotes remain governed by the current baseline in every path.

## Proposed Mechanics Estimand

At a preregistered post-cooldown release assignment time \(t_0\), define:

\[
U_{s,\ell}(t)
=
\sum_{i:t_0 < t_i \le t}
\mathbf 1\{i\text{ increases exposure on side }s\}
\frac{q_i}{\max(q_{\rm order},q_{\rm lot})}.
\]

The candidate permits a new exposure-increasing order only when its planned fill units fit inside the remaining budget. Partial fills debit only their realized units. ACK-pending orders retain their baseline fill race; the policy cannot retroactively cancel quantity already filled.

The feasibility stage must not yet choose a profitable budget. It should first measure whether outcome-blind candidate budgets create a useful middle region between one-cycle near-noop and stop-add participation shutdown.

## Resolved Mechanics Contract For A Future Spec

The first normal same-side cooldown expiry is the assignment surface. The campaign must still be non-flat and the baseline must otherwise be eligible to submit an exposure-increasing quote. The intervention ends at the first opposite-side fill, explicit lineage reset, process restart, or UTC-day censor. It is not reassigned after another same-side fill.

The state is denominated in planned/realized fill units:

\[
U_{\rm available}
=
B-U_{\rm consumed}-U_{\rm reserved}.
\]

- `consumed_units` increases only by realized exposure-increasing fill quantity divided by `max(order_size, lot_size)`;
- `reserved_units` covers the remaining quantity of every active or pending-cancel exposure-increasing order admitted after assignment;
- a cancel ACK or terminal order state releases its unfilled reservation;
- a partial fill transfers the corresponding quantity from reserved to consumed units;
- a new order is rejected before submission if its planned units exceed the available budget; one-order overshoot is forbidden;
- reducing orders never reserve or consume this budget and must follow the same path in all arms until paths diverge for another legitimate reason;
- the absolute inventory limit and emergency safety gates remain dominant; this research budget can only be tighter;
- any exposure-increasing active order already present at assignment makes the episode unsupported unless the producer proves it was admitted under the same ex-ante budget state. It cannot be retroactively cancelled to improve the candidate;
- a process restart is an unsupported censor in the historical mechanics audit. Daily fresh-start remains an explicit research limitation rather than a claim of continuous-live lineage parity.

SELL/SHORT is the preregistered primary mechanics side. BUY is a separate negative-control slice; sides cannot be pooled to rescue support. Opener orders are outside scope because the assignment occurs after cooldown release.

Candidate budgets are selected without reading value outcomes. On the unlimited-control Development path, compute the distribution of realized post-release exposure-increasing units by side and freeze the distinct whole unit values nearest p25/p50/p75, capped at three units. `B=infinity` is the control. `B=0` is excluded because it recreates the already tested stop-add-until-flat extreme. If the quantiles collapse to fewer than two nonzero candidates, the mechanics family closes for insufficient action resolution.

With a one-unit opener and no unsupported pre-existing exposure order, `B=1` permits one later unit and can cap the lineage at two units; `B=2` can cap it at three, and `B=3` at four. These are mechanics interpretations, not claims that the corresponding observed depth losses are recoverable.

No budget value may be selected from reward, PnL, markout, campaign terminal, or tail outcomes. A later action experiment would require a new frozen identity and known propensity.

## Mechanics-Only Outputs

The first executable identity, if registered later, should read no economic means or signs and should report:

- eligible lineages, days, sides, and consecutive-fill-unit support;
- candidate budget hit rate and final quote-action change rate;
- regenerated order, queue, cancel/ACK, and fill-path divergence;
- fill and activity retention;
- consumed, reserved, available, released and rejected unit accounting, with exact conservation at every lifecycle transition;
- incremental inventory-unit distribution and inventory-tail reach;
- reducing-side path invariance;
- blocker masking and unsupported/censored mass;
- Python/C++ state-machine parity;
- a design MDE derived only from predeclared baseline variance sufficient statistics.

The old 0.01546 USDC SELL MDE is contextual evidence from a different one-cycle denominator, not a gate for this family. Distributing the full observed multi-level SHORT net deficit gives only 0.00879 USDC per SHORT campaign and 0.00571 USDC per all campaigns. These are accounting scales, not causal upper bounds. A future identity must compute its MDE on the ex-ante first supported post-opener SELL-add/post-cooldown release denominator. It may not select realized multi-level losers as the denominator.

Only a budget region with nontrivial path changes, acceptable fill retention, and enough support to beat the design MDE may justify a later randomized action identity. Mechanics feasibility itself grants no Validation, action, shadow, or live authority.
