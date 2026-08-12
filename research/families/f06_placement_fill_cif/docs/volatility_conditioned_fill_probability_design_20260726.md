# Placement And Active-Order Fill Surface Design

Date: 2026-07-26

Status: the original fixed-horizon direct-CIF experiment is frozen historical evidence. It has been superseded for new research by `placement_fill_full_curve_cif_v3`, which estimates the complete fill-before- cancel-ACK curve. The old 1s/5s/10s values are report-only cuts, not separate model targets or gates. The active-order KEEP/REPLACE surface remains separate and unbuilt. No live policy or parameter changed.

## Objective And Estimands

The next step is not to fit a surface from the aggregated paired-v2 CSV. It is to build a causal, per-decision lifecycle panel that supports two different estimands.

For a newly submitted placement action:

\[
P_F^\pi(a,h\mid x_0)
=
P(A_a\mid do(a),x_0)
P\!\left(
F<\min(A_a+h,C_{\mathrm{ACK}}^\pi)
\mid A_a,do(a),x_0
\right).
\]

For an already-active order, activation is conditioned on and the continuation estimand begins from its current queue, age, and path state. KEEP preserves the queue; REPLACE or cancel/re-enter resets it and is a lifecycle action, not a simple distance perturbation.

## Active-Touch Decomposition

For an activated order, the algebraic identity

\[
P(F_h\mid A,d,x)
=
P(T_h\mid A,d,x)
P(F_h\mid T_h,A,d,x)
\]

is valid, but the second factor is called `fill given active touch`, not queue fill. The paired matcher combines exact-price queue depletion, later-through fill, strictly-through forced fill, and deeper-path selection.

The lifecycle state machine is:

```text
Untouched
  -> ExactTouchedQueued
       -> ExactQueueFill
       -> LaterThroughFill
       -> CancelACK
  -> StrictThroughFill
  -> CancelACK
```

The direct dynamic-fill target is the total fill cumulative incidence:

\[
P_F(h)
=
\operatorname{CIF}_{\mathrm{exact\ queue}}(h)
+
\operatorname{CIF}_{\mathrm{through}}(h).
\]

Touch type and exact/through components are mechanism diagnostics. They are not separate policy objectives.

## Current Models And Their Boundaries

| Component | Current role | What it does not estimate |
|---|---|---|
| empirical P3 | 10-second distance-to-touch survival curve; supplies `delta_star` and `kappa_eff` to quote core | activation, exact-queue/through mechanisms, side/role state, or lifecycle fill |
| dynamic fill hazard | 100ms favorable/adverse fill hazard for active BUY orders | action-specific activation or full-lifecycle fill before cancel ACK |
| BUY fill-selection scorer | ranks the quality/toxicity of a possible BUY fill | probability that the order fills |
| causal-v5 13-head model | return, direction, volatility, and toxicity state | order fill probability |
| legacy quote-EV fill heads | archived research artifacts | active live execution probability |

`maker_fill_prob` is a replay gate/calibration scalar, not a state-conditioned prediction model. The current live configuration also uses the dynamic hazard's BUY adverse-value q90 as an explicitly user-directed cancel/re-entry trial. The new panel must either include that policy in the frozen baseline exit process or model it as a separate treatment; it cannot silently alter labels.

## Required Per-Decision Panel

The paired-v2 output is only side-by-distance-by-day sufficient statistics. It does not contain the rows needed for training. Create one wide row per side-decision with `current`, `minus_1_tick`, and `plus_1_tick` candidate blocks:

- experiment, split, UTC day, decision id, cohort id, and side;
- baseline inventory and role (`opener`, `add`, or `reducing`);
- feature-ready timestamp and every decision-visible feature timestamp;
- requested action price and effective tick-rounded price;
- action-specific activation result, activation timestamp, and GTX rejection;
- activation queue amount, source, exact-level identity, and queue path;
- first active-touch timestamp and type;
- first-fill timestamp, exact/through mechanism, and partial/full quantity path;
- cancel request, cancel ACK, and any fill during the ACK race;
- complete first-fill/cancel-ACK lifecycle outcomes, with report cuts derived from the frozen Development active-lifetime distribution;
- administrative censoring and its reason.

Every feature must satisfy `feature_ready_ts <= decision_ts`. The label endpoint may use future lifecycle events; no future realized cancel time may enter the feature vector.

## Placement Versus Continuation

Train two models rather than mixing risk origins:

1. `placement_fill_surface_v1`: a pre-submit model using only `x_0`. It includes action-specific activation and integrates over the queue distribution seen after ACK. Distance monotonicity is imposed only on candidates submitted at the same decision with the same lifecycle schedule.
2. `active_order_continuation_surface_v1`: conditions on an already active order and may use current queue, order age, refill/recovery path, and pending cancel state. KEEP and REPLACE are not constrained to be monotone in quoted distance because they own different queue positions and exposure paths.

## Volatility And Exposure Horizon

The current signal variance is absolute price variance per second:

\[
\sigma^2_{\mathrm{price},1s}
\quad [({\rm USDC/BTC})^2/s].
\]

At decision time, define an action-specific scheduled exposure using only the known policy schedule and the frozen cancel-latency distribution:

\[
H_{\mathrm{sched}}(a)
=
t_{\mathrm{scheduled\ request}}(a)-t_0
+E[L_{\mathrm{cancel\ ACK}}\mid x_0,s_{\mathrm{system}}].
\]

The realized label risk endpoint is:

\[
\min(t_0+h,t_{\mathrm{cancel\ ACK}}).
\]

Use the dimensionless placement distance

\[
z_a
=
\frac{d_{\mathrm{price},a}}
{\max(\sqrt{\sigma^2_{\mathrm{price},1s}H_{\mathrm{sched}}(a)},\epsilon)}.
\]

Retain raw distance, log volatility, and fast/slow volatility ratio. Equal `z` does not imply equal fill quality in jump, liquidation, or weak-refill regimes. Cancel/re-enter has two active stages and must retain its explicit state path; one scalar horizon is only a placement feature, not a complete representation.

## Queue And Storage Gates

Native visible-level queue is mandatory for formal queue-path diagnostics. The formal placement panel reconstructed the native exchange book directly and obtained valid queue paths for about 96.9% of all three action children. A missing queue path remains explicit; a fallback identity feature cannot manufacture queue information.

Before generating a full panel, require:

```text
free space >= 60 GiB reserve + 2.5 * estimated final panel size
```

Before the formal build, 53 rebuildable v10 window-cache files, retired top-level L2 compatibility links, and an unreferenced deep replay copy were removed. Free space increased enough to pass the reserve gate. The Development builder then admitted one compressed day partition at a time and deleted its large baseline trace. The 40-day final placement panel occupies about 208 MiB; the direct-CIF result occupies about 39 MiB.

## Model And Validation Contract

After the smoke passes, freeze the exact eligible-day intersection, chronological Development folds, embargo, Validation, and family-specific sealed holdout. Freeze feature timing, model candidates, calibration, prediction gates, and latency/queue/P3/runtime hashes before reading outcomes.

Fit the direct dynamic fill CIF first. Suitable candidates are a monotone discrete-time hazard, a monotone GAM, or a monotone gradient-boosted survival model. Report day-clustered Brier skill, log loss, calibration, PR/lift, monotonicity, side/role support, exact-versus-through CIF, and deep-queue coverage. Validation is read once only after the model identity is frozen.

## Non-Overlapping Action Value

Use USDC throughout:

\[
V(a\mid x)
=
E\!\left[
\sum_i
\frac{q_iP_i}{10^4}
(m_{i,H}-fee_{\mathrm{bps}})
-\Delta C_{\mathrm{campaign}}
-C_{\mathrm{explicit\ reset/churn}}
\mid do(a),x
\right].
\]

Maker-signed markout already contains spread capture. Queue already affects fill probability and must not be deducted again as an unspecified queue cost. Repair value already included in terminal campaign MTM must not be added twice.

Prediction is only a shadow diagnostic. If placement or continuation prediction passes its frozen gates, register a new known-propensity `action_execution_v1` experiment. Only randomized action uplift can decide whether KEEP, REPLACE, widen, re-center, or cancel/re-enter changes live behavior.

## Implementation Status (2026-07-26)

The formal machine-readable family contract is frozen in `docs/placement_fill_cif_v1_spec_20260726.json`. The streamed 40-day Development panel contains 664,335 placement cohorts with zero observed pathwise monotonicity violations, zero future-ready feature violations, and no native sequence gaps. See `docs/placement_fill_cif_v1_development_20260726.md`.

The side-specific direct CIF produced 2,950,881 chronological OOF predictions. All 18 side-by-role-by-horizon cells improved Brier error over the exposure-only baseline and passed ranking support, but all 18 failed the frozen absolute calibration-intercept gate. Consequently Validation and the sealed holdout remain unread, and the artifact is diagnostic only. The active-order KEEP/REPLACE estimand remains separate and unbuilt.

## Full-Curve Supersession (2026-07-27)

The next family now directly estimates

\[
P(T_{fill}\le t,\ T_{fill}<T_{cancelACK}\mid x,a)
\]

on a 100ms discrete-time grid. The action lifecycle supplies the cancel-ACK boundary; no universal 1s, 5s, or 10s horizon truncates training. Development active-lifetime p25/p50/p75 are 5.010s, 5.816s, and 7.900s and are used only as empirical report points. See `docs/placement_fill_full_curve_cif_v3_development_20260727.md`.

The current v3 result remains Development-only. Validation and sealed holdout are unread, and KEEP, REPLACE, and campaign repair retain independent risk origins and models.
