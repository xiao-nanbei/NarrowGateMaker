# Multiscale EMA ADD-WAIT Incremental Value v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: Prediction-only F05 identity preregistered; Development outcomes unread.

## Research Boundary

This identity asks whether a causal multiscale EMA surface adds chronological out-of-fold information beyond campaign state and existing local/trend features for the side-specific estimand:

```text
delta_Q = Q_pi0(ADD NOW) - Q_pi0(WAIT ONE EXTERNAL EPOCH)
```

It is not an EMA crossover strategy and does not replace the current cooldown. It cannot register F09, change an order, or authorize live. SELL and BUY are fit, evaluated, and gated separately. A pooled result cannot promote either side.

The exact machine-readable contract is `multiscale_ema_add_wait_incremental_value_v1_spec_20260809.json`. An execution amendment binding the exact Spec, runner, fork implementation, operational baseline, model/P3, queue, latency semantics, and tests is mandatory before any fork label is generated; an empty or partial artifact list fails closed.

## Frozen Denominator

The denominator is copied from the current authoritative 40-day replay baseline identity and its bound execution plan, without reusing that baseline's economic result. The execution-plan SHA256 is `5c47033e67b75a9cbef6c336825dbea713b117c54674828d956f6626886fb7d4`.

The ordered Development days are:

```text
2026-04-17  2026-04-18  2026-04-19  2026-04-20  2026-04-22  2026-04-23
2026-05-01  2026-05-02  2026-05-03  2026-05-04  2026-05-05  2026-05-06
2026-05-13  2026-05-29  2026-05-30  2026-05-31
2026-06-02  2026-06-03  2026-06-05  2026-06-06  2026-06-07  2026-06-08
2026-06-09  2026-06-10  2026-06-11  2026-06-12  2026-06-13  2026-06-14
2026-06-15  2026-06-16  2026-06-17  2026-06-18  2026-06-19  2026-06-20
2026-06-21  2026-06-22  2026-06-23  2026-06-24  2026-06-25  2026-06-26
```

These are historical Development days already consumed by other research. They are not independent confirmation. Validation and sealed holdout remain unread and are forbidden to this identity.

## Eligible Opportunity

An opportunity is side-specific and add-only. Opener, reducing, and cross-zero orders are excluded. It must be baseline-shadow eligible, market-ready, causal, and free of inherited same-side active or pending order state.

Two cooldown phases are retained:

- `COOLDOWN_ACTIVE`: all non-cooldown permissions allow add, but the current 85-second cooldown makes the baseline action `WAIT ONE EXTERNAL EPOCH`.
- `COOLDOWN_EXPIRED`: the cooldown and every other baseline permission allow add, so the baseline action is `ADD NOW`.

At most two opportunities are sampled per UTC-day x side x cooldown-phase cell. Sampling is outcome-blind and stable: first retain the smallest-hash row for each campaign-side and phase, then retain the two smallest campaign representatives in each cell. The maximum is 320 fork opportunities. Sample membership must be frozen before fork outcomes are generated.

## External Epoch

The next decision clock is external to both fork arms. `G_market` contains the decision-visible BBO, L2, last visible execution-trade index, feature-ready, prediction, and paired snapshot-mid content. Source cursors may not regress; the mid is encoded as an integer half-tick and may move in either direction. The market event index is only a tape locator and is not part of content identity.

```text
tau_plus = first u > t such that
           G_market(u) is strictly after G_market(t),
           scheduled_requote(u) is true, and
           readiness(u) is true
```

Submit, cancel, ACK, fill, inventory, campaign, queue, order-age, cursor, and hazard changes caused by a fork do not advance this clock. A forced decision caused by the candidate cannot release `WAIT`.

At assignment:

- `ADD NOW` submits the exact baseline add, including executable price, quantity, GTX, activation, queue, and latency semantics.
- `WAIT ONE EXTERNAL EPOCH` does not submit that add.
- At the same `tau_plus`, both arms resume the frozen continuation policy `pi0`.

## Baseline Fallback

The economic threshold is frozen at `0.0001 USDC` per opportunity. It is a governance threshold, not a market constant and not a value selected from this panel.

- While cooldown is active, depart from baseline WAIT only if the simultaneous LCB of `Q(ADD)-Q(WAIT)` is strictly above `0.0001`.
- After cooldown expires, depart from baseline ADD only if the simultaneous UCB of `Q(ADD)-Q(WAIT)` is strictly below `-0.0001`.
- Missing, unsupported, or uncertain evidence always returns the current baseline action for that cooldown phase.

This rule is diagnostic in F05. It is not executed here.

## Joint Washout

Order terminal is not sufficient. Assignment ownership persists until both arms share a common economic washout:

- both inventories are flat within the operational campaign epsilon of `1e-10 BTC`, and both campaign states are inactive;
- every descendant order is exchange-terminal;
- active, pending-submit, pending-cancel, and pending-ACK counts are zero;
- old queue cursor and hazard ownership are zero;
- no second assignment has occurred.

If one arm becomes flat first, it is quarantined and cannot start another campaign while the other arm remains unwashed. A terminal race that reopens inventory permits reducing-only recovery. EMA state continues to update from the common external market path.

There is no arbitrary maximum wait. If the data boundary arrives first, the row is right-censored and excluded from primary training and OOF scoring. The runner must report contemporaneous mid-MTM and executable-BBO liquidation valuations, remaining inventory, and lifecycle state. Those diagnostics do not claim to bound eventual campaign terminal value.

## EMA Surface

The fixed half-life basis is:

```text
0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256 seconds
```

Every update uses the canonical mid and feature-ready clock from the same immutable quote snapshot:

```text
EMA_h(t_i) = exp(-ln(2) * dt / h) * EMA_h(t_i-1)
           + (1 - exp(-ln(2) * dt / h)) * mid(t_i)
```

M0 contains campaign state plus existing causal local/trend, flow, refill, and prospective queue fields. M1 contains every M0 field plus continuous EMA relative levels, velocities, adjacent-scale distances, cross ages, ordering persistence, curvature, and causal-volatility-normalized distances. BUY uses the natural sign; SELL uses the opposite sign so positive means favorable to the add side. Hard golden/death-cross signs are diagnostics only.

M0 and M1 must use identical row IDs, targets, campaign weights, and test denominators. If an M1 feature is unsupported, that row is excluded from both models with an explicit reason.

## Fixed Model

Each side uses a separate fixed Ridge regression:

```text
alpha = 10.0
fit_intercept = true
solver = svd
```

There is no hyperparameter search, target transform, or target winsorization. Continuous values use outer-train weighted-median imputation plus fixed missing indicators, then outer-train weighted-median/IQR scaling clipped to `[-8, 8]`. `cooldown_phase` has exactly the two frozen categories. All preprocessing is fit on the outer-train rows only.

A campaign's total training weight is exactly one across all retained forks:

```text
row_weight = 1 / retained_fork_count_for_campaign
```

Intervals cluster first by UTC day and then by campaign within day. Fork rows from a long campaign never become independent effective samples.

## Chronological OOF

The first 16 days form the initial history. The remaining 24 days are four non-overlapping six-day test folds:

| Fold | Calendar embargo | Test days |
|---|---|---|
| 1 | 2026-06-01 | 2026-06-02, 03, 05, 06, 07, 08 |
| 2 | 2026-06-08 | 2026-06-09, 10, 11, 12, 13, 14 |
| 3 | 2026-06-14 | 2026-06-15, 16, 17, 18, 19, 20 |
| 4 | 2026-06-20 | 2026-06-21, 22, 23, 24, 25, 26 |

For every fold, the preceding calendar UTC day is embargoed. After that day filter, any training row that is right-censored or whose joint washout is at or after the first test assignment timestamp is purged. This label-end purge is mandatory even when daily fresh-start makes overlap unlikely.

The primary incremental gates are side-specific day/campaign-clustered LCBs strictly above zero for both OOF squared-error reduction and OOF absolute-error reduction (`loss_M0 - loss_M1`). The diagnostic departure set additionally uses the frozen simultaneous band and economic threshold. One side cannot rescue the other.

## Permissions

At freeze time:

- Development outcomes: unread.
- Validation: unread and forbidden.
- Sealed holdout: unread and forbidden.
- F09 registration: false.
- Order/action authority: false.
- Live authority: false.

Even a side-specific M1 pass only permits proposing a new, separately frozen F09 identity. It does not create or authorize that successor automatically. After any Development outcome is read, the EMA basis, actions, external clock, washout, censoring, sample cap, model, preprocessing, economic threshold, folds, embargo, purge, bootstrap, and side separation cannot be changed under this identity.
