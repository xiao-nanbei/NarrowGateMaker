# Causal Multichannel Boolean Cooldown V2 Interpretation Errata

Date: 2026-08-12

Audited identity: `causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1`

This errata supersedes the **interpretation** in the historical Development closure report. It does not alter the admitted OOF rows, selected policies, numeric estimates, strict-native failure receipt, Validation status, or sealed holdout status.

## Correct Status

The authoritative conclusion is:

```text
strict-native historical label path:
  blocked before a formal economic panel because same-millisecond trade/book
  order is not identifiable

owner modeled-queue path:
  limited one-shot nested OOF completed without exchange queue authority
  and produced no supported M0 side

combined exact no-policy closure:
  not supported
```

Therefore the project may say:

> Strict-native labels were blocked, and the limited modeled-queue one-shot search did not pass outer OOF.

It may not say that strict-native research produced a no-policy result, that all multichannel Boolean cooldown policies failed, or that repeated-policy PnL was tested.

## Census Correction

The source panel executed:

\[
8{,}600\times8=68{,}800
\]

actual duration arms. The historical report's `120,400` value is the size of a dense side-action union matrix:

\[
8{,}600\times14=120{,}400
\]

It contains structural slots that do not belong to the opportunity's side and must not be described as executed arms.

## Frozen Gate Versus Implemented Gate

The frozen execution amendment required:

1. M0 absolute post-OOF gate.
2. Paired M1 minus M0 LCB plus M1 absolute gate.
3. Paired M2 minus M1 LCB plus M2 absolute gate.
4. Bonferroni familywise control over the two incremental comparisons.

The admitted report instead evaluated 14 independent `side x panel x feature-block` cells against `CONTROL_85N`. The paired M1 minus M0 and M2 minus M1 contrasts were not produced. The continuous comparator was also not compared against Boolean state as a paired incremental estimand.

Both BUY and SELL M0 absolute gates failed. Consequently `supported_sides=[]` remains the correct no-advance result under the frozen M0-first hierarchy. However, this result cannot support claims that M1 adds no value over M0, M2 adds no value over M1, or continuous state adds no value over Boolean state.

## Search Scope

The admitted outer OOF selected 56 policies:

| Structure | Count |
|---|---:|
| One literal | 18 |
| Two literals in one AND clause | 9 |
| Two one-literal clauses joined by OR | 29 |
| Multiple ordered rules | 0 |

All 56 policies contained exactly one rule. Large feature blocks were reduced outcome-blind to 32 predicates before bounded search. This is a valid limited search, but it is not evidence about the full predicate universe or a general high-order ordered AND/OR/NOT policy class.

## OOF Interpretation

All 56 selected inner-fold candidates had positive point estimates, ranging from `+0.000687` to `+0.005201` USDC per campaign-weighted opportunity. In outer OOF, 11 of 14 cell estimates turned negative, three remained weakly positive, and all 14 lower bounds were below zero. This is strong evidence of winner's curse and chronological instability in the tested search.

It is not leakage evidence. Expanding folds were chronological, train and test days did not overlap, purge audits passed, and no outer-test outcome was found in candidate selection.

The actual outer-test denominators were:

| Panel | Eligible label days | Outer-test days per side |
|---|---:|---:|
| Prefix 40 R0/M0/M1 | 40 | 28 |
| Prefix 33 M2 common support | 33 | 21 |

The interval is a campaign-equal weighted UTC-day cluster sandwich interval using a normal 1.96 critical value. It is not a day-cluster bootstrap, does not apply a small-cluster t correction, and is not simultaneous over 14 cells.

For Prefix-33 M2, equal-day sensitivity changes the point-estimate sign:

| Side | Campaign-weighted | Equal-day |
|---|---:|---:|
| BUY | -0.000098 | +0.000423 |
| SELL | -0.000222 | +0.000974 |

The lower-bound no-pass is not overturned, but the statement "average effect is negative" is not robust to day weighting.

## Exact Closed Scope

The current evidence closes only:

```text
existing modeled-queue one-shot labels
+ side-specific 8-duration vocabulary
+ large-block 32-predicate outcome-blind subset
+ single-rule bounded Boolean candidates selected by the admitted search
+ current chronological outer folds and current weak interval
```

It does not close:

```text
strict receive-time or otherwise order-identified queue labels
paired M1-minus-M0 or M2-minus-M1 feature-family increments
continuous-minus-Boolean incremental value
broader multi-rule ordered Boolean policies
D+1 or continuous treatment of censoring
repeated-policy full-path economics
restart-aware or live transport evidence
```

No unified policy is frozen. Repeated-policy, action, and live authority remain false. Validation and sealed holdout remain unread.

Machine-readable audit: [`interpretation errata JSON`](causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1_interpretation_errata_20260812.json).
