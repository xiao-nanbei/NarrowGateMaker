# Queue-Value Cancel/Re-enter v3 Development Audit

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Superseding queue-input audit

The 2026-07-20 deep-book audit found that this experiment's top-20 `queue_ahead` fallback did not preserve the active-order queue mechanism. Therefore:

- the Development decision below remains valid as a conservative **do not promote** result;
- the reported DR estimates, state thresholds, queue features, and K1 mechanism attribution must not be reused as evidence about an exact active-price queue;
- this family is not eligible for threshold tuning, Validation access, or holdout access;
- a future queue-value family requires strict active-order-price L2 with zero fallback before its action identity is frozen.

On 2026-06-05, replacing retained top-20 L2 with strict native-snapshot deep-250 L2 changed fills from `1,595` to `2,062`, campaigns from `669` to `895`, and median initialized queue from `0.1162 BTC` to `0 BTC`. Only two decision IDs overlapped after the paths diverged. For the 52 previously eligible BUY-add entries, the deep book covered 48 price levels (`92.3%`); 29 of those levels were valid known-zero queues, but the top-20 fallback had assigned them positive queue. See [the deep active-order queue probe](deep_active_order_queue_probe_20260720.md).

## Decision

`queue_value_cancel_reenter_v3` did not pass its frozen Development gate.

- Validation was not opened.
- The sealed holdout was not opened.
- External-market M1 was not evaluated.
- No SPIBB artifact was trained.
- No live, shadow, C++, config, or baseline change is permitted from this result.

This closes the frozen action definition:

> On the first active BUY exposure-increasing add order that enters the calibrated adverse queue-value state, cancel the order, wait for the cancel ACK, and then return to the ordinary baseline quote path.

It does not reject queue-state modeling in general. It rejects the claim that this specific K1 action has positive conditional value under the frozen Development identity.

## Frozen identity

The action outcome was not read before the following identity was frozen:

| Component | Frozen value |
|---|---|
| Baseline | operational empirical-P3 baseline dated 2026-07-20 |
| Config SHA256 | `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601` |
| P3 delta star | `13.99908598` USDC/BTC (about 140 ticks at 0.1 USDC/BTC per tick) |
| P3 effective kappa | `0.06743811` per (USDC/BTC), the local negative log-touch slope |
| Queue artifact | queue calibration v3, `q0.70` |
| Queue artifact SHA256 | `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd` |
| REST latency profile | `private_not_distributed` |
| Book visibility profile | `private_not_distributed` |
| Action probabilities | K0 `0.50`, K1 `0.50` |
| Action seed | `20260720` |
| External reference | disabled |
| Replay engine | Python authoritative merged-clock replay |

The chronological panels were:

| Panel | Days | Access |
|---|---:|---|
| State fit | 24 | Used for state-model fit |
| State internal embargo | 1 | Excluded |
| State calibration | 5 | Used for calibration only |
| Transition embargo | 1 | Excluded |
| Action Development | 25 | Opened |
| Action embargo | 1 | Excluded |
| Validation | 9 | Locked and unread |
| Second embargo | 1 | Excluded |
| Sealed holdout | 9 | Locked and unread |

The frozen split SHA256 is `f85fe5d8a323332208458ff09cf9515e3afb5ee95e47821e9065cf039f882935`.

## Replay repair

The earlier v2 checkpoint was superseded before action evaluation because it did not provide a valid action identity:

- the state bundle was fit through all Development dates and applied backward;
- policy visibility did not use the EC2 receive-time delay profile;
- a K1 re-entry order did not inherit the intervention ID;
- fills after re-entry were omitted from the intervention fill value;
- the baseline config was not the current operational empirical-P3 identity.

v3 fixes the K1 lifecycle:

1. K1 requests cancellation of the active order.
2. The action waits for the original cancel ACK.
3. Ordinary baseline eligibility controls when re-entry is allowed.
4. The first baseline-authorized re-entry order inherits the intervention ID.
5. Re-entry fills and later cancel ACKs are attributed to the same action.
6. The episode exits only when the re-entry order is actually submitted, or the campaign flattens before re-entry.

In Development, 445 of 455 K1 campaigns submitted a re-entry order. The other 10 campaigns flattened before baseline re-entry became eligible. No re-entry was submitted before the original cancel ACK.

## State model

The local state model used 497,118 causal rows over 30 state-model days. No row had `feature_ready_ts > decision_ts`.

The BUY first-hit model passed the frozen calibration gate:

| Metric | Model | Constant null |
|---|---:|---:|
| Calibration rows | 43,804 | 43,804 |
| First-hit direction Brier | 0.19396 | 0.25002 |
| Selected adverse-state rate | 6.31% | n/a |
| Selected adverse probability | 0.3053 | n/a |
| Selected expected value | -0.1510 ticks | n/a |

This establishes short-horizon state ranking under the frozen replay model. It does not establish action value.

## Development randomized panel

The panel contains one intervention per campaign:

| Action | Rows |
|---|---:|
| K0 keep | 454 |
| K1 cancel then baseline re-enter | 455 |
| Total | 909 |

The reward target is decision-to-flat MTM, or day-end MTM when the campaign is censored. The decomposition

\[
\text{reward}
=
\text{fill value}
-
\text{campaign-cost residual}
-
\text{queue-reset cost}
\]

is an accounting identity. The campaign-cost residual is not separately causally identified. The maximum numerical identity error was `5.55e-17 USDC`.

K1 materially changed the intended mechanism:

| Metric | K0 | K1 |
|---|---:|---:|
| Intervention fills | 219 | 20 |
| Mean terminal MTM | -0.07755 | -0.06646 |
| Median campaign duration | 530.2 s | 427.8 s |
| p95 campaign duration | 4,712.2 s | 3,742.9 s |
| Mean campaign MAE | -0.27167 | -0.24194 |
| Terminal q10 | -0.39467 | -0.37994 |

These raw action means are descriptive only. They cannot replace the chronological DR contrast or the full replay comparison.

## DR action uplift

The formal paired contrast is `K1 - K0`:

| Metric | Estimate | 95% day-cluster interval | Daily positive |
|---|---:|---:|---:|
| Decision reward | +0.00443 USDC | [-0.04567, +0.04516] | 44.4% |
| Terminal MTM | +0.00444 USDC | [-0.04636, +0.04572] | 44.4% |
| Direct intervention fill | -0.45232 | [-0.56685, -0.31536] | 0.0% |

Numerical overlap was adequate for the evaluated folds:

- K1 effective sample size: `172`
- K0 effective sample size: `182`
- unsupported mass: `0`
- maximum importance weight: `2.0`

The reward and terminal lower bounds are negative, and the daily sign rate is below the frozen 55% requirement. The action therefore fails Development even before considering the missing tail denominator.

There were zero terminal campaigns at or below `-5 USDC` in either action. This is missing tail support, not evidence that K1 removes tail risk.

## Full replay comparison

The 50/50 randomized strategy mixture also underperformed the untouched baseline replay:

| Metric | Control | Randomized | Change |
|---|---:|---:|---:|
| Raw PnL | -111.27 | -119.88 | -8.62 USDC |
| Fills | 14,072 | 14,216 | 1.010x |
| BUY fills | 7,051 | 7,122 | 1.010x |
| SELL fills | 7,021 | 7,094 | 1.010x |
| Campaigns | 4,224 | 4,200 | 0.994x |
| Inventory time | 1.000x | 1.013x | +1.28% |
| Open campaigns | 15 | 17 | +2 |

Daily PnL delta was positive on 9 of 25 days and negative on 16. Its median was `-0.1214 USDC/day`.

The slight improvement in raw per-intervention means therefore did not survive the full sequential inventory path. This is consistent with action interference and campaign nonlinearity, and is another reason not to promote from row-level diagnostics.

## Data boundary

The historical book is causal 100 ms top-20 L2. The median active quote was about 219.5 ticks from BBO, with p90 about 470.5 ticks. Consequently, `queue_fraction_left` was effectively constant and the active order's true price-level queue was not directly observed.

This result rejects promotion of K1 under the frozen q0.70/top-20 replay identity. The later deep-book audit shows that it cannot identify the K1 mechanism on the true active-price queue. A new queue-value family first needs active-order-price L2 rebuilt under the same receive-visibility contract, with known-zero levels distinguished from out-of-range observations and no formal queue fallback.

## Conclusion

The calibrated local state contains predictive information, but:

\[
\text{state calibration}
\not\Rightarrow
\text{positive cancel/re-entry uplift}.
\]

Under the preregistered rule, this family stops at Development. It must not be rescued by threshold tuning on Validation, by opening the sealed holdout, or by adding external venues after seeing the failure.

The next research family should either:

1. wait for deep active-order-price L2 and define a genuinely queue-observable keep/cancel action; or
2. study an action whose state is observable in top-of-book/event data and whose reward does not depend on modeled deep queue position.

Artifacts are stored under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/queue_value_cancel_reenter_v3_20260720`
