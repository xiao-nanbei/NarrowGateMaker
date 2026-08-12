# Live Loss Attribution Before the 2026-07-24 Restart

Last materially modified: 2026-07-27

## Window and accounting

The fixed analysis window is:

```text
2026-07-22 22:38:07 UTC
through
2026-07-24 22:38:07 UTC
```

This is the 48 hours immediately preceding the final production restart. The process also restarted at `2026-07-23 01:05:19`, `2026-07-24 21:37:58`, `2026-07-24 22:07:32`, and `2026-07-24 22:35:30`. Exchange inventory was carried across those restarts. `SYNC_ADJUST` rows are therefore state reconciliation, not fills.

The leading campaign already open at the left boundary and the trailing short campaign still open at the right boundary are excluded. The BUY q90 action was enabled only at the last short restart and produced no completed campaign inside this window, so the closed-campaign evidence below represents the pre-action operational baseline.

Flat-to-flat PnL is rebuilt directly from actual fills:

```text
SELL cash flow = +price * quantity
BUY cash flow  = -price * quantity
campaign PnL   = sum(cash flow - commission)
```

This avoids process-local realized-PnL resets at restart boundaries.

| Metric | Value |
|---|---:|
| Closed campaigns | 366 |
| Actual fills | 1,150 |
| Net terminal PnL | -3.6686 USDC |
| Gross winning PnL | +8.5512 USDC |
| Gross losing PnL | -12.2198 USDC |
| Campaign win rate | 53.83% |
| Median campaign PnL | +0.00265 USDC |
| Median campaign duration | 146.0s |
| IOC commission | 0.1568 USDC |

The ordinary campaign remained slightly profitable. A small negative tail consumed the positive mass.

## Primary loss location: exposure-increasing adds

| Add fills in campaign | Campaigns | Net PnL | Win rate | Median duration |
|---|---:|---:|---:|---:|
| 0 | 276 | +0.1468 | 57.61% | 107.9s |
| 1 | 46 | -1.7149 | 45.65% | 456.4s |
| 2 | 21 | -1.2095 | 33.33% | 729.7s |
| 3-4 | 13 | +0.0377 | 46.15% | 1,248.8s |
| 5+ | 10 | -0.9287 | 40.00% | 2,740.7s |

The 90 campaigns with at least one add lost `-3.8154 USDC`, more than the entire window loss. The 276 campaigns without an add earned `+0.1468 USDC`. Add count is not monotone in outcome: this rules out treating another fixed count, cooldown, or inventory threshold as the alpha.

| Side / family | Campaigns | Net PnL |
|---|---:|---:|
| LONG, no add | 138 | +0.0915 |
| LONG, with add | 58 | -2.3851 |
| SHORT, no add | 138 | +0.0553 |
| SHORT, with add | 32 | -1.4303 |

LONG adds dominated this window. In the earlier non-overlapping [`2026-07-19` to `2026-07-21` audit](live_48h_loss_attribution_20260722.md), SHORT adds dominated. Combining both windows gives:

| Family | Campaigns | Net PnL |
|---|---:|---:|
| No add | 670 | +0.3307 |
| At least one add | 186 | -13.0119 |
| LONG with add | 106 | -5.5378 |
| SHORT with add | 80 | -7.4741 |

The stable mechanism is therefore add eligibility after inventory already exists, not a permanently bad BUY or SELL side.

## Tail and response-path evidence

The campaign q10 threshold was `-0.09225 USDC`. The 37 q10 campaigns lost `-7.8729 USDC`; 81.08% contained an add and 62.16% were LONG.

Maker-signed markout uses the first logged receive-time quote mid at or after each horizon. Because the quote clock is normally 5-10 seconds, the 5-second column is a coarse diagnostic rather than an exact 5-second BBO observation.

| Add-fill campaign class | 5s | 20s | 30s | 60s | 300s |
|---|---:|---:|---:|---:|---:|
| Non-tail | -0.334 | +0.225 | +0.061 | +0.398 | +1.500 bps |
| q10 tail | -0.590 | -0.816 | -1.003 | -1.849 | -5.329 bps |

The useful separation is not an instantaneous 5-second sign. It emerges in the 20-300 second shock, refill, recovery, and repair path.

| Side / campaign outcome | 20s | 60s | 300s |
|---|---:|---:|---:|
| BUY add, winner | +0.354 | +0.689 | +3.741 bps |
| BUY add, loser | -0.390 | -1.053 | -5.034 bps |
| SELL add, winner | +0.499 | +0.718 | +2.949 bps |
| SELL add, loser | -0.952 | -1.754 | -3.450 bps |

The median first-add move from the opener was more adverse for losing campaigns:

| Side | Winning campaigns | Losing campaigns |
|---|---:|---:|
| LONG | -1.60 bps | -4.09 bps |
| SHORT | +0.14 bps | -2.49 bps |

That move alone is not monotone enough to be a policy threshold. It is useful only as one state variable in a path-conditioned marginal-add model.

## System amplifiers, not the primary alpha gap

Thirteen circuit breakers fired in the window. Twelve belong to complete campaigns:

| Breaker group | Campaigns | Net PnL |
|---|---:|---:|
| Complete breaker campaigns | 12 | -1.6486 USDC |
| Breaker campaigns with adds | 7 | -1.4738 USDC |
| Non-breaker add campaigns | 83 | -2.3416 USDC |
| Non-breaker no-add campaigns | 270 | +0.3216 USDC |

Six breaker campaigns escalated through three GTX rejections to IOC and lost `-0.9677 USDC`; their IOC commission was `0.1568 USDC`. Breaker calibration and maker-close execution can amplify a tail, but add campaigns remain negative without a breaker.

There were 114 stale-book warnings. Eighty-eight occurred inside 44 complete campaigns whose combined PnL was `-1.9876 USDC`. This is not causal attribution: long campaigns mechanically have more opportunity to overlap an hourly feed interruption. Non-stale add campaigns still lost `-2.1162 USDC`.

## Implication for the current BUY q90 live trial

`buy_exposure_adverse_q90_cancel_reenter_v1` acts on BUY exposure-increasing orders, which includes both an opener and an add. The pre-restart evidence supports role-specific accounting:

```text
BUY opener action
BUY add action
BUY reducing baseline
```

The action log already records `inventory_role`. At `2026-07-24 23:25:19 UTC`, it contained nine completed cancel/recovery pairs: seven opener and two add. Outcome joins must continue to report candidate, cancel, re-entry, would-fill, realized fill, and campaign outcome separately for those roles. The positive no-add campaign aggregate does not prove that every opener is good, but it prevents an opener-heavy cancel count from being interpreted as evidence that the add loss mechanism was fixed. Two add actions are also far below any economic-support threshold.

## Next alpha direction

The next experiment should pre-register a new information family:

```text
family: cross_venue_marginal_add_attribution_m1_v1
surface: baseline-eligible exposure-increasing add while inventory != 0
roles excluded: opener and reducing
sides: BUY and SELL modeled and gated separately
```

The economic hypothesis is:

```text
global-confirmed repricing
  + Binance bridge has not fully absorbed it
  + weak local refill/recovery
  -> trend-through add, high marginal campaign cost

local Binance shock
  + no independent-venue confirmation
  + strong refill/microprice/queue recovery
  -> local absorption, potentially valuable add
```

First compare prediction sets without creating an action:

```text
M0:
  native 100ms L2/queue path
  public aggTrade-visible flow
  signed move since opener/last add
  campaign age, inventory, MAE, add ordinal
  reducing-order repair state

M1 = M0 plus:
  Bitget/Bybit/OKX spot consensus
  Bitget/Bybit/OKX perpetual consensus
  spot/perp agreement and dispersion
  Binance BTCUSDT bridge residual
  USDTUSDC conversion state
  receive/feature-ready freshness
```

Primary labels are marginal decision-to-terminal campaign cost and q10-tail risk. The 20s/60s/300s maker-signed response path and time-to-repair are secondary mechanism labels. External historical 1-second states are adequate for this 20-300 second campaign question; they are not used to claim a 10-100ms cancel alpha.

M1 may register a later randomized action only if it adds stable chronological information over M0 for the same side, survives leave-one-venue-out and latency/freshness stress, and passes calibration plus within-day top-budget precision. Any later action must use one 50/50 intervention per campaign and the frozen `action_execution_selective_v2` scorecard. No threshold is selected from this 48-hour attribution.

This is a genuinely new information hypothesis. It does not rename the closed fixed rearm, one-cycle skip, one-tick widen, or local keep/cancel families.
