# Live Loss Attribution: 2026-07-19 to 2026-07-21

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: Historical/frozen live evidence; this is not the current runtime baseline.

## Identity and window

- Host: AWS Tokyo EC2, 2 vCPU / 4 GiB, Amazon Linux.
- Process window: `2026-07-19 17:58:43 UTC` through `2026-07-21 16:41:20 UTC` (46h 43m). The process restarted at the beginning, so a continuous 48-hour process window was not available.
- Config SHA256: `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`.
- P3 artifact SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`.
- Active model bundle: `saved_btcusdc_causal_v3_calonly_20260717`.
- Relevant controls: `fill_cooldown=85s`, `max_inventory=0.026 BTC`, `adverse_markout_threshold=3`, `adverse_markout_pause_threshold=5`.

The first campaign inherited a `-0.001 BTC` exchange position during startup sync. It earned `+0.153 USDC`; including it therefore does not create the loss diagnosis below.

## Accounting

Flat-to-flat campaign PnL was rebuilt directly from live fills:

```text
SELL cash flow = +price * quantity
BUY cash flow  = -price * quantity
campaign PnL   = sum(cash flow - commission)
```

The 490 closed campaigns produced:

| Metric | Value |
|---|---:|
| Fills | 1,504 |
| Net campaign PnL | -9.0126 USDC |
| Gross winning PnL | +8.9513 USDC |
| Gross losing PnL | -17.9639 USDC |
| Median campaign PnL | +0.0023 USDC |
| Commission | -0.2325 USDC |

The median campaign was slightly positive, but the negative tail was roughly twice the positive mass.

## Primary attribution: exposure-increasing adds

| Campaign family | Campaigns | Net PnL | Win rate | Median duration |
|---|---:|---:|---:|---:|
| No add | 394 | +0.1839 | 56.9% | 89.5s |
| Exactly 1 add | 45 | -2.4174 | 40.0% | 341.0s |
| Exactly 2 adds | 21 | -2.7598 | 23.8% | 560.7s |
| 3-4 adds | 15 | -1.3720 | 33.3% | 1,067.8s |
| 5+ adds | 15 | -2.6473 | 46.7% | 1,596.6s |

All 96 campaigns with at least one add lost `-9.1965 USDC`; the 394 no-add campaigns were slightly positive. This is the dominant loss location.

Side attribution sharpens it further:

| Side / family | Campaigns | Net PnL |
|---|---:|---:|
| LONG, no add | 201 | +0.4189 |
| LONG, with add | 48 | -3.1527 |
| SHORT, no add | 193 | -0.2350 |
| SHORT, with add | 48 | -6.0438 |

SHORT add campaigns account for about two thirds of total net loss. Observed inventory never exceeded `0.007 BTC`, so the configured `0.026 BTC` cap did not control this loss family.

## Lifecycle and adverse path

The campaign shadow tape shows monotonic deterioration as exposure fills accumulate:

| Exposure fills | Median age | Mean MAE | Mean max inventory |
|---|---:|---:|---:|
| 1 | 85.9s | -0.0363 | 0.0010 BTC |
| 2 | 339.6s | -0.1593 | 0.0020 BTC |
| 3 | 556.3s | -0.2521 | 0.0022 BTC |
| 4-5 | 1,064.7s | -0.2568 | 0.0028 BTC |
| 6+ | 1,593.2s | -0.5014 | 0.0035 BTC |

The first add occurred at a median age of about 203s for LONG and 154s for SHORT, well after the fixed 85-second cooldown. The cooldown delays re-entry, but does not establish that the adverse state has ended.

For SHORT campaigns, a first add after the market had already moved at least 5 bps against the opening short identified 20 campaigns: 19 lost money and their combined terminal PnL was `-4.5003 USDC`. For LONG, a first add after at least a 2 bps adverse move identified 26 campaigns with `-3.1057 USDC` total PnL, but also included nine winners. These are risk-stratification results, not counterfactual policy values.

## Fill quality

Receive-time mids from live quote decisions were used to calculate diagnostic maker-signed markout. Mean add-fill results were:

| Role | 5s | 30s | 60s | 300s |
|---|---:|---:|---:|---:|
| BUY add | -0.43 bps | -0.48 bps | -0.70 bps | -1.08 bps |
| SELL add | -0.85 bps | -1.19 bps | -0.65 bps | -1.26 bps |

Adds belonging to losing campaigns continued to deteriorate at 60-300 seconds, while adds in winning campaigns generally repaired over that horizon. This is consistent with a state-conditioned repair/rearm problem rather than a fixed seconds problem.

At quote time, the current cooldown paused about 54% of BUY-add and 61% of SELL-add decisions. After rearm, 70-79% of add decisions still carried the soft `markout` defense state, but median spread multiplier was only about 1.05. The explicit `adverse` reason appeared on only about 0.5% of BUY-add and 1.6% of SELL-add decisions.

The BUY fill-selection overlay appeared on only ten BUY-add fills. Nine belonged to losing campaigns, with poor 60-300 second markout. This sample is too small for promotion or rollback, but it is enough to require inventory-role-specific validation before that overlay can affect exposure-increasing BUY orders.

## Secondary attribution

- Seven circuit-breaker campaigns lost `-1.1631 USDC` in total.
- Nine IOC close fills paid `0.2325 USDC` of taker commission.
- Non-breaker campaigns still lost `-7.8495 USDC`.
- There were 62 post-only rejects and one startup sync adjustment, but no critical close failure in this process window.

Circuit-breaker calibration is a secondary issue. Several thresholds fell to `0.01-0.03 USDC`, which is too close to ordinary fill noise and deserves a separate horizon/floor audit, but it does not explain the main loss.

## Research follow-up

The attribution motivated a side-specific state-conditioned add-rearm family, with reducing quotes unchanged:

```text
surface: first eligible exposure-increasing quote after a fill cooldown
control: current 85s baseline rearm
candidate: continue blocking one add cycle until local repair state recovers
BUY and SELL: modeled and evaluated separately
size / reducing / max inventory: unchanged
external venue state: excluded from M0
```

Candidate state should include causal campaign adverse move, campaign age and add count, local aggressive flow, queue depletion, refill, microprice recovery, and current side markout state. Use one randomized intervention per campaign with shared replay randomness and estimate DR uplift. The strong SHORT `adverse move <= -5 bps` bucket is a useful support region, not a live threshold.

Do not infer a policy from this observational window alone. In particular, the PnL of the observed add campaign is not the counterfactual PnL that would have occurred had the add been skipped. The attribution identifies where to randomize and measure action uplift; it does not itself establish action causality.

That causal follow-up has now been completed on a frozen 56-day Development panel. SELL was directionally negative; BUY had a weak positive reward point estimate but insufficient active support, a confidence interval crossing zero, and worse repair/duration outcomes. Both side-specific identities were closed without reading Validation or sealed holdout. See [`state_conditioned_rearm_after85_v1_20260722.md`](../../f09_campaign_action_uplift/docs/state_conditioned_rearm_after85_v1_20260722.md).

A stronger SELL-only follow-up then randomized campaign-wide add permission: baseline versus blocking every later SELL add until flat. It had ample support and improved MAE/downside diagnostics, but selected the blocking action on 85.7% of chronological OOF rows and retained only 10.8% of expected SELL add fills. Reward uncertainty crossed zero and campaign-cost/repair gates failed. That family is also closed without opening Validation; see [`sell_campaign_add_permission_v1_20260722.md`](../../f09_campaign_action_uplift/docs/sell_campaign_add_permission_v1_20260722.md).

The subsequent queue-value family no longer tuned another adverse-state threshold. It fitted BUY/SELL cause-specific hazards for favorable fill, adverse fill, cancel, adverse price jump, campaign repair, and queue recovery, then selected K0 keep versus K1 cancel/re-enter from explicit order value. On 17 Development days, K1 retained 7.67% of eligible-order fills and 8.29% of toxic fills: toxic reduction was not superlinear relative to activity loss. Randomized ITT reward was `-0.01448 USDC/intervention`, with a fully negative pooled interval and a significantly harmful BUY result. This closes the exact cancel-until-state-exit action without reading later panels; see [`queue_value_net_hazard_keep_cancel_v2_20260722.md`](../../f07_active_order_continuation/docs/queue_value_net_hazard_keep_cancel_v2_20260722.md).
