# Recovery-event rearm v1 (2026-07-22)

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): both frozen side-specific families remain closed. Exact reward, fill, campaign, duration and recovery numbers below are withdrawn with the old top-20/q0.70 order denominator. The support/action failure remains a valid no-promotion decision and does not authorize retuning this family on repaired data.

## Question

The fixed 85-second exposure-increasing cooldown is the current control. This family asks whether an add quote should remain blocked until a causal local recovery event is visible, while keeping reducing quotes, order size, maximum inventory, taker behavior, and external references unchanged.

This is a side-specific randomized action experiment, not a parameter sweep. Each campaign receives at most one 50/50 assignment:

- `baseline_rearm`: resume add quoting when the baseline cooldown ends;
- `continue_block_until_recovery`: keep blocking add quote cycles until the frozen recovery event occurs.

## Frozen recovery event

At the first baseline-eligible post-cooldown add decision, four causal path components are mapped to `[0, 1]`:

```text
shock_decay = clip(
    1 - positive_adverse_flow_1s
        / max(positive_adverse_flow_5s,
              positive_adverse_flow_since_fill,
              0.05),
    0,
    1,
)
refill = clip(refill_recovery_ratio, 0, 1)
microprice_recovery = clip(recovery_microprice_ratio, 0, 1)
queue_recovery = clip(refill_current_vs_start_ratio, 0, 1)
```

The composite is the equal-weight geometric mean with component epsilon `1e-6`. A row is valid only with a causal path, at least two L2 snapshots, and book age no greater than 2 seconds. The candidate acts when the valid score is below the side-specific threshold and releases when a later valid score reaches the threshold. Invalid entry states fall back to baseline; an already active candidate episode waits for a valid recovery observation.

`refill` measures recovery relative to the depleted level. `queue_recovery` measures current visible depth relative to the pre-shock starting depth. They are related but not interchangeable.

## Outcome-blind support selection

Thresholds were frozen before reading reward, PnL, markout, campaign cost, MAE, or duration. The selector loaded only causal state, assignment identity, and `intervention_fill_count`. It searched quantiles from 5% to 30%, targeted a 15% candidate rate, required at least 50 candidate rows on 10 days, and used a conservative fill-retention estimate that assumes every baseline fill attached to a blocked entry would be lost.

| Side | Frozen score threshold | Candidate rate | Conservative fills retention | Support |
|---|---:|---:|---:|---|
| SELL | 0.01600795 | 11.00% | 87.04% | pass |
| BUY | 0.02147895 | 15.03% | 87.49% | pass |

Both sides satisfy the requested 5%-30% candidate budget and the 85% formal fills-retention floor without using value outcomes to tune the threshold. BUY remains support-only in this run; no BUY outcome panel was opened.

## SELL Development result

SELL was the pre-declared first side. Its formal replay used 56 Development days, one intervention per campaign, exact 50/50 propensity, fresh-start daily state, the corrected operational baseline, empirical P3, queue-v3 q0.70, and the frozen AWS Tokyo latency profile.

Formal support remained inside budget:

- 1,644 campaign rows across 56 days;
- 180 active entry-state rows across 51 days;
- 97 active baseline and 83 effective candidate assignments;
- candidate rate 10.95%;
- conservative fills retention 87.19%;
- policy ESS 343 on 712 chronological OOF rows;
- zero unsupported mass and zero overlap violations.

The value result failed decisively:

| Metric | DR uplift | UTC-day 95% interval | Positive-day rate |
|---|---:|---:|---:|
| Reward (USDC/campaign) | -0.01560 | [-0.02996, -0.00377] | 24% |
| Campaign-cost avoidance | -0.01362 | [-0.02844, -0.00181] | 32% |
| Negative-terminal protection | -0.01360 | [-0.02699, -0.00321] | 28% |
| Development-q10 protection | -0.00886 | [-0.02166, +0.00004] | 36% |
| Campaign MAE avoidance | -0.01913 | [-0.03798, -0.00335] | 32% |
| Repair within 30 minutes | -0.00352 | [-0.01714, +0.00856] | 28% |
| Repair-time avoidance (seconds) | -9.27 | [-33.71, +13.66] | 44% |

There were no terminal events below -5 USDC in either logged arm, so that specific tail count is uninformative rather than favorable. Only 21.7% of effective candidate assignments blocked more than one quote cycle, showing that the frozen recovery threshold often released quickly. More importantly, the terminal and MAE results are already negative with confidence intervals below zero. This is not an activity-collapse failure: support and fills retention passed while conditional value worsened.

The `action_defense_v1` scorecard therefore sets `ranking_score=null` and closes `sell_recovery_event_rearm_v1` at Development. Validation and sealed holdout remain unread. The current live and replay baseline are unchanged.

## Interpretation and stop rule

The four variables are useful state descriptors, but this particular geometric score and action do not identify profitable delayed rearm. A low recovery score may describe a campaign already on a poor path without proving that withholding the next add quote improves that path. It may also suppress fills that would have repaired inventory.

Do not rescue this family by changing component weights, threshold, score aggregation, or candidate-rate target on the consumed Development panel. Any new recovery mechanism needs a new economic action, new family identity, and a new outcome-blind support freeze.

## Evidence identity

- family spec: `reports/recovery_event_rearm_v1_20260722/sell/frozen_family_spec.json`
- support grid: `reports/recovery_event_rearm_v1_20260722/sell/support_grid.csv`
- formal panel: `reports/recovery_event_rearm_v1_20260722/sell/development_v2.action_panel.csv`
- OPE summary: `reports/recovery_event_rearm_v1_20260722/sell/development_ope_v2.summary.json`
- scorecard: `reports/recovery_event_rearm_v1_20260722/sell/development_ope_v2.scorecard.json`
- config SHA: `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`
- P3 SHA: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- queue SHA: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- latency SHA: `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`
