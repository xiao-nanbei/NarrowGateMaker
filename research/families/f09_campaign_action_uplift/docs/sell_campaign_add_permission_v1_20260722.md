# SELL Campaign Add Permission v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): the exact PnL, fill, campaign and tail values below are withdrawn with the superseded order denominator. The economic conclusion remains final for this action identity: blocking every later SELL add was participation shutdown rather than identified uplift. Repaired data does not reopen the family or its locked panels.

## Decision

`sell_campaign_add_permission_v1` failed its frozen Development gate and is closed. Validation and the family-specific sealed holdout were not read. The current live/replay baseline is unchanged.

This family asked a stronger question than the closed one-cycle SELL skip:

```text
surface: first baseline-eligible SELL add quote in an active short campaign
K0: baseline add permission
K1: block every later exposure-increasing SELL quote until flat
propensity: 50% / 50%, assigned once per campaign
BUY / reducing / size / max inventory / taker: unchanged
external reference: disabled (local M0 only)
```

The candidate had real mechanical force, but it was far too broad. It reduced tail exposure by removing nearly all subsequent SELL add fills rather than by identifying a stable positive-value action region.

## Frozen Identity

- Evidence split SHA256: `08e69b4b86cf8b6f5bc982765168f35c273435ed1efe83bf0b90c647c70bd665`
- Development action panel SHA256: `7f50983bde15fa16537318e6ec32dc90113e608e205ebf44fe34ca69ac4053b9`
- Evaluator manifest identity: `91793c68b55f8732e75ab4bf2c6053b0a6eed3ec2c133526bb8f387e5c2a0021`
- Config SHA256: `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`
- P3 SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- Queue-v3 q0.70 SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- AWS Tokyo latency SHA256: `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`

The evaluator used an outer chronological `30 train + 1 embargo + 7 test` schedule. Within every past-only training fold, action-specific Ridge nuisance models generated DR potential outcomes and a depth-2 honest tree used disjoint structure/estimation UTC days. Unsupported leaves fell back to baseline.

## Support

The behavior panel passed every pre-outcome support check:

| Measure | Result |
|---|---:|
| Independent campaigns | 1,734 |
| Development days | 56 |
| Baseline / candidate assignments | 883 / 851 |
| Behavior propensity | exactly 0.50 / 0.50 |
| Baseline subsequent SELL add fills | 986 |
| Candidate subsequent SELL add fills | 0 |
| Duplicate decisions | 0 |
| Maximum interventions per day/campaign | 1 |

Development OOF covered 764 campaigns on 25 future days. Policy ESS was 383, so the failure is not an overlap or sample-size artifact.

## Development Result

Higher is better for every outcome below.

| Outcome | DR uplift | UTC-day 95% interval | Positive days |
|---|---:|---:|---:|
| Decision-to-terminal reward | +0.00815 | [-0.02160, +0.03711] | 14/25 |
| Campaign-cost avoidance | -0.00025 | [-0.02848, +0.02859] | 12/25 |
| Negative-terminal protection | +0.03133 | [+0.00584, +0.06077] | 18/25 |
| Development-q10 protection | +0.02959 | [+0.00896, +0.05594] | 21/25 |
| Campaign-MAE avoidance | +0.08434 | [+0.04916, +0.12728] | 23/25 |
| Repair event | -0.00019 | [-0.00116, +0.00086] | 10/25 |
| Restricted repair-time utility | +257.15 s | [+161.35, +353.16] | 21/25 |
| Day-end censoring avoidance | -0.00019 | [-0.00119, +0.00081] | 10/25 |
| Subsequent SELL add fills | -1.09156 | [-1.37413, -0.82771] | 1/25 |

The learned policy selected K1 on 85.7% of OOF rows. Its estimated SELL add-fill retention was only 10.8%, versus the frozen minimum of 85%. Reward daily sign was 56%, barely above its sign gate, but the reward lower bound crossed zero and campaign-cost, repair, censoring, candidate-rate, and activity gates also failed.

The unified `action_defense_v1` scorecard was applied retrospectively after this already-closed family to validate the new scoring implementation. It reported total score `-0.2681`, positive tail contribution, mechanism score `-1.0`, `ranking_score=null`, and economic class `overbroad_risk_control`. Because the score profile did not exist when this family was frozen, this scorecard is diagnostic only; it does not alter the original Development decision.

No Development campaign reached the diagnostic `terminal <= -5 USDC` event, so this panel cannot establish extreme-tail efficacy.

## Interpretation

The positive downside and MAE estimates are real risk-control evidence: stopping all later SELL adds makes campaigns smaller and shorter. They are not evidence of fill-selection alpha. The action buys those improvements by removing about 89% of expected subsequent SELL add fills, while conditional reward remains uncertain.

Together with the earlier one-cycle skip result, the current evidence brackets the action-strength problem:

- one-cycle skip was nearly a no-op because only about 3.52% of eligible quotes directly filled;
- stop-until-flat is an overreaction that collapses activity;
- post-85 state extension had insufficient active support and unstable value.

The next family must therefore change the action mechanism, not retune this tree or lower its threshold on the consumed Development panel. A defensible candidate needs an endogenous, observable recovery event that restores add permission while preserving at least 85% of baseline activity. It requires a new preregistered randomized replay identity and cannot reuse this family name or open its locked Validation panel as a rescue attempt.

## Artifacts

- `MarketData/.../sell_campaign_add_permission_v1_20260722/development_cate_v1.summary.json`
- `MarketData/.../sell_campaign_add_permission_v1_20260722/development_cate_v1.report.md`
- `MarketData/.../sell_campaign_add_permission_v1_20260722/development_cate_v1.development_oof.csv`
- `MarketData/.../sell_campaign_add_permission_v1_20260722/development_cate_v1.artifact.json`
- `MarketData/.../sell_campaign_add_permission_v1_20260722/development_cate_v1.scorecard.json`
- `MarketData/.../sell_campaign_add_permission_v1_20260722/evaluator_preoutcome_experiment_manifest.json`
