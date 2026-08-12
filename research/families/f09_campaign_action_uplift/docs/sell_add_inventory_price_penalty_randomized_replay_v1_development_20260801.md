# SELL-Add Inventory Price Penalty Randomized Replay v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

The frozen 40-day Development replay is complete. The authoritative decision is:

```text
decision=close_sell_add_inventory_price_penalty_on_development
```

The exact fixed price-penalty action is closed. Validation and sealed holdout were not read, `ranking_score=null`, and no action or live authority was created.

## Frozen Action

The candidate changed only an exposure-increasing SELL quote while the campaign was already SHORT:

\[
n_{\rm short}=\max\left(0,-q/0.001\right),
\qquad
\Delta_{\rm SELL-add}=\min(1.5,0.5n_{\rm short})\ \mathrm{bps}.
\]

- Flat SELL opener: unchanged.
- SHORT one unit: ask moved outward by 0.5 bps.
- SHORT two units: ask moved outward by 1.0 bps.
- SHORT three or more units: ask moved outward by 1.5 bps.
- Reducing BUY, order size, inventory limit, cooldowns, and other blockers: unchanged.
- BUY q90: disabled identically in both arms.
- Assignment: one deterministic 0.5/0.5 assignment per campaign, before the first final-eligible SELL-add path was generated.

The 0.5 bps step was an observational economic anchor near the recent SELL-add short-horizon loss scale, not a causal optimum. No parameter grid was searched.

## Evidence Contract

- Development denominator: 40 exact frozen UTC days.
- Primary panel: 24 explicit Grade-A days.
- Sensitivity panel: 16 explicit Grade-B days; excluded from the primary scorecard.
- Replay: Python-authoritative native snapshot/delta full path, regenerating quote, cancel/ACK, queue, fill, inventory, cooldown, and campaign paths.
- Market source identity: 1,222 files, 11,625,657,192 bytes, canonical SHA256 `27787b4aa129563c3d5bf173ef7705e44a511462de0c5989a8b1db7e8b457227`.
- Formal checkpoints: 40/40; 2,371 assignments, 40,200 total replay fills.
- Randomization: 1,181 control and 1,190 candidate assignments, exact recorded propensity 0.5, zero overlap or unsupported-mass failure.
- q90 evaluations, source gaps, invalid sequences, and sync censoring: zero.
- Full C++ tick-replay authority: false. C++ kernels remain supporting parity evidence only.
- Contract tests: 64 passed, zero failures, errors, or skips.
- Post-run repository regression: 1,199 passed, 4 skipped, zero failures.

## Grade-A Primary Result

All intervals below are UTC-day-clustered 95% bootstrap intervals. Value is USDC per randomized campaign assignment unless another unit is shown.

| Metric | Result | Frozen gate | Pass |
|---|---:|---:|:---:|
| Candidate assignment rate | 48.90% | exact 0.5 propensity/support | yes |
| Actual final-action change | 100.00% `[100.00%, 100.00%]` | LCB > 5% | yes |
| SELL-add fill retention | 60.98% | >= 90% | no |
| Activity retention | 73.74% | >= 75% | no |
| Cap truncation | 0.00% | <= 10% | yes |
| Full cap truncation | 0.00% | <= 1% | yes |
| Realized/requested penalty | 101.46% | >= 90% | yes |
| Assignment-to-terminal reward uplift | -0.001339 `[-0.011489, +0.009242]` | LCB > 0 | no |
| Reward-positive UTC days | 12/24 = 50.00% | >= 55% | no |
| Full-policy value | -0.1401 `[-0.8029, +0.4990]` USDC/day | LCB > 0 | no |
| Multi-level SHORT loss protection | -0.000351 `[-0.010248, +0.010289]` | LCB > 0 | no |
| Max-inventory avoidance | +0.000184 `[+0.000121, +0.000247]` BTC | LCB >= 0 | yes |
| Inventory-time avoidance | +0.1758 `[+0.1013, +0.2612]` BTC-s | diagnostic | positive |
| Campaign MAE avoidance | +0.013006 `[+0.001395, +0.025091]` USDC | LCB >= 0 | yes |
| Negative-terminal protection | +0.005034 `[-0.003563, +0.014167]` | LCB >= 0 | no |
| q10 shortfall protection | +0.004030 `[-0.001696, +0.009801]` | LCB >= 0 | no |

The scorecard had no support or validity failure. It failed the economic, retention, daily-sign, multi-level-loss, and required tail gates. The canonical classification is `risk_control_evidence_only`, not a strategy candidate.

## Mechanism Interpretation

The action was neither a no-op nor clipped away by the pair-spread cap. It changed every assigned candidate campaign and delivered essentially the full requested price movement. The failure is economic:

1. It removed about 39% of SELL-add fills and about 26% of aggregate activity.
2. It reduced Grade-A entry into multi-level SHORT from 45.51% in the randomized control arm to 36.28% in the candidate arm.
3. It reduced maximum inventory, inventory time, repair time, MAE, q10, and descriptive CVaR.
4. The remaining candidate multi-level paths were more adverse. Consequently, the randomized multi-level-loss contrast and the primary reward did not improve.

The arm-specific multi-level rates and conditional values are post-treatment mechanism diagnostics, not a new causal subgroup estimand. They cannot be used to select a narrower action after seeing this result.

Descriptive Grade-A tail levels did improve:

| Arm | q10 | CVaR10 |
|---|---:|---:|
| Control | -0.13235 | -0.31089 |
| Candidate | -0.10475 | -0.26616 |

Those improvements do not rescue the action because the prespecified day-clustered tail lower bounds crossed zero and mean assignment-to-terminal reward declined.

## Grade-B Sensitivity

Grade B reproduced the primary direction rather than rescuing it:

- reward uplift: -0.002904 `[-0.011452, +0.005987]` USDC/assignment;
- reward-positive days: 7/16 = 43.75%;
- SELL-add fill retention: 59.79%;
- activity retention: 74.83%;
- multi-level SHORT loss protection: -0.001715 `[-0.009428, +0.006565]` USDC/assignment;
- full-policy value: -0.2225 `[-0.6785, +0.2643]` USDC/day.

Grade B was sensitivity-only and never entered the primary scorecard.

## Decision Boundary

This result closes only the exact fixed curve `0.5/1.0/1.5 bps` with q90 OFF on the frozen F09 Development identity. It does not prove that all inventory-conditioned prices are useless. It does show that this economically material outward penalty behaves much more like participation suppression than selective value improvement.

Do not:

- tune the step down after reading this result;
- search a penalty grid on the consumed Development panel;
- open this identity's Validation or sealed holdout;
- combine it with a reducing-BUY change under the same treatment identity;
- deploy it to live.

If F09 continues, a more aggressive reducing BUY after multi-level SHORT must be a separate, preregistered economic action. It must preserve this result as a closed negative control and must not reuse the candidate's post-treatment subgroups as eligibility rules.

## Artifacts

Authoritative output root:

```text
${NARROWGATE_DATA_ROOT}/reports/
  sell_add_inventory_price_penalty_randomized_replay_v1_20260801/
  development/
```

Key SHA256 values:

- Frozen Spec bytes: `0d9d4c8ea30cab963e5e080fa78fdb1f5ad25063aa30929f05e899d659cbc08a`
- Canonical Spec identity: `083f20ba83850b61ea2db97247c5d80aa951fa72126b8017d3f82f5ea97a299f`
- `report.json`: `1ed7f52d323115213975a297edfa6094b105124202126d7c5198808b56f63739`
- `manifest.json`: `fb95694dfa3c99f08f7023107a64a1cdb6bb0f075911fe761533b456b23f9988`
- `campaign_randomized_panel.parquet`: `fcb2cdab02d04fca174765c3aeda85e7c1a42725f39bb42de040aa8f34119eb5`
- `campaign_event_journal.parquet`: `93cfd0c7bb14a01f1321183f360556d47253a4f543024a898a016c24b03ff4af`
- `daily_summary.csv`: `7d48927c57306633ac4feb6417d35d5c48bd80ec9619d74cc20d0397b5c941dc`
- `canonical_evidence.json`: `cce8483f2dc33a59808f986f03654075706c0f534c65dd50ea968d540d7e1222`
- `scorecard.json`: `409638868acf4c70ce3c408adce733f927f09bd9d48feb53955dc56b8dc93986`
