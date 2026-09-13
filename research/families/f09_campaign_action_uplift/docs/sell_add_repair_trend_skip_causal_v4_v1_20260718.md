# SELL Add Repair vs Trend-Through Skip Causal v4 v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): the exact causal-v4 DR, fill, repair, campaign and tail values below are withdrawn with their superseded denominator. The family-level no-promotion decision remains final for this exact one-cycle skip action; repaired data does not authorize retuning or opening its locked panels. Any successor requires a new action and frozen family identity.

## Decision

`sell_add_repair_trend_skip_causal_v4_v1` failed its Development gate and is closed. The 9-day Validation and 10-day sealed holdout were not replayed or read.

This closes the exact action family "skip one otherwise eligible exposure-increasing SELL add cycle." It does not show that every SELL campaign control is ineffective, but it does show that the frozen local shock/refill/recovery state could not identify a stable subset where this single-cycle skip improved both campaign value and the repair-vs-trend-through path.

No live, C++, configuration, order-size, reducing-side, inventory-limit, or rolling-baseline change is authorized.

## Frozen Design

| Item | Value |
|---|---|
| Surface | SELL exposure-increasing add while already short |
| Behavior actions | baseline 50%, skip one add cycle 50% |
| BUY | baseline only |
| Intervention unit | at most one per campaign |
| Size / reducing / inventory limit | unchanged |
| External reference | excluded |
| Competing-risk horizon | 30 minutes |
| Repair event | campaign reaches flat before trend-through |
| Trend-through event | execution trade reaches baseline SELL quote plus one tick before repair |
| Development | 100 previously inspected good days through 2026-06-23 |
| Embargo | 2026-06-24 |
| Validation | 2026-06-25 through 2026-07-03, locked |
| Embargo | 2026-07-04 |
| Sealed holdout | 10 good days from 2026-07-05 through 2026-07-15, locked |

The family froze a 37-feature local-only surface, action-specific Ridge nuisance models, past-only chronological nuisance folds, and a second chronological depth-2 honest treatment tree. The tree learned from cross-fitted doubly robust pseudo-outcomes for `skip - baseline`. It could select skip only above the frozen baseline trend-risk quantile and in a leaf where reward, campaign-cost avoidance, trend-through avoidance, and competing-risk utility agreed.

Artifact identities:

- family-spec SHA256: `bb5e2708941bcf07a257b778081ab126ab0481f48f7aaed968b465044db18bd4`
- empirical P3 SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- queue-v3 q0.70 SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- AWS Tokyo latency tape SHA256: `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`
- live-like config SHA256: `0c13d1533fe28c4f3bbaafdd85f62ed0b6074314e177b000356ab4b529a4aa9a`

## Replay Integrity

The Development replay produced 2,961 independent short campaigns:

| Check | Result |
|---|---:|
| baseline / skip rows | 1,506 / 1,455 |
| selected propensity | 0.50 on every row |
| interventions per campaign | exactly one |
| SELL / add rows | 2,961 / 2,961 |
| external-reference rows | 0 |
| repair / trend-through / competing-risk censored | 1,073 / 1,887 / 1 |
| campaign terminal censored | 8 |
| reward identity max error | `6.94e-18` USDC |

The randomized behavior mixture retained 99.94% of control fills. Campaign count was 1.0009x and absolute inventory time was 1.0007x. The aggregate randomized path lost `1.89 USDC` versus control across 100 days, so an unconditional skip rule is not an improvement. These aggregate mechanism figures are not the conditional-policy estimate.

Only 53 of 1,506 baseline intervention quotes filled on the selected cycle (3.52%). A skipped cycle therefore often changed no fill, while all later orders and campaign evolution still ran normally. This is an important mechanism result: a one-cycle action has a small first-order exposure surface under the current baseline.

## Development OOF

After the 50-day nuisance warmup and embargo, the policy layer evaluated 766 rows on 28 future days. It selected skip on only 4 rows:

| Support | Value |
|---|---:|
| baseline / skip logged rows | 393 / 373 |
| learned skip rows | 4 |
| learned skip rate | 0.52% |
| policy ESS | 391 |
| repair / trend-through events | 282 / 484 |

| Outcome, higher is better | DR uplift per decision | 95% day-cluster interval |
|---|---:|---:|
| decision-to-terminal reward | +0.000175 | [-0.000327, +0.000995] |
| campaign-cost avoidance | +0.000177 | [-0.000432, +0.000914] |
| negative-terminal protection | -0.000022 | [-0.000586, +0.000487] |
| development-q10 shortfall protection | -0.000087 | [-0.000404, +0.000131] |
| repair-first probability | -0.003671 | [-0.013285, +0.001602] |
| trend-through avoidance | -0.003689 | [-0.013020, +0.001558] |
| competing-risk utility | -0.007177 | [-0.025037, +0.002751] |
| intervention-fill probability | +0.000004 | [-0.000130, +0.000133] |

The reward point estimate is effectively zero and its lower bound is negative. More importantly, the lifecycle outcomes disagree with it: repair-first, trend-through avoidance, and the combined competing-risk utility are all negative.

The only supported tree leaf with a positive reward estimate had reward `+0.01018`, but repair-first was `-0.03454`, trend-through avoidance was `-0.03338`, and competing-risk utility was `-0.06781`. It was correctly rejected. No leaf was eligible to execute skip.

The learned skip rate of 0.52% was also below the frozen 3%-40% action budget. This was not repaired by lowering the risk threshold or loosening the leaf gates after outcomes were read.

## Interpretation

The experiment answered an action question, not merely a risk-ranking question. The state contained short inventory, campaign-so-far PnL/MAE/age/add count, exact-L2 queue and microprice state, adverse flow shock, depletion, refill, and recovery paths. Even with known 50/50 propensity, those features did not identify a stable region where skipping one SELL add cycle improved campaign value and the event path together.

Two practical conclusions survive:

1. A single skipped cycle is usually a very small intervention under the current baseline because the otherwise eligible quote fills only about 3.5% of the time.
2. A reward-only leaf can look favorable while repair and trend-through outcomes worsen. SELL campaign policy must keep competing-risk outcomes as co-primary gates.

Any future SELL family must have a new economic action and a new frozen identity. It must not reopen this family by changing the tree depth, the baseline-risk quantile, the event horizon, or the candidate-rate budget on the same locked panels.

## Artifacts

Canonical private artifacts live under:

`${NARROWGATE_RESULTS_DIR}/sell_add_repair_trend_skip_causal_v4_v1_20260718/`

Key files:

- `sell_add_repair_trend_skip_causal_v4_v1.family_spec.json`
- `sell_add_repair_trend_skip_causal_v4_v1.evidence_split.json`
- `sell_add_repair_trend_skip_causal_v4_v1_development.action_panel.csv`
- `sell_add_repair_trend_skip_causal_v4_v1_result.summary.json`
- `sell_add_repair_trend_skip_causal_v4_v1_result.artifact.json`
- `sell_add_repair_trend_skip_causal_v4_v1_result.policy_oof.csv`

The executable entrypoints are:

```bash
python -m models.audit.local_action_uplift --help
python -m models.audit.sell_repair_trend_skip_cate --help
```
