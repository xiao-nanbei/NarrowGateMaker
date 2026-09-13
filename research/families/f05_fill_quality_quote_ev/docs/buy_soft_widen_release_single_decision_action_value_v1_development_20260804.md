# BUY Soft-Widen Release Single-Decision Action Value v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

Close `buy_soft_widen_release_single_decision_action_value_v1` on Development. Neither the normal hard-gate route nor the owner-risk route may register a full-path policy successor. The retired BUY fill-selection model, its `0.44` threshold, action, and shadow remain OFF and research-only.

This closes the tested successor action:

\[
\text{release one existing BUY soft-widen to spread multiplier }1.0
\]

It does not close all future BUY fill-quality research and does not claim that every BUY quote tightening is harmful.

## Estimand

The old selector estimated a filled-only outcome under the baseline policy. This identity instead froze a direct action contrast:

\[
\Delta Q^{\pi_0}(x_t)
=
Y_{t\rightarrow\text{day-end MTM}}(
  \text{release one soft-widen}
)
-
Y_{t\rightarrow\text{day-end MTM}}(
  \text{current baseline}
).
\]

At one pre-frozen canonical 10-second BUY opportunity, the candidate caps the already-active side-policy multiplier at `1.0`. It then returns immediately to the operational baseline. Queue, fills, inventory, cooldown, campaign state, and all continuation are regenerated. Opener and add are fitted and gated separately.

## Frozen Inputs

- Development panel: 40 retained days.
- Outcome-blind census: 142,891 baseline-eligible, exposure-increasing BUY decisions with an existing multiplier above `1.0`.
- Stable-hash sample: 480 opener and 480 add opportunities, 12 per role per day.
- Model: fixed Ridge with `alpha=10`, fixed causal feature list, no tuning.
- OOF: 16 initial training days followed by four expanding chronological six-day folds.
- Selection threshold: `0.0001 USDC/action`.
- Validation and sealed holdout were not read.

The frozen Spec SHA256 is `cf640c7636ad0358b7a86c425a21f99ad6f633237f2102bd17b7a00526ecffd3`.

## Mechanics

All 960 forks reached the exact target, retained the frozen role and policy permission, and changed the executable BUY price. Releasing the soft-widen moved the quote inward by a median 13 ticks in both roles:

| Role | Forks | Mean ticks inward | Median | Range |
|---|---:|---:|---:|---:|
| opener | 480 | 15.35 | 13 | 7-98 |
| add | 480 | 14.86 | 13 | 7-81 |

Python/C++ sampled mechanics parity was exact for one opener and one add, including target, role, permissions, multiplier, and effective price. Ten targeted contract/parity tests passed.

The action was mechanically real but economically sparse:

| Role | Nonzero terminal paths | Fraction | Positive | Negative |
|---|---:|---:|---:|---:|
| opener | 20/480 | 4.17% | 5 | 15 |
| add | 15/480 | 3.13% | 4 | 11 |

Total fill count was unchanged in all 960 forks. A quote change therefore usually did not change the eventual fill path; when it did, continuation could create a large terminal difference despite equal total fill counts.

## Direct-Value OOF

| Role | OOF selected | Mean USDC/action | Day-clustered 95% CI | USDC/OOF day | Day-clustered 95% CI | Positive days |
|---|---:|---:|---:|---:|---:|---:|
| opener | 92/288 | -0.00003478 | [-0.00009626, +0.00000217] | -0.0001333 | [-0.0003500, +0.0000083] | 1/24 |
| add | 187/288 | -0.00003636 | [-0.00007202, -0.00000656] | -0.0002833 | [-0.0005708, -0.0000500] | 0/24 |

Both roles passed mechanics and selected-sample support. Both failed the economic hard gates and the owner progression gates. The OOF prediction versus realized-value correlations were `-0.0562` for opener and `-0.0272` for add.

Unconditional point estimates were positive only because a few rare path forks were very large. Across all 480 actions per role, the day-clustered mean interval still crossed zero. The 99% winsorized means were negative: `-0.00003375 USDC/action` for opener and `-0.00002606 USDC/action` for add. These diagnostics do not replace the frozen OOF gates.

## Interpretation

The old action did not fail merely because its score threshold was poorly tuned. The tested economic lever has abundant quote-level support and moves prices materially, but a single release changes the terminal path only rarely. The frozen local causal features do not identify the rare beneficial continuations; the OOF selector instead selected negative average value.

Accordingly:

- do not retrain the old filled-only classifier;
- do not tune the old threshold or spread-multiplier cap;
- do not register a multi-decision BUY release policy from this result;
- do not open Validation or holdout;
- keep BUY fill-selection action and shadow disabled in live and backtest baseline;
- preserve the old model and this successor as research evidence only.

The 960 economic paths were generated by the current C++ full replay. A positive screen would have required authoritative Python full-path economics before promotion. Because both progression routes failed, sampled exact Python/C++ mechanics parity is sufficient for this conservative closure and does not grant C++ or live authority.

## Artifacts

- Frozen Spec: `research/families/f05_fill_quality_quote_ev/docs/buy_soft_widen_release_single_decision_action_value_v1_spec_20260804.json`
- Authoritative Development report: `${NARROWGATE_DATA_ROOT}/reports/buy_soft_widen_release_single_decision_action_value_v1_20260804/development_report.json` (`SHA256=7daffd6794e044e2864be89cc29c31aed7a5a0c58f1cb919d145196175a580f0`)
- Frozen opportunity manifest: `${NARROWGATE_DATA_ROOT}/reports/buy_soft_widen_release_single_decision_action_value_v1_20260804/opportunity_manifest.parquet` (`SHA256=70f6b11c99eaccb3fed4814522ec52405237e9ee828ec7cf09444a0d69a5428e`)
- OOF decisions: `${NARROWGATE_DATA_ROOT}/reports/buy_soft_widen_release_single_decision_action_value_v1_20260804/direct_action_value_oof.parquet` (`SHA256=3ac7e8f88a6c624e79cf1bfe41b08a54839908e3925ed7b115750920fea1884b`)
