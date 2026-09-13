# Side-specific action uplift on a frozen existing-data split

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-18

> Current status (2026-07-27): the family-level decision not to promote these fixed local actions remains final. Its exact DR uplift, PnL, fill, campaign, tail and winner-ordering values are withdrawn because the action denominator predates the normalized L2, repaired trade-side and time/unit identities. Data repair does not reopen this family; any successor requires a new action, family ID, frozen split and score profile.

## Decision

The existing retained good-day universe is sufficient to reject the first fixed local add-quote action family. New good days are not required to rescue this family.

No BUY or SELL action passed development and validation. The family remains research-only, the live baseline is unchanged, and the 20-day family-specific holdout remains sealed.

## Method change

Future good days are no longer a routine prerequisite for action-uplift research. A new action family first receives a frozen chronological split from the current retained universe:

| Panel | Days | Use |
| --- | ---: | --- |
| Development | 80 | Past-only nuisance/action fitting and hypothesis development |
| Embargo 1 | 1 | Excluded |
| Validation | 20 | One fixed development-to-validation evaluation |
| Embargo 2 | 1 | Excluded |
| Sealed holdout | 20 | One-shot confirmation only after development and validation pass |

The holdout is family-specific sealed evidence. Its dates may have appeared in unrelated research, so it is not described as globally untouched.

New dates are needed only after the family changes following holdout access, the family-specific holdout is exhausted, no overlap-supporting split remains, or a material production distribution shift requires confirmation. A negative or uncertain estimate is not a reason to wait and rerun the same family.

## Frozen identity

| Component | Identity |
| --- | --- |
| Family | `side_specific_local_actions_causal_v4_20260718` |
| Split schema | `narrowgate_evidence_split.v1` |
| Split manifest SHA256 | `2ffa6e799d105f9c2e8c0835d25288cd81bacd40c7e5a314f236b05744f5d646` |
| Action-family SHA256 | `767586140c55dffa70bd659a2bdadd71edda7feea740e85d93c6979b5b817f00` |
| Feature manifest SHA256 | `0d5117283378c5f46127a60652eafe8c718365c6357d4e89ab9e4e6c8f7280ac` |
| Replay workspace SHA256 | `f4ced1ece4a18f343070ea9454bee06edd185de9c70d0b8b9353bdb238f919a6` |
| Config SHA256 | `0c13d1533fe28c4f3bbaafdd85f62ed0b6074314e177b000356ab4b529a4aa9a` |
| P3 artifact SHA256 | `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652` |
| Queue artifact SHA256 | `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd` |
| REST latency sample SHA256 | `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2` |

The development and validation replays used the same frozen workspace and configuration identities. Replaying after documentation-only edits reproduced the action panels byte for byte.

## Action family

Each campaign receives at most one exposure-increasing add-quote intervention:

| Action | Behavior propensity |
| --- | ---: |
| `baseline` | 0.40 |
| `prevent_over_widen` | 0.20 |
| `widen_1tick` | 0.20 |
| `recenter_1tick` | 0.20 |

The replay keeps order size, reducing-side behavior, inventory limit, hard safety gates, empirical REST latency and queue mechanics unchanged. It records the complete propensity vector and runs the assigned action through the full order, fill and subsequent inventory path.

The campaign reward is attributed once:

```text
reward = fill_value - incremental_campaign_cost - queue_reset_cost
```

The first family is add-only. Opener and reducing roles have no action overlap and cannot inherit these results.

## Panel sanity

Development produced 5,746 interventions over 80 days; validation produced 1,508 interventions over 20 days. BUY and SELL, and all four actions, had support in both panels.

The reward identity error was numerically zero. The balanced behavior mixture did not collapse participation:

| Panel | Control/random fills | Control/random campaigns | Control/random inventory time | Behavior-mixture raw delta |
| --- | ---: | ---: | ---: | ---: |
| Development | 47,901 / 47,890 | 16,146 / 16,139 | 8,930.73 / 8,926.46 | -0.23 USDC |
| Validation | 12,277 / 12,285 | 3,990 / 3,984 | 2,502.70 / 2,507.20 | -0.76 USDC |

These rows are mechanism checks, not policy-value estimates.

## Development OPE

Development uses chronological past-only cross-fitting with 50 training days, one embargo day and 10-day forward test blocks. Values below are doubly robust reward uplift in USDC per intervention.

| Side | Action | Rows | DR uplift | Day-clustered 95% interval | ESS | Positive-day rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| BUY | `prevent_over_widen` | 695 | -0.00877 | [-0.03870, +0.01952] | 217 | 55.2% |
| BUY | `widen_1tick` | 679 | +0.01783 | [-0.01233, +0.04682] | 236 | 48.3% |
| BUY | `recenter_1tick` | 660 | -0.00583 | [-0.04358, +0.02487] | 221 | 51.7% |
| SELL | `prevent_over_widen` | 461 | -0.01134 | [-0.04676, +0.01790] | 167 | 48.3% |
| SELL | `widen_1tick` | 464 | -0.02168 | [-0.05433, +0.01049] | 153 | 48.3% |
| SELL | `recenter_1tick` | 465 | +0.01318 | [-0.01481, +0.04277] | 157 | 58.6% |

Every confidence interval crosses zero. Before validation was read, the largest positive point estimates were frozen as diagnostic candidates: BUY `widen_1tick` and SELL `recenter_1tick`. Neither was promotion-eligible.

The frozen decision record predates the final 2,000-trial documentation rerun. The underlying development action panel is byte-identical and all point estimates are unchanged.

## Fixed development-to-validation OPE

All nuisance and action-value models were fitted on development only. Validation was not folded back into training.

| Side | Action | Rows | DR uplift | Day-clustered 95% interval | ESS | Positive-day rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| BUY | `prevent_over_widen` | 176 | -0.01564 | [-0.05166, +0.01761] | 176 | 50% |
| BUY | `widen_1tick` | 161 | +0.00394 | [-0.04063, +0.05211] | 161 | 45% |
| BUY | `recenter_1tick` | 204 | -0.00953 | [-0.05614, +0.04120] | 204 | 40% |
| SELL | `prevent_over_widen` | 126 | +0.05260 | [-0.00275, +0.10886] | 126 | 70% |
| SELL | `widen_1tick` | 101 | -0.02280 | [-0.09215, +0.02332] | 101 | 55% |
| SELL | `recenter_1tick` | 127 | -0.00905 | [-0.07135, +0.05033] | 127 | 55% |

BUY widening retained only a small positive point estimate and its daily sign rate fell below 50%. SELL recentering reversed sign. SELL `prevent_over_widen` became the validation winner after being negative in development. This winner rotation is instability, not a reason to select the validation maximum.

## Tail support

The predeclared extreme-tail outcome was terminal campaign MTM at or below `-5 USDC`, with at least five logged candidate events required.

No candidate action had a qualifying event in development or validation. Validation contained one such event on the SELL baseline action only. The tail gate therefore fails for missing support. Zero candidate events must not be interpreted as evidence that an action eliminates tail risk.

## Final read

- No action passes side-specific reward, terminal, tail and stability gates.
- The development leaders do not survive validation.
- The sealed 20-day holdout is not read because validation cannot rescue a failed development family.
- The action family is closed as a completed negative result.
- Live code, live configuration and the rolling baseline are unchanged.

The next family should be a predeclared state-conditioned action, such as a local shock/refill/recovery eligibility rule, rather than another fixed global tick or second threshold. It must receive a new family identity and a new frozen evidence allocation before outcomes are read.

## Artifacts

Canonical private artifacts live under:

`${NARROWGATE_RESULTS_DIR}/action_uplift_existing_split_20260718/`

Key files:

- `side_specific_local_actions_causal_v4.evidence_split.json`
- `side_specific_local_actions_causal_v4.development_decision.json`
- `identity_locked_development.action_panel.csv`
- `identity_locked_validation.action_panel.csv`
- `identity_locked_development_ope.rollup.csv`
- `identity_locked_validation_fixed_ope.rollup.csv`

The repository entrypoints are:

```bash
python -m models.audit.evidence_split --help
python -m models.audit.local_action_uplift --help
python -m models.audit.local_action_ope_report --help
```

## Follow-up family

The subsequent side-specific family `sell_add_repair_trend_skip_causal_v4_v1` used a new frozen split identity, known 50/50 propensity, and a competing-risk target. It skipped exactly one otherwise eligible exposure-increasing SELL add cycle. It also failed its Development gate; Validation and sealed holdout remained unread. See `docs/sell_add_repair_trend_skip_causal_v4_v1_20260718.md`.
