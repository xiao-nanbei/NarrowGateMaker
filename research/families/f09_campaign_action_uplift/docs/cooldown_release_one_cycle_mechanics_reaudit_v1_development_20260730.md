# Cooldown Release One-Cycle Mechanics Reaudit v1 - Development

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

The current-stack re-audit reconfirms that skipping exactly one eligible exposure-increasing add cycle is a low-leverage fill-path intervention. The historical one-cycle action remains closed.

This result does not register a new action, read Validation or sealed holdout, or authorize live deployment. F09 remains active for mechanism research with no registered action.

## Why This Is Not a New Action Family

The intervention overlaps three completed F09 identities:

- `first_add_marginal_order_value_v1`: first eligible add, baseline versus one-cycle skip;
- `sell_add_repair_trend_skip_causal_v4_v1`: SELL one-cycle skip, closed as near no-op;
- `recovery_event_rearm_v1`: first eligible add after the 85-second cooldown, reducing quote unchanged.

Changing the denominator, blocker fidelity, or replay implementation does not change the economic intervention. This identity is therefore diagnostic-only.

## Frozen Scope

- Panel: the exact 40 Development days used by the current F09/F10 evidence.
- Clock: corrected wall-time `85s * consecutive same-side fill units` baseline.
- Stack: native snapshot/delta queue, BUY q90 cancel/ACK/recovery/re-entry, path-dependent consecutive-loss cooldown, deterministic sync-degrade tape, frozen P3, queue, and AWS Tokyo latency identities.
- Observation: two side-specific no-op observer replays per day.
- Invariance: BUY-observer and SELL-observer decision/order lifecycle digests must match exactly; all 40 days passed.
- Outcome access: only within-day post-assignment variance was read to compute a balanced two-arm design MDE. No outcome mean, sign, treatment contrast, policy score, or threshold search was read.

## Results

| Side | Release episodes | Baseline eligible | Final action-change opportunities | Selected-cycle fills | Fill rate, 95% Wilson | Design MDE, USDC |
|---|---:|---:|---:|---:|---:|---:|
| BUY | 2,420 | 2,415 (99.79%) | 2,403 (99.30%) | 172 / 2,403 | 7.16% [6.19%, 8.26%] | 0.014365 |
| SELL | 2,367 | 2,357 (99.58%) | 2,348 (99.20%) | 147 / 2,348 | 6.26% [5.35%, 7.31%] | 0.015456 |

The pre-frozen near-noop classification requires the selected-cycle fill-rate 95% upper bound to remain below 10%. Both sides pass that diagnostic rule.

The intervention is not a quote-action no-op: roughly 99% of release episodes would suppress a submitted add cycle. It is a fill-path near no-op because only about 6%-7% of those selected cycles actually fill. The current-stack SELL rate is higher than the historical 3.52% result, but remains below the frozen near-noop bound and does not reopen the action.

At the first release decision, temporal masking was 5 / 2,420 for BUY (0.21%) and 13 / 2,367 for SELL (0.55%). The remaining final-action losses were due to same-timestamp sync-adjust/markout blockers. Other cooldown and pause mechanisms therefore do not explain away the low selected-cycle fill leverage.

The MDE is a design diagnostic, not an effect estimate. Under the observed within-day baseline outcome variance, a balanced randomized study would need an effect on the order of 0.014-0.015 USDC per release to reach 80% power at a two-sided 5% level. This run did not estimate whether such an effect is positive or negative.

## Governance

- `historical_action_remains_closed=true`
- `randomized_action_registration_allowed=false`
- `action_experiment_authorized=false`
- `validation_read=false`
- `sealed_holdout_read=false`
- `live_deployment_authorized=false`

A future F09 action must change the economic intervention itself. Re-running a one-cycle skip with more dates, a different denominator, or improved blocker parity is not sufficient.

## Identity

- Frozen spec file SHA256: `c395d4e7ec9224e1eb4022ed55a90c9f33776cb285b566ccb7e5e6b1f0dbdf87`
- Canonical spec SHA256: `ea3829f25d8474515ea9c215d605557f817714229b574429cc4b5ae8e13dbcb9`
- Evaluator SHA256: `b6831a25048ddbf8cb9e37ce97f77add5d5ffc9b9084d2049d6672e335b29811`
- Test SHA256: `29c1d041b9109d29ad4f4307af1569d248af9bd9a52ad84f4e65845a24bda75f`
- Machine report SHA256: `4b347f0298a96a83c228cb0d99f8285030e30a0482bb499954bc76451033b68b`
- Machine manifest SHA256: `d2a771dcae4d219fe52b944586aa80f0194305735900f4757faa45a7270b74fe`
- Machine output: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/cooldown_release_one_cycle_mechanics_reaudit_v1_20260730/development`
