# Causal Multichannel Boolean Cooldown V2 Owner Modeled-Queue Development Closure

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

> **Interpretation superseded on 2026-08-12.** The numerical OOF results below remain historical evidence, but the phrases "exact no-policy closure" and "structurally ineligible" are too broad. Read the current [interpretation errata](causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1_interpretation_errata_20260812.md), which separates the blocked strict-native path from the limited modeled-queue one-shot no-pass result and records that the frozen feature hierarchy was not implemented.

Date: 2026-08-12

Identity: `causal_multichannel_window_boolean_cooldown_duration_v2_owner_modeled_queue_v1`

Status:

```text
strict historical label path: failed closed on non-identifiable event ordering
owner modeled-queue exploration: nested OOF complete
supported side policies: none
repeated-policy / 40-10-50 / restart / transport: structurally ineligible
action authority: false
live authority: false
Validation read: false
sealed holdout read: false
```

## 1. Scope

This document closes the exact v2 Development branch requested by the multichannel Boolean cooldown handoff. It preserves the v1 negative evidence, adds action-magnitude and campaign context, materializes separate BBO, trade, and depth EMA/cross channels, and evaluates bounded AND/OR/NOT duration rules with nested chronological OOF.

It does not claim that 85 seconds is optimal, that all EMA state is useless, or that every future cooldown policy is closed.

## 2. Strict-Native Historical Path

The raw snapshot/delta source union passed its frozen sequence and timestamp admission for 41 target days. That source admission was necessary but not sufficient to identify strict economic labels.

Historical public trade rows expose millisecond timestamps while the raw book stream contains sub-millisecond ordering. For trades and book events sharing a millisecond, the historical inputs do not identify which event was visible first. Resetting counters, inventing a tie-break order, or treating a modeled order as exchange authority would change queue seeds and fill paths. The formal strict runner therefore failed closed before producing a reusable economic panel.

Authoritative receipts:

- [`strict-native failure report`](causal_multichannel_window_boolean_cooldown_duration_v2_strict_native_formal_execution_failure_20260811.md)
- [`strict-native failure receipt`](causal_multichannel_window_boolean_cooldown_duration_v2_strict_native_formal_execution_failure_receipt_20260811.json)
- execution amendment v9 SHA256: `04c7833677bea6d14d125a91ba5196b3e7cb989c22d1e88dd80951dddd87b34c`

Consequences:

```text
formal strict labels generated: false
strict queue policy eligible: false
strict economic outcomes read: false
raw admitted source bytes reusable: true
```

## 3. Separate Owner Modeled-Queue Successor

The owner route was frozen as a distinct identity rather than weakening the strict contract. Its queue authority is explicitly:

```text
modelled_queue_without_exchange_queue_authority
```

The successor reuses the admitted 40-day modeled one-shot panel. It cannot grant strict queue, research-supported, action, or live authority. The only possible advancement route was an owner-risk-accepted repeated-policy successor after positive outer-OOF evidence.

Frozen bindings:

- owner Spec SHA256: `362cb1848da44e8b6f4e274ab4e99f7077e9cea7efafcbd77cc404e53774c666`
- study config SHA256: `636074fdbf52b363bcde953926db0a529e5f9ac349324cddd4473f70f56e6659`
- OOF execution amendment SHA256: `e8e33aab820fb35c7d12e2b488cdadf50730a5ac86ada7e17ec4a6c339e40ff3`
- OOF execution identity SHA256: `0a16147b39d507d20c468bd26091c6f6d747809587aa18cf9a46a7d956a0efdb`

2025 provider data supplied outcome-blind predicate thresholds and support only. All economic labels came from the frozen 2026 modeled-queue Development panel.

## 4. Feature And Label Census

The admitted feature panel contains 40 days and 8,600 opportunities. The modeled action table contains 120,400 opportunity-action slots. Of the 8,600 opportunities, 8,429 are point eligible and 171 retain explicit censoring or unsupported status. No unsupported outcome was imputed. A further 197 finite values attached to unsupported rows were redacted before fitting.

Feature blocks:

- `R0`: historical BBO-mid Boolean reproduction.
- `M0`: action magnitude, side/role, inventory, campaign, fill, and cooldown ownership context.
- `M1`: M0 plus BBO/price EMA and cross state.
- `M2`: M1 plus separately normalized individual-trade and depth channels.

M0/M1 have the full 40-day modeled-label denominator. M2 has a frozen 33-day common-support denominator; seven days lacking its complete source support are not silently filled or pooled.

## 5. Nested Chronological OOF

Every outer fold received a support-valid non-control candidate. Candidates were not replaced by `CONTROL_85N` before outer testing. Deployment gates were applied only after OOF scoring.

The table reports the identified OOF mean and lower confidence bound in USDC per campaign-weighted opportunity.

| Panel | Side | Block | Mean | LCB | Gate |
|---|---|---:|---:|---:|---|
| Prefix 40 | BUY | R0 | -0.001561 | -0.003650 | fail |
| Prefix 40 | BUY | M0 | -0.001617 | -0.003585 | fail |
| Prefix 40 | BUY | M1 | -0.001006 | -0.002838 | fail |
| Prefix 40 | SELL | R0 | +0.000194 | -0.000773 | fail |
| Prefix 40 | SELL | M0 | -0.004421 | -0.013788 | fail |
| Prefix 40 | SELL | M1 | +0.000052 | -0.001416 | fail |
| Prefix 33 | BUY | R0 | +0.000029 | -0.001744 | fail |
| Prefix 33 | BUY | M0 | -0.002579 | -0.005818 | fail |
| Prefix 33 | BUY | M1 | -0.000579 | -0.002220 | fail |
| Prefix 33 | BUY | M2 | -0.000098 | -0.001679 | fail |
| Prefix 33 | SELL | R0 | -0.000297 | -0.003963 | fail |
| Prefix 33 | SELL | M0 | -0.001108 | -0.003382 | fail |
| Prefix 33 | SELL | M1 | -0.000401 | -0.001774 | fail |
| Prefix 33 | SELL | M2 | -0.000222 | -0.003619 | fail |

All 14 Boolean deployment gates failed. The failures are not mechanical no-ops: for example, BUY M2 changed duration for 1,492 opportunities across 1,010 campaigns and 21 OOF days, an action rate of 60.21%. The failure reasons are economic and identification based:

```text
identified_oof_lcb_not_above_economic_epsilon
partial_identification_lower_bound_unavailable
```

The required continuous-state comparator also completed. It did not nominate either side for promotion, and its contract never allowed it to replace the Boolean policy or grant authority.

Authoritative OOF artifacts:

- artifact root: `${NARROWGATE_DATA_ROOT}/reports/causal_multichannel_window_boolean_cooldown_duration_v2_20260810/owner_modeled_queue_nested_oof_v1`
- manifest SHA256: `55413332e1d72ea7c5980653a268912a0880ed441f9d95a3dacd3712230d4197`
- report SHA256: `ac477729691f3ca0a759e6062968a35aaac9f7584710152706570742735e65d9`
- selected-candidates SHA256: `52f97816ad3d636a568aeb083d97af382d90264656fc0310d07e00db73b06337`
- binding SHA256: `b0c16729ed40ea068a34f146d83bfade9300513bc7d06c74f153da664b1ce5fa`

The added 10-day and pooled 50-day economic panels were not imputed or read. Their status is `not_run_not_imputed`, because no frozen 2026 modeled labels exist for those dates and they cannot create support after the prefix gate.

## 6. Exact Post-OOF Closure

No BUY or SELL policy passed the preregistered OOF gate:

```text
supported_sides = []
BUY policy = CONTROL_85N
SELL policy = CONTROL_85N
```

The post-OOF finalizer therefore emitted an exact no-policy closure without reading any additional economic table. Under the frozen stage dependencies, the following are structurally ineligible rather than merely unfinished:

- unified learned policy freeze;
- repeated-policy Python/C++ ABI and parity;
- BUY-only, SELL-only, and joint 40/10/50-day full-path A/B;
- restart-aware continuous confirmation;
- live/AWS transport and canary.

Running those stages with a hand-picked failed rule would create a new outcome-informed identity and would not complete this preregistered branch.

Closure artifacts:

- artifact root: `${NARROWGATE_DATA_ROOT}/reports/causal_multichannel_window_boolean_cooldown_duration_v2_20260810/owner_modeled_queue_post_oof_v1`
- manifest SHA256: `fbb82e470e59980d47dcf340d29bfb3176133d0d20fc7b969d0d284694342661`
- closure file SHA256: `db8120bbf76321054b67515cf6237ca8642d3cf856ea80fb35aa50341fafdfd2`
- closure canonical SHA256: `ec341dc4ec19143ec7d8c2e8ca62d861bcd1b710389fffff5760c7be62b906d7`

## 7. Final Permissions And Interpretation

```text
research-supported promotion: false
owner repeated-policy successor: false
action authorized: false
live authorized: false
Validation read: false
sealed holdout read: false
```

The exact closed scope is:

```text
owner modeled-queue one-shot labels
+ frozen R0/M0/M1/M2 feature families
+ frozen Boolean candidate search
+ frozen duration vocabulary
+ nested chronological OOF
```

The result does not close a future strict raw-native successor with identified receive-time ordering, a genuinely different multichannel architecture, or an independently preregistered action identity. Such work must start from a new identity and cannot tune this consumed Development result.
