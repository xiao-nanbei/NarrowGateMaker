# Live 120h Adverse-Selection Diagnostic v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: completed live observational diagnostic. This identity grants no prediction, action-experiment, Validation, holdout, shadow, or live authority.

## Identity

The authoritative fill-markout window is `2026-07-25 02:11:34.730 UTC` through `2026-07-30 02:11:34.730 UTC`. The frozen [Spec](live_120h_adverse_selection_diagnostic_v1_spec_20260730.json) has canonical hash `c6866274b950f7e09f1ab4494935b6d46cf02576c4377fd3e7bb854131f1a4f6`.

The persisted evidence directory is:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/live_120h_adverse_selection_diagnostic_v1_20260730`

`live_120h_exact_mid_fills.csv` is authoritative for fill markout. `live_120h_campaigns_clean.csv` is authoritative for the clean closed-campaign summary. The separately generated operational-context JSON starts about 58 seconds later and is sensitivity context only; its older markout calculation must not replace the exact-mid rows.

The original ad-hoc generator source identity is unavailable. Claim reproduction is therefore limited to the hash-frozen CSVs. The repository [recomputation module](../audit/live_120h_adverse_selection_diagnostic.py) checks all bound artifact hashes and arithmetic claims. Its generated report has SHA256 `71e375c6876cafa7cd968aafec325c034ebf7ef2ae8fa4a219c5bac543008265`.

## Fill Decomposition

The approximate 10-second maker value is:

\[
v_{10s}
=
\text{entry edge}
+
\text{maker-signed subsequent market move}.
\]

The maximum row-level accounting discrepancy is below `6e-15 bps`.

| Side | Total fills | Usable | Entry edge | Subsequent move | Net value | Win rate |
|---|---:|---:|---:|---:|---:|---:|
| All | 2,622 | 2,417 | +2.657 bps | -3.108 bps | -0.451 bps | 41.5% |
| BUY | 1,310 | 1,188 | +2.565 bps | -2.972 bps | -0.407 bps | 42.9% |
| SELL | 1,312 | 1,229 | +2.745 bps | -3.239 bps | -0.494 bps | 40.1% |

All six UTC date slices have a negative mean value. This supports a broad adverse-selection diagnosis for this window: maker spread capture was present, but the subsequent maker-adverse move was larger on average.

The horizon is not an exact exchange-time 10-second endpoint. It uses the first quote decision observed at or after `fill_ts + 10s`; the median additional delay is 2.072 seconds and p90 is 4.579 seconds. The result must therefore be called an approximate 10-second markout.

## Side And Role

| Group | Usable fills | Net value | Win rate |
|---|---:|---:|---:|
| BUY opener | 213 | -0.076 bps | 49.8% |
| SELL opener | 727 | -0.486 bps | 40.3% |
| SELL add | 286 | -0.494 bps | 39.5% |

BUY opener is near neutral in this window, while SELL opener and SELL add are weaker. This is observational state localization, not evidence that all SELL openers or adds should be disabled.

The operational-context artifact records 2,501 BUY q90 cancel requests and a 4.02% actionable BUY fill-selection hit rate. Those figures explain the current protection coverage but do not identify the counterfactual value of a SELL-side intervention.

## Order Age

| Active age at fill | Usable fills | Net value | Win rate |
|---|---:|---:|---:|
| Less than 1 second | 184 | -0.098 bps | 49.5% |
| 4.5 to 5.5 seconds | 333 | -0.658 bps | 37.8% |

Older orders are associated with weaker outcomes. This does not establish that shorter requote, cancel, or replace improves value: order age is selected by the market path and current policy. The observation does not reopen the closed F07 keep/cancel action families.

## Campaign Concentration

The 983 clean closed campaigns have:

- win rate: 52.3%;
- average winner: +0.03654 USDC;
- average loser: -0.05172 USDC;
- aggregate price/cashflow PnL: -5.3690 USDC.

There are 158 campaigns with maximum absolute inventory above one `0.001 BTC` order unit. They are 16.1% of campaigns and contribute -4.0127 USDC, or 74.7% of the **net negative aggregate** campaign PnL. This is not a gross-loss attribution. The 756 SHORT campaigns contribute -5.0636 USDC of aggregate campaign PnL.

This supports two distinct research questions:

1. whether SELL opener/first-fill value is predictable before the loss path;
2. whether post-fill cumulative SHORT inventory can be limited without collapsing normal participation or repair.

The two questions must not be combined into one action. An incremental inventory budget cannot repair weak SELL opener selection, and a first-fill model does not establish the value of blocking later adds.

## Operational Context

The frozen exact-fill rows contain 37 nonzero commission entries totaling `0.9505` reported commission units. The commission asset is absent, so this identity does not relabel the total as USDC. Fees do not enter the price markout identity and therefore cannot explain the negative maker-signed price move, although they can worsen net account PnL.

The operational-context artifact records two `SYNC_ADJUST` rows. The separate claim that the post-restart subset has 2,364 usable fills is not bound to a source log/hash in this identity and remains contextual only.

## Decision

Supported:

- adverse selection exceeded spread capture on average in the frozen window;
- SELL opener and SELL add are the weaker exposure-increasing groups;
- longer-lived fills are associated with lower value;
- SHORT and multi-inventory campaigns concentrate net negative campaign PnL.

Not supported:

- changing requote or cooldown time;
- disabling all SELL exposure;
- deploying a SELL selector;
- selecting an incremental inventory budget;
- reading Validation/holdout or changing live configuration.

The next work is evidence-only. F05 may preregister a distinct SELL first-fill conditional-value feasibility identity. F09 may continue the already concept-only cumulative post-cooldown inventory-budget mechanics design, with SELL/SHORT support reported separately. Neither branch inherits action authority from this live diagnostic.
