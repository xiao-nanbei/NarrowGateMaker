# Causal v12 Owner-Amended Economic Rescore v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: expected value and fill selectivity supported on 27 previously read native days; campaign q10 non-inferiority remains unresolved. This retrospective rescore creates no independent confirmation or research-derived live authority.

## Amendment Boundary

The original v1 Specs and decisions remain unchanged. After reviewing their results, the project owner replaced the one-sided 90% fill-retention floor with a symmetric activity band:

\[
0.80 \le \frac{F_{\rm ML-ON}}{F_{\rm ML-OFF}} \le 1.20.
\]

This is an outcome-informed owner amendment. It is valid as the prospective v2 contract and as a transparent retrospective sensitivity, but it cannot be described as the gate that was frozen before the v1 outcomes.

## Economic Selectivity

When control PnL is negative and ML-ON has fewer fills, the new diagnostic is:

\[
E_{\rm loss/fill}
=
\frac{
  (\mathrm{PnL}_{on}-\mathrm{PnL}_{off})/|\mathrm{PnL}_{off}|
}{
  (F_{off}-F_{on})/F_{off}
}.
\]

`E_loss/fill > 1` means the relative loss reduction exceeds the relative fill reduction. It is a proportional-thinning benchmark, not a causal decomposition of which removed fill generated the gain.

| Panel | Positive PnL days | PnL delta | Fill retention | Loss reduction | Fill reduction | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| Historical late 22 | 16/22 (72.73%) | +26.7066 USDC | 85.87% | 40.44% | 14.13% | 2.862 |
| Post-fit Grade-A 5 | 3/5 (60.00%) | +6.0334 USDC | 84.20% | 51.98% | 15.80% | 3.289 |
| Combined 27 | 19/27 (70.37%) | +32.7400 USDC | 85.58% | 42.17% | 14.42% | 2.925 |

For the combined 27 days:

- mean PnL delta: `+1.2126 USDC/day`;
- 95% UTC-day clustered interval: `[+0.5755, +1.8437] USDC/day`;
- loss/fill ratio interval: `[1.5742, 4.3841]`;
- proportional-thinning selectivity surplus: `+21.5463 USDC`, interval `[+6.0195, +36.5504]`.

The five-day interval `[-0.2435, +2.5635]` is asymmetric, but endpoint lengths are not probability mass. The corresponding empirical day-bootstrap fraction above zero is `94.53%`. The combined 27-day result is stronger because its lower bound is positive without relying on that interpretation.

## Administrative Day End

UTC midnight remains the dependence-clustering unit, but it is not a live strategy reset boundary. The v2 hierarchy therefore uses closed-campaign value as primary and keeps day-end open inventory/MTM as an accounting diagnostic.

| Component | ML-ON minus ML-OFF | Mean daily delta | 95% clustered interval |
|---|---:|---:|---:|
| Closed campaigns | +29.8355 USDC | +1.1050 | [+0.4801, +1.7306] |
| Day-end open MTM | +2.9045 USDC | +0.1076 | [+0.0013, +0.2580] |
| Total terminal MTM | +32.7400 USDC | +1.2126 | [+0.5755, +1.8437] |

Closed campaigns explain `91.13%` of the total PnL delta. The result is not primarily produced by favorable marking of unfinished day-end inventory.

Future confirmation must carry inventory, cash, cooldown lineage, campaign state, and model state across UTC midnight. Day clustering can remain, while strategy-state resets cannot.

## Gate Result

Seven of eight owner-amended hard gates pass:

- closed-campaign PnL lower bound positive: pass;
- fills within 80%-120%: pass;
- loss/fill selectivity lower bound above one: pass;
- inventory time ratio `0.9323`: pass;
- campaign CVaR mean non-worsening: pass;
- BUY and SELL maker-value tolerance: pass;
- campaign q10 point non-worsening: unresolved/fail under the retained v1 rule.

Campaign q10 changed by `-0.01269 USDC/day`, with interval `[-0.03880, +0.01419]` and 12/27 positive days. This does not prove material tail harm, but it also does not satisfy the retained zero-tolerance point gate. No q10 tolerance is introduced after seeing this result.

Canonical decision:

`owner_amended_mean_and_selectivity_supported_campaign_q10_unresolved`

## Identity And Authority

- Spec SHA256: `eb3c5fedd3a638cdb93c14e9ebd3a8915448a8495a30bfdc5f7cd0a35f4ea707`
- Report SHA256: `dc491ae5642d7743d4563156fdd5b9fc6772eebc4684977e19f16d17d1246b40`
- Report path: `${NARROWGATE_DATA_ROOT}/reports/causal_v12_owner_amended_economic_rescore_v2_20260802/report.json`

The separate owner-authorized live canary remains an operational decision. This rescore strengthens its expected-value rationale, but does not convert consumed historical panels into independent confirmation.
