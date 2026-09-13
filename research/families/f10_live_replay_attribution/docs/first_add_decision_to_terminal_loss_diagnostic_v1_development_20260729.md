# First-Add Decision-To-Terminal Loss Diagnostic v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

`development_evidence_built_no_action_authority`

This Development-only diagnostic is complete. Validation and sealed holdout were not read. It does not authorize ranking, action registration, or live deployment.

## Native denominator

- 40/40 frozen Development days completed: 24 Grade A primary and 16 Grade B sensitivity days.
- 2,071 first actual exposure-increasing add decisions were joined exactly to their order, fill, and campaign terminal.
- Feature-clock violations, open records, native sequence failures, and q90 parity failures were all zero.
- Queue ahead was observable for 2,070 rows. One Grade-B BUY quote 896.5 ticks from mid had explicit `unknown` queue identity and was never imputed as zero.

The primary target is:

\[
Y=V(T_{campaign})-V(t_{first\ add\ decision})
\]

in USDC per first-add decision.

## Outcome

| Panel | Side | Rows | Mean USDC | Day-clustered 95% interval |
|---|---:|---:|---:|---:|
| Grade A primary | BUY | 596 | -0.04272 | [-0.05342, -0.03175] |
| Grade A primary | SELL | 658 | -0.05564 | [-0.07047, -0.04066] |
| Grade B sensitivity | BUY | 459 | -0.04664 | [-0.06819, -0.02621] |
| Grade B sensitivity | SELL | 358 | -0.03181 | [-0.05136, -0.01178] |

First-add decisions therefore form a genuine loss denominator on both sides. That fact alone does not identify an avoidable action.

Grade-A BUY contrasts associated higher queue with worse value and higher refill/cancel ratios with less-negative value. These contrasts did not replicate in Grade B. Other frozen decision-visible mechanism contrasts were either unsupported or crossed zero. The evidence was therefore passed to F05 as a prediction problem, not converted into an F09 action.

## Authority

The authoritative machine reports are outside the repository under:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/first_add_decision_to_terminal_loss_diagnostic_v1_development_20260729`

- `report.json` SHA256: `0ce683433171f8ab3ad7eaef306186b60e8be1d00f315cb05e72d53a824aa1b3`
- `manifest.json` SHA256: `b9043aeb3f6fe7c813d876c974ae8231620d8f889825da2255dcfbd9f7db8305`
