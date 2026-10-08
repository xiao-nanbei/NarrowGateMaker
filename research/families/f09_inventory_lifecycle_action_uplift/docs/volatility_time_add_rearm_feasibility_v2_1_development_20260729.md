# Volatility-Time Add Rearm Feasibility v2.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Decision

The Development-only `volatility_time_add_rearm_feasibility_v2_1` identity supports the normalized-BBO variance clock as a rearm mechanics component. It does not support a randomized action experiment.

- `variance_clock_mechanics_passed=true`
- `current_live_blocker_parity_passed=false`
- `feasibility_passed=false`
- `action_experiment_created=false`
- `action_or_live_authorization=false`
- `reward_or_pnl_read=false`
- `validation_read=false`
- `sealed_holdout_read=false`

The correct decision is `clock_mechanics_supported_hold_action_for_blocker_parity`.

## Identity Lineage

The frozen v1 identity used completed individual-trade one-second closes. It failed clock availability and remains closed; its gates and interpretation were not changed after observing the result.

The first BBO-clock v2 run exposed an implementation defect in a diagnostic: the final one-second sample interval was counted in full after an intra-interval candidate release, producing aggregate valid-time rates above one. The rearm timestamp and Python/C++ state-machine result still agreed, but the output has no evidence authority. It is preserved with an explicit invalidation record:

- invalidation SHA256: `b1243ab7ab45a886fcbfad1cdce664913546a6c64edc406d7f0ccfb3870de06e`
- preserved implementation SHA256: `aa58e3dc75ce89f817947cd5068dffc51a8c9657e7d4f6f3109caf925eb891d2`

v2.1 creates a new frozen identity and clips clock-coverage accounting exactly at the candidate release. It does not change the source, rearm state machine, candidate, split, or gates.

## Frozen Contrast

The corrected live baseline is:

\[
T_{\mathrm{baseline}}
=85\,\mathrm{s}\times
\max(1,n_{\mathrm{same\mbox{-}side\ fill\ units}}).
\]

The candidate freezes a side-specific episode budget:

\[
B_{s,e}=\nu_{\mathrm{ref},s}\times85\,\mathrm{s}\times
\max(1,n_{\mathrm{same\mbox{-}side\ fill\ units}}),
\qquad
QV_e(u)=\int_0^u \nu_t\,dt.
\]

It changes only elapsed-time measurement for exposure-increasing add rearm. Reducing quotes, order size, inventory limit, P3, queue, and latency remain unchanged. Same-side add and reducing fills both increment fill units; an opposite-side fill is the only fill-related reset.

The causal clock uses completed one-second buckets from BTCUSDC executable BBO mid in `normalized_l2_100ms_v2`:

\[
\nu_t=10^8\frac{\sigma^2_{p,t}}{m_t^2}
\quad [\mathrm{bps}^2/\mathrm{s}].
\]

For bucket `[t,t+1s)`, only the final valid BBO whose timestamp is strictly less than `t+1s` is used. The completed bucket becomes visible at bucket end plus the frozen ready-delay scenario. Missing or stale variance freezes the clock; there is no future backfill or default variance.

Reference rates fitted on the first 20 Development days are:

- BUY: `0.3664154173 bps^2/s`
- SELL: `0.6020856312 bps^2/s`

The remaining 20 Development days contain 3,376 BUY and 2,107 SELL episodes. The evaluator loaded 22,854 lifecycle rows without loading outcome columns.

## Development Result

| Delay | Side | Start valid | Earlier >5s | Later >5s | Effective | Max cap | Valid clock | Median delta | Python/C++ |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0ms | BUY | 99.76% | 47.90% | 21.92% | 69.82% | 1.18% | 99.85% | -1.18s | 0 |
| 0ms | SELL | 99.57% | 40.67% | 21.07% | 61.75% | 2.52% | 99.80% | 0.00s | 0 |
| 250ms | BUY | 99.76% | 47.90% | 21.86% | 69.76% | 1.18% | 99.87% | -1.33s | 0 |
| 250ms | SELL | 99.57% | 40.67% | 21.07% | 61.75% | 2.52% | 99.80% | 0.00s | 0 |
| 1000ms | BUY | 99.76% | 47.99% | 21.86% | 69.85% | 1.18% | 99.87% | -1.40s | 0 |
| 1000ms | SELL | 99.57% | 40.53% | 21.07% | 61.60% | 2.52% | 99.79% | 0.00s | 0 |

All six side-by-delay cells pass frozen support, clock-quality, two-sided variation, liveness, and Python/C++ parity gates. The result is robust to the 0ms, 250ms, and 1000ms feature-ready delay scenarios. The candidate is neither a near-no-op nor a stop-add-until-cap policy.

## Why No Action Was Created

Clock mechanics is only one component of current live behavior. The frozen blocker contract still records:

- BUY q90 cancel/re-enter lifecycle replay: unsupported;
- consecutive-loss global cooldown replay: unsupported;
- sync-degrade event semantics: not frozen;
- adverse, defense, and stale-book guards: supported.

The historical BBO source also carries exchange timestamps rather than the AWS Tokyo receive-time identity required for current-live action authority. These gaps can mask or alter an apparent cooldown release. Therefore clock support cannot be converted into reward evaluation, randomized replay, Validation access, or live deployment.

The next permissible work is blocker-lifecycle parity under a new frozen identity. Only after that passes may a new action experiment be preregistered; v2.1 itself must remain evidence-only.

## Frozen Evidence

- v2.1 Spec SHA256: `9444d521e6a771c838b78ec43cfe28e8e375130307c3717fb82b1d1f6e3d00e5`
- implementation SHA256: `518b9f7d78f59e93e977d0e692d0abb94ba43f192103167e89cc075394ac9952`
- source fill trace SHA256: `c64df393ffd2ab1624ba067563a22295702471e6dec17ee7ae54d1927803832b`
- report payload SHA256: `71bac7376f3ec1c641355fd3d664ddfad971f7a7fc2c243b396b46789dbfb20f`
- report JSON SHA256: `aac2b686dc6841ceace468c38da3d61a072a098efa5cb8f162af332a281a7148`
- manifest SHA256: `80b2fac641829df3a75426f95d2b9091bf0af05140e8550615d14fc2ae40af4c`
