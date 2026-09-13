# Queue Value Net Hazard Keep/Cancel v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Research question

This family stopped tuning the v1 adverse-state threshold. It directly fitted side-specific cause hazards for every active exposure-increasing order:

- favorable fill;
- adverse fill;
- cancel;
- adverse price jump;
- campaign repair;
- queue recovery as a separate transition hazard.

The action state was selected from explicit order value:

\[
V_{keep}(x)-V_{cancel/reenter}(x),
\]

not from another manually chosen toxicity cutoff. `K0` kept the order and its queue position. `K1` cancelled, waited for the actual ACK and state exit, then bound the first baseline re-entry order. BUY and SELL were fitted separately; reducing quotes, size, inventory limit, taker behavior, and external references were unchanged.

## Frozen identity

- Family: `queue_value_net_hazard_keep_cancel_v2`
- Behavior: `K0/K1 = 50%/50%`, one intervention per campaign
- Model fit: 12 chronological earlier days
- Internal embargo: 1 day
- Calibration: 5 days
- Development: 17 days
- Validation: 8 days, unread
- Sealed holdout: 8 days, unread
- Candidate-rate budget: 5%-30%, calibration target 15%
- Competing-risk bundle: `queue-value-net-cbca9d1a67686e05`
- Bundle SHA256: `d83e6cec877a3a7baf54a0189917ba93823bb80a8e45d7a8a316cce326c932b0`
- Evidence split SHA256: `bfbdde216573e970f95fe3690e9e38449c96b58b2a2efbfc494bee328b0f2c09`

The native runtime input was the strategy-independent exact-level snapshot/delta stream plus individual trades. Features were decision-time local L2, queue, flow, microprice, and campaign-so-far state. No future campaign field or external market state entered the hazard fit.

## Value and selectivity definitions

The hazard model produced cause probabilities over the frozen action horizon. `V_keep` combined favorable-fill value, adverse-fill loss, and adverse price jump loss. `V_cancel/reenter` combined post-recovery fresh-order option value, pre-ACK adverse-fill risk, and lost queue option value. Entry and exit used calibration quantiles of the resulting value difference, not fixed seconds or hand-tuned risk levels.

A defensive maker action is allowed to reduce volume, but it must remove toxic fills faster than all fills. On the common randomized-decision denominator:

\[
r_F = 1-\frac{F_1}{F_0}, \qquad
r_T = 1-\frac{T_1}{T_0}
\]

The intuitive diagnostic is:

\[
L=\frac{r_T}{r_F}.
\]

Because this ratio is unstable when activity barely changes, formal inference uses:

\[
S_T=r_T-r_F
\]

and:

\[
R_T=\log\frac{T_0/F_0}{T_1/F_1}, \qquad
N_T=\tanh\left(\frac{R_T}{\log 2}\right).
\]

Positive values mean toxic fills fell disproportionately faster. Cancelling all orders cannot pass: both reductions approach 100%, so `S_T` approaches zero. The frozen family-spec prose accidentally wrote the reciprocal log-ratio; the checkpointed implementation, tests, and score profile consistently use the formula above. The frozen artifact was not edited after outcomes.

For future families, `action_execution_selective_v2` removes a fixed absolute fill-retention gate. Large volume loss is allowed only when conditional net value, `S_T`, and `R_T` all have positive day-clustered lower bounds.

## Calibration and mechanics

All BUY/SELL cause models and queue-recovery hazards produced finite calibrated artifacts. Calibration activation was approximately 15% on both sides. The favorable/adverse fill amplitudes were approximately:

| Side | Favorable | Adverse |
|---|---:|---:|
| BUY | +0.798 bps | -1.654 bps |
| SELL | +0.930 bps | -1.509 bps |

The one-day mechanics smoke exercised cancel request, ACK, wait, state exit, re-entry binding, queue reset, and subsequent campaign accounting. Runtime was about 3.1% slower than v1. This established mechanics only, not action value.

## Development result

The frozen 17-day Development replay produced 1,101 independent interventions: 563 keep and 538 cancel, split into 616 BUY and 485 SELL. The eligible campaign rate was 17.10%, inside the preregistered budget.

### Toxic-fill selectivity

| Metric | Point | 95% UTC-day interval |
|---|---:|---:|
| Intervention fills retention | 7.67% | [4.07%, 12.22%] |
| Toxic fills retention | 8.29% | [3.89%, 13.97%] |
| Toxic-fill reduction | 91.71% | [86.03%, 96.11%] |
| All-fill reduction | 92.33% | [87.78%, 95.93%] |
| Reduction leverage `L` | 0.9933 | diagnostic only |
| Reduction surplus `S_T` | -0.0061 | [-0.0326, +0.0222] |
| Selectivity log ratio `R_T` | -0.0771 | [-0.3789, +0.3712] |
| Nonlinear score `N_T` | -0.1107 | [-0.4980, +0.4896] |

K1 therefore removed toxic fills slightly *slower* than it removed all fills. It was a broad participation shutdown, not selective toxicity avoidance.

### Randomized ITT action value

The primary diagnostic retained every randomized row and used inverse- propensity Hajek arm means with 5,000 complete-UTC-day bootstrap samples. Rows with incomplete native paths were not selected away.

| Scope | K1 - K0 reward | 95% UTC-day interval | Positive days |
|---|---:|---:|---:|
| Pooled | -0.01448 USDC/intervention | [-0.02577, -0.00239] | 4/17 |
| BUY | -0.02310 | [-0.04377, -0.00449] | 5/17 |
| SELL | -0.00347 | [-0.02352, +0.01550] | 6/16 |

BUY was significantly harmful. SELL had no positive evidence. Pooled negative terminal and q10 protection intervals also crossed below zero. MAE and repair time improved, but these risk diagnostics cannot compensate for negative action value and failed selectivity.

The full randomized strategy path reported 99.37% aggregate fills retention and `+1.3321 USDC` aggregate PnL, but only 7/17 days were positive. This does not override the order-level result: the whole-run delta includes changed campaign birth/death and downstream interference, while the intervention ITT estimates the registered action on its common eligible population.

## Native support boundary

- Seed support: 96.09%, below the frozen 98% gate
- Outcome support: 89.92%, below the frozen 98% gate
- Same-millisecond ambiguity: 99 rows
- Invalid native paths: 68 rows

Strict DR/OPE is therefore blocked. Filtering unsupported outcomes would condition on an action-dependent post-treatment state, so the randomized ITT kept them and remains diagnostic within the frozen simulator.

## Decision

The unified scorecard returned `diagnostic_only_support_failed`, a null ranking score, and hard failures for reward, selectivity, terminal protection, and intervention activity. This exact family is closed at Development.

- Do not tune its entry/exit advantage thresholds.
- Do not reinterpret aggregate PnL as action uplift.
- Do not open Validation or sealed holdout.
- Do not deploy it to live.

The useful conclusion is narrower: direct competing-risk estimation and net order value are now implemented, but this particular cancel-until-state-exit action is too destructive. A future action must preserve more favorable queue option value or use a materially different re-entry action, with a newly frozen family identity and the volume-tolerant selective-v2 score contract.
