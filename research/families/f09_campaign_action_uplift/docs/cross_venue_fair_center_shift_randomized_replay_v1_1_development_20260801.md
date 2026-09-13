# Cross-Venue Fair-Center Shift Randomized Replay v1.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

`close_cross_venue_fair_center_shift_on_development`

The continuous cross-venue fair-center shift is mechanically effective and has a positive pooled point estimate, but it does not pass the frozen Development alpha gates. Validation and sealed holdout remain unread. No action, shadow-deployment, or live authority is created.

## Frozen Identity

- Panel: the existing 40-day F09 Development identity, with 24 Grade-A days primary and 16 Grade-B days sensitivity-only.
- Assignment: campaign-level prospective 0.5/0.5 randomization before either full path is generated.
- Control: the current local quote center.
- Candidate: shift the whole bid/ask pair by the causal cross-venue fair-price adjustment while preserving pair spread, size, cooldown, inventory logic, reducing logic, latency, queue mechanics, and GTX constraints.
- BUY q90: disabled identically in both arms.
- Historical external input: causal one-second individual-trade bars with common support across all-venue and true leave-one-venue-out variants. This is historical sensitivity evidence, not AWS receive-time live transport.
- Primary outcome: assignment-to-campaign-terminal value in USDC.
- Score profile: `action_alpha_v1`, frozen hash `2c1d739632a918f2dd83d0c19143e0ff0cc9aecedfc79a549441d1732bb42fd9`.

Canonical Spec identity: `2738e1210a0d332ab967def073eccd9d93ad4a9c3c6f8428908bb5a0c4f94392`.

## Grade-A Primary Result

| Metric | Result |
|---|---:|
| Campaign rows | 9,164 |
| Candidate / control rows | 4,542 / 4,622 |
| Candidate assignment rate | 49.56% |
| Actual action-change rate | 100.00% |
| Activity retention | 92.35% |
| Fill retention | 97.00% |
| Reward uplift | +0.001446 USDC/assignment |
| Reward 95% interval | [-0.002732, +0.006053] |
| Positive UTC-day rate | 54.17% |
| HT policy value | +0.6351 USDC/day |
| HT policy-value 95% interval | [-1.0143, +2.3994] USDC/day |
| BUY reward uplift | +0.003324 USDC/assignment |
| BUY 95% interval | [-0.002637, +0.010096] |
| SELL reward uplift | -0.000429 USDC/assignment |
| SELL 95% interval | [-0.004671, +0.003930] |

The candidate improved descriptive q10 from -0.08577 to -0.08239 USDC and CVaR10 from -0.20246 to -0.18990 USDC. It also shortened repair time by 16.42 seconds, with a positive interval `[7.80, 25.33]`. These diagnostics do not compensate for the primary value lower bound crossing zero. Negative- terminal protection, q10 shortfall protection, MAE avoidance, repair-event, and censoring intervals also fail the frozen action scorecard requirements.

The authoritative scorecard is `hard_gate_failed`, with `ranking_score=null`, `total_score=-0.022059`, and these principal failures:

- policy-value lower bound is not positive;
- conditional net-value lower bound is not positive;
- positive-day rate is below the frozen 55% floor;
- terminal, q10, MAE, repair-event, and censoring gates are unsupported.

## Sensitivity And Transport

Grade B is positive: reward uplift is +0.004647 USDC/assignment with interval `[+0.000915, +0.007513]`, and HT policy value is +1.9308 USDC/day with interval `[+0.3410, +3.4592]`. Grade B is sensitivity-only and cannot rescue a failed Grade-A primary panel.

True leave-one-venue-out Grade-A point estimates are:

| Variant | Reward uplift | HT USDC/day | Direction |
|---|---:|---:|---|
| All venues | +0.001446 | +0.6351 | positive |
| Leave Bitget out | +0.000274 | +0.0524 | positive |
| Leave Bybit out | -0.000632 | -0.1986 | negative |
| Leave OKX out | +0.000872 | +0.1871 | positive |

The Bybit leave-out reversal fails the preregistered LOO direction gate. The result therefore depends materially on one venue and cannot be described as a transport-stable three-venue fair-price alpha.

Common historical support is 89.65% overall, with a minimum daily fraction of 66.73%. Outside common support, both arms use the local baseline center.

## Interpretation

The experiment rejects two simpler explanations:

- The action is not a no-op: it changed the candidate quote path in every Grade-A assigned campaign while retaining most fills and activity.
- The failure is not caused by a broad participation shutdown: fill retention is 97.00% and activity retention is 92.35%.

There is a promising descriptive BUY contribution and Grade-B result, but the primary Grade-A uncertainty, slightly negative SELL point estimate, and Bybit LOO dependency prevent action identification. These results may motivate new receive-time prediction evidence, but they may not be used to retune venue weights, basis windows, gain, side scaling, or eligibility on the consumed Development panel.

## Governance

- `validation_read=false`
- `sealed_holdout_read=false`
- `action_experiment_authorized=false`
- `live_deployment_authorized=false`
- `historical_trade_bar_live_transport_authority=false`
- `full_cpp_tick_replay_authority=false`

The live-shadow implementation remains code-only and is not active on EC2. The local private config enables its evidence log, but deployment preflight correctly rejects the existing 13-head bundle because it lacks the current causal feature-semantics identity, even while ML is disabled. This fail-closed contract must not be bypassed for a Development-closed action.

Authoritative artifacts:

- Report: `${NARROWGATE_DATA_ROOT}/reports/cross_venue_fair_center_shift_randomized_replay_v1_1_20260801/development/report.json`
- Scorecard: `${NARROWGATE_DATA_ROOT}/reports/cross_venue_fair_center_shift_randomized_replay_v1_1_20260801/development/scorecard.json`
- Canonical evidence: `${NARROWGATE_DATA_ROOT}/reports/cross_venue_fair_center_shift_randomized_replay_v1_1_20260801/development/canonical_evidence.json`
