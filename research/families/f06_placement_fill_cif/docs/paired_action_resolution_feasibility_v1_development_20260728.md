# Paired Action Resolution Feasibility v1: Development Result

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: Development complete and family closed. Validation and sealed holdout remain unread. No prediction, value, action experiment, or live deployment is authorized.

## Question

This family trained no model. It asked whether the raw native-L2 paired paths can distinguish baseline-relative placement distances before another ordered fill surface is attempted.

Each frozen placement cohort was expanded to seven prices:

```text
closer 4 / 2 / 1 ticks, current, farther 1 / 2 / 4 ticks
```

All prices shared the same market path, baseline lifecycle, latency identity, and a 5,000ms clock measured from activation. The 5,000ms cut is the hash-frozen `strategy.requote_interval`; it is an engineering report clock, not a natural or optimal fill horizon.

## Frozen Identity

- Spec: `paired_action_resolution_feasibility_v1_spec_20260728.json`, SHA256 `84529857e60ad5755a732bc9d6b138c21e786f7dc4bc98d2e6fcd4e639c20915`.
- Implementation SHA256: `2a5cb5e31d8c5658bc7142408bde937bc318e723b1c50c21beed0c3131dafdc9`.
- Development report: `${NARROWGATE_RETIRED_DATA_ROOT}/reports/paired_action_resolution_feasibility_v1_development_20260728/report.json`, SHA256 `a6c36d5bb46a284a177b247dc3b8b99d96db92939cf2746abb366e907f578098`.
- Panel: 50 frozen Development UTC days from 2026-04-13 through 2026-06-25.
- Validation read: `false`.
- Sealed holdout read: `false`.

The clock producer, native module, sparse-tape builder, lifecycle adapter, trade loader, source panel, every raw book file, and every individual-trade file were byte-hash checked. A cache-only rerun produced the identical report SHA256.

## Mechanics

| Check | Result |
|---|---:|
| submitted cohorts | 800,853 |
| all-seven-activated cohorts | 800,692 |
| all-seven activation rate | 99.9799% |
| observed deeper-fill-without-shallower-fill violations | 0 |
| frozen +/-1-tick parity rows | 2,402,559 |
| activation-status mismatches | 0 |
| first-fill timestamp mismatches | 0 |
| filled-quantity mismatches | 0 |

This establishes that the expanded seven-price replay did not alter the frozen three-price matcher identity.

## Raw Fill Resolution

The estimand is the paired first-fill probability difference on the common clock. Day-clustered 95% simultaneous bands cover every BUY/SELL x opener/add/reducing x gap x contrast cell.

| Distance comparison | Cells with positive simultaneous lower bound |
|---|---:|
| closer 1 tick vs current | 0/6 |
| current vs farther 1 tick | 0/6 |
| closer 1 tick vs farther 1 tick, total span 2 ticks | 6/6 |
| closer 2 ticks vs current | 6/6 |
| current vs farther 2 ticks | 6/6 |
| closer 4 ticks vs current | 6/6 |
| current vs farther 4 ticks | 6/6 |

For every side-role-direction cell, the minimum one-sided detectable distance was 2 ticks. The mean one-sided probability increment was approximately:

- 1 tick: `0.00049` to `0.00056`, but all simultaneous intervals crossed zero;
- 2 ticks: `0.00099` to `0.00104`, all 12 cells passed;
- 4 ticks: `0.00200` to `0.00203`, all 12 cells passed.

Thus the ordered v1 model's essentially zero one-tick output was not purely a model defect. The raw data also does not identify either adjacent one-tick move under the frozen multiplicity and day-cluster contract. It does identify a two-tick total separation.

## Economic Resolution

Raw fill resolution is not action value. For each shallower/deeper pair, the audit formed a conservative `deeper - shallower` interval:

```text
shared fills x deterministic deeper execution-price improvement
+/-
shallower-only fills x 100bps stressed conditional fill value
```

The 100bps stress is a judgmental envelope, not an estimated markout. The comparison error is the empirical maximum pending-fill uncertainty from the frozen predecessor report:

```text
0.0002980355530292383 USDC / decision
```

Results:

| Gate | Result |
|---|---:|
| raw fill-resolution cells | 42/54 |
| economic interval-resolution cells | 0/54 |
| one-tick economic cells | 0/18 |

Even at the widest paired contrast, the largest deterministic shared-fill price improvement was only about `0.00003284 USDC/decision`, roughly one ninth of the maximum pending uncertainty bound and still less than one third of the smallest predecessor bound, approximately `0.000106`. Marginal-fill value would need to be both directionally stable and large enough to overcome the remaining uncertainty. For one-sided comparisons, the approximate conditional value scale required just to equal the pending bound was:

| Gap | Required marginal-fill scale |
|---|---:|
| 1 tick | about 69-164bps |
| 2 ticks | about 36-61bps |
| 4 ticks | about 17-32bps |

This experiment did not estimate those conditional values, so it cannot use their possible magnitude as positive action evidence.

## Decision

The frozen decision is:

```text
close_fill_surface_path_economic_resolution_absent
```

Consequences:

1. Do not create `ordered_common_support_fill_surface_v2`.
2. Do not fit a paired action-gap head merely because 2/4-tick raw fill differences are statistically detectable.
3. Do not open Validation or sealed holdout for this family.
4. Do not create `placement_action_value_surface_v1` or a randomized placement action experiment from this evidence.

The existing absolute fill-CIF components may remain diagnostics. They do not have authority to select placement distance. Any future reopening requires a new economic estimand with materially tighter pending uncertainty or direct, out-of-sample marginal-fill value evidence; it cannot be a calibration or monotonicity patch to this identity.
