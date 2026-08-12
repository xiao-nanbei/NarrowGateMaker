# Dynamic mechanism campaign audit v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Current status (2026-07-27): retain the actual EC2 fill/campaign component as historical descriptive evidence only. Exact Development replay add-toxicity, repair-delay, distance, cap-hit, campaign and PnL values used the superseded top-20/q0.70 and model/book identity and remain withdrawn. Live observations and replay values must not be combined into a causal action conclusion; a replay successor requires a newly frozen current denominator.

## Status

This audit is observational attribution under the current operational baseline. It does not change live policy and it does not identify action uplift.

Frozen identity:

- Config SHA256: `1ba03a6d9c4e091d531346f70fccedde882bd8ab1fc2cd4ddbe31e995ff5f601`
- Empirical P3 SHA256: `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652`
- Queue v3 q0.70 SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- AWS Tokyo 2 vCPU / 4 GiB latency tape SHA256: `2c025fc77df39e9944aff3728dcb96484c8b14c4712b04b0b743b8646bd38df2`
- Development replay contract SHA256: `a3383848c7f7659a267141f90c65fe7b832e1643c5291e5b67e0cc60eaba63d0`
- Development panel: 56 frozen strict event-L2 days, 2026-01-01 through 2026-06-12
- Live diagnostic window: 2026-07-19 18:51:46.551798 UTC through 2026-07-21 18:51:46.551798 UTC

The complete generated report and per-campaign tables are under:

`MarketData/NarrowGate_BTCUSDC/reports/dynamic_mechanism_campaign_audit_v1/current_baseline_dev56_v2_20260722/`

## Frozen live decomposition

The exact live window contains 508 closed campaigns and 1,556 fills in those closed campaigns. One final fill opened a censored campaign and is excluded.

| Group | Campaigns | Terminal PnL | Mean PnL | Median duration |
|---|---:|---:|---:|---:|
| No add | 408 | +0.3122 | +0.0008 | 89.8s |
| At least one add | 100 | -9.3668 | -0.0937 | 509.4s |
| Worst 20 | 20 | -8.7043 | -0.4352 | 689.8s |

The labels `no_add_net_positive` and `add_net_negative` describe each group's aggregate outcome. They do not claim that every member has the same sign.

## Dynamic mechanism transmission

### Regime and P3

Regime scaling is active rather than nominal:

- Live campaign mean scale: about `1.53x`.
- Development campaign mean scale: about `1.81x`.
- Development add campaigns: about `1.63x`.

The P3 pair-spread floor binds materially, but it does not dominate every decision:

- Live sampled campaign rate: `28.9%`.
- Development all-campaign rate: `11.3%`.
- Development no-add rate: `8.7%`.
- Development add rate: `23.0%`.

This means low-volatility tightening is sometimes removed by P3, especially in add campaigns, but the mechanism is not permanently pinned to the floor.

### Effective kappa and depth ratio

The Development campaign mean `kappa_used` is `0.067449`, versus empirical P3 `0.067438`. The average ratio is `1.00016`. Live is similarly close to one.

Depth kappa therefore has almost no ordinary-state action range under the current baseline. Rare states move it, but it is not a meaningful continuous controller for most campaigns.

### Dynamic cap

The cap level itself moves:

- Live mean cap: `24.52 bps`.
- Development mean cap: `31.72 bps`.
- Development add campaigns: `27.21 bps`.

It is almost never binding:

- Live sampled cap-hit rate: `0%`.
- Development cap-hit/final-compress rate: about `0.24%`.
- Development add-campaign rate: about `0.08%`.

The current dynamic cap is therefore an operational ceiling, not a material source of campaign adaptation.

### Cooldown and quote geometry

The live cooldown samples show the expected staircase rather than a smooth adaptive mechanism:

- `85s`: 3 campaign maxima
- `170s`: 61
- `255s`: 30
- `340s`: 8
- `425s`: 2

Development has the same `85/170/255/...` structure. Median maximum cooldown is `170s` for add campaigns and rises to roughly `319s` in the worst 20.

Development add campaigns have mean add distance `3.85 -> 4.27 bps` from base to final. Reducing distance is `3.35 -> 3.73 bps`. The reducing side is often widened by common overlays, but actual defense pause is rare: campaign-mean reducing pause rate is below `0.1%`.

## Add toxicity versus repair failure

For attribution only, the audit predeclares:

- immediate add toxicity: first-add 30s maker-signed markout `<= -0.5 bps`;
- repair failure: more than `300s` from first add to first reducing fill.

These thresholds are diagnostics, not treatment definitions.

### Live 48h

| Add class | Campaigns | Terminal PnL |
|---|---:|---:|
| Immediate toxicity only | 43 | -4.0939 |
| Repair failure only | 11 | -1.6059 |
| Mixed | 5 | -2.0747 |
| Other add | 41 | -1.5923 |

The 100 add campaigns have first-add 30s markout averaging about `-0.82 bps`. SHORT add campaigns are worse in this window (`-1.03 bps`, `-5.9967` terminal PnL) than LONG add campaigns (`-0.61 bps`, `-3.3701`). This is a two-day diagnostic, not a stable side claim.

### Development56

| Add class | Campaigns | Terminal PnL |
|---|---:|---:|
| Immediate toxicity only | 672 | -104.6215 |
| Repair failure only | 280 | -23.8250 |
| Mixed | 303 | -40.7765 |
| Other add | 623 | -8.8760 |

All 1,878 add campaigns sum to `-178.099`, while 8,496 no-add campaigns sum to `+11.039`. The first-add 30s markout averages about `-1.01 bps`. This association does not identify the value of skipping an add because action changes fill, queue, and the subsequent campaign path.

## Reducing-side repair support gate

The support probe uses the baseline reducing price and individual execution trades. A touch or strict trade-through is not treated as a queue fill counterfactual.

| Inventory side | Defense-pause campaigns | Active days | Pause intervals | Duration | Strict through | Affected negative-add rate |
|---|---:|---:|---:|---:|---:|---:|
| LONG / SELL reducing | 74 | 31 | 252 | 1,647.95s | 14 | 3.46% |
| SHORT / BUY reducing | 68 | 38 | 148 | 894.81s | 10 | 6.00% |

The predeclared gate required at least 50 campaigns, 10 days, 20 strict-through intervals, and 5% affected negative-add campaigns per side. Neither side passes all four conditions. The live window is even thinner: five pause intervals across three campaigns, with one strict trade-through.

The inventory emergency threshold is also inactive. `0.5 * 0.026 = 0.013 BTC`, while the observed maximum is `0.007 BTC` live and `0.012 BTC` in Development. The loss-based emergency override does activate in one Development tail campaign, so the two emergency paths must not be conflated.

## Decision

`reducing_repair_release_v1` is closed at the support stage. It is not generated, tested, or promoted, and live defense remains unchanged.

The next action family should target the first baseline-eligible exposure-increasing add opportunity, not the realized first add fill:

- `A0`: baseline add quote cycle;
- `A1`: skip exactly one add quote cycle;
- BUY and SELL evaluated separately;
- one 50/50 randomized intervention per campaign;
- reducing quote, size, inventory limit, and taker behavior unchanged;
- full queue, latency, fill, and campaign continuation replay;
- reward is decision-to-terminal MTM with campaign cost, repair time, tail, and activity reported separately.

This family must first pass an action-support preflight. The observational losses above are the reason to test it, not evidence that `A1` is profitable.
