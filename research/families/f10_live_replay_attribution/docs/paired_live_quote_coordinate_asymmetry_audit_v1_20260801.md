# Paired Live Quote Coordinate Asymmetry Audit v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: **diagnostic complete; structural SELL-too-close hypothesis not supported; mixed-mid fill-edge claim withdrawn; one markout-sign semantic default corrected locally; no action or live authority**.

## Question and boundary

The audit asks whether the observed 360-hour opener entry-edge difference came from a structural quote-coordinate defect:

\[
(ask-mid) < (mid-bid)
\]

for SELL exposure while flat. It does not estimate an action effect or read a strategy panel. BUY and SELL are paired only when the live engine wrote them in the same decision cycle, with identical `mid` and inventory state. The primary denominator additionally requires flat inventory and both sides to allow post and exposure increase.

Window: `2026-07-17 06:58:01` through `2026-08-01 06:58:01 UTC`.

## Paired quote result

The source contains 188,463 complete decision pairs, including 53,137 flat common-support pairs.

| Coordinate | BUY distance | SELL distance | SELL - BUY |
|---|---:|---:|---:|
| quote-core/base | 2.82504 bps | 2.81827 bps | -0.00677 bps |
| final target | 2.92213 bps | 2.93661 bps | +0.01448 bps |

The final target therefore places SELL slightly **farther**, not closer. The mean difference is about 0.94 tick and the median is one tick. Equal-day clustered inference gives:

\[
\operatorname{mean}_{day}(d_{SELL}-d_{BUY})
=0.01743\ \text{bps},
\qquad 95\%\ CI=[-0.00299,+0.03784].
\]

Daily direction is positive on 11 of 16 UTC slices and ranges from -0.07105 to +0.08300 bps. This is neither a stable -0.40 bps SELL coordinate defect nor a one-sided structural effect.

The policy layer widens BUY by 0.09709 bps and SELL by 0.11834 bps on average. That small difference moves the base coordinate from -0.00677 bps to the final +0.01448 bps. State decomposition behaves as expected: when only SELL is in `defend`, SELL is farther; when only BUY is in `defend`, BUY is farther. The legacy decision schema does not expose every cap and raw pre-rounding field, so this audit does not pretend to assign a separate causal effect to each cap.

## Why the old 0.40 bps gap appeared

The previous 360-hour fill table mixed two mid identities. Exact lifecycle rows carry the order's decision context and finite `age_ms`. Rows without a matched order outcome could inherit a fallback mid from sparse quote evidence. There were 86 quote-log gaps longer than 60 seconds; the largest was 65,934.643 seconds, from `2026-07-25 14:44:50.319 UTC` to `2026-07-26 09:03:44.962 UTC`.

Among 2,691 opener fills, only 31 lacked lifecycle identity. Those 31 included fallback entry edges between roughly -57 and +46 bps and changed the side means materially:

| Opener identity | BUY | SELL | SELL - BUY |
|---|---:|---:|---:|
| mixed old table, 2,691 fills | 2.93612 bps | 2.53728 bps | -0.39885 bps |
| exact lifecycle, 2,660 fills | 2.79772 bps | 2.80697 bps | +0.00925 bps |

Therefore the historical `BUY 2.94 vs SELL 2.54 bps` comparison is withdrawn. Missing lifecycle rows must be excluded rather than repaired with a different mid estimand.

The adverse-selection diagnosis still survives as a more modest sensitivity result. Requiring exact lifecycle identity and future quote observation delay no greater than 10 seconds retains 7,483/7,924 fills (94.43%):

| Side | Entry edge | Subsequent move | 10s maker value | Win rate |
|---|---:|---:|---:|---:|
| BUY | +2.60257 bps | -3.02936 bps | -0.42680 bps | 41.63% |
| SELL | +2.70062 bps | -3.19030 bps | -0.48968 bps | 40.32% |

Both sides remain negative, but the fresh SELL-minus-BUY value gap is about -0.06288 bps, not the old -0.61 bps opener gap. Campaign cash-flow and terminal PnL identities do not depend on this fallback mid and are not invalidated by the fill-table correction.

## Markout sign correction

The observed EC2 YAML hash is `896b8055d456935050dc0df8fdfa25184824ada6d212e539c91677f8f47f3278`. It omitted `markout_side_asymmetry_sign`, so the runtime code defaulted to `-1`. Runtime `config.py` and `quote_core.py` hashes were respectively `2eb44ab...f4c6` and `ceeb0a3...6605`.

Both side EMAs use maker-signed markout:

\[
m_{BUY}=P_H-P_{fill},\qquad
m_{SELL}=P_{fill}-P_H,
\]

so positive is favorable for both. If BUY EMA is better than SELL EMA, the semantic response is BUY closer and SELL farther, which requires sign `+1` in the current quote-core parameterization. The `-1` default is therefore a real semantic inversion.

It is not the 360-hour loss explanation. On the flat common-support rows, the estimated aggregate corrected-minus-observed coordinate shift is only -0.000407 bps; its p10/p90 are about -0.02367/+0.02531 bps. Python live, backtest, and C++ defaults have been changed locally to `+1`, while historical arms that explicitly set `-1` remain reproducible. This correction has not been deployed to EC2 and creates no live authority.

## Decision

1. The structural `SELL quote too close by 0.40 bps` hypothesis is rejected.
2. The old mixed-identity opener edge comparison is withdrawn.
3. The markout side-sign default is corrected as an engineering contract, not registered as an alpha arm.
4. No quote-distance action, inventory action, Validation read, holdout read, or live promotion follows from this diagnostic.
5. After the separate q90 terminal-risk-set repair, further PnL progress needs genuinely decision-visible alpha or a different baseline economic mechanism; another inventory suppression/repair parameter search is not supported.

Machine result SHA256: `dfedb81c768ab8c52c4b508af7ea1da02827dfc8c82adba6ef62d8483fb5484b`.

Authoritative external payloads: `${NARROWGATE_DATA_ROOT}/reports/paired_live_quote_coordinate_asymmetry_audit_v1_20260801/`.
