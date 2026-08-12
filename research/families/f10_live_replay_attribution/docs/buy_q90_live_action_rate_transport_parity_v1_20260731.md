# BUY q90 Live Action-Rate Transport Parity v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

Development mechanics diagnostic complete.

Decision:

`historical_q90_replay_under_treated_by_mixed_visibility_clock`

This result identifies a replay clock defect. It does not establish that the live q90 policy is harmful, beneficial, or correctly calibrated. Prediction, transport, action, rollback, and live-deployment permissions remain false. No PnL, markout, campaign reward, Validation, or sealed holdout was read.

## Question

The historical q90 ON arm produced 105 cancel requests in about 960 replay hours, or 0.109379 cancel/hour. The live receive-time path produced 2,940 cancel requests in 143.34 hours, or 20.510714 cancels/hour. This audit asks which factor creates the approximately 188-fold rate difference:

\[
\frac{C}{h}
=
\frac{E}{h}
\times
\frac{V}{E}
\times
\frac{C}{V},
\]

where \(E\) is emitted q90 evaluation rows, \(V\) is valid evaluation rows, and \(C\) is the first threshold crossing that issues a cancel request.

## Aggregate Decomposition

| Measure | Live | Historical replay | Live / replay |
|---|---:|---:|---:|
| Evaluations/hour | 27,710.233 | 26,621.720 | 1.041x |
| Cancel/evaluation | 0.00074019 | 0.00000411 | 180.153x |
| Cancels/hour | 20.510714 | 0.109379 | 187.519x |

The main-loop/evaluation cadence is already aligned. The aggregate anomaly is inside the probability that an evaluation reaches a cancel request.

Live cancel and recovery logs are internally consistent:

- 2,940 cancel requests and 2,751 recoveries;
- 93.57% recovery/cancel ratio;
- no repeated cancel for the same active client-order id;
- median cancel-to-recovery time 0.214 seconds;
- median recovery-to-next-cancel time 30.238 seconds.

These observations reject a simple duplicate-log explanation.

## Same-Date Sensitivity

The only full overlapping local day was 2026-07-25. It is Grade B because the normalized source contains a 28.409-second internal gap, so this result is sensitivity evidence only.

| Measure | Live 2026-07-25 | Replay 2026-07-25 | Ratio |
|---|---:|---:|---:|
| Evaluation rows/hour | 20,144.500 | 32,413.669 | 0.621x |
| Valid/evaluation | 86.9979% | 0.4785% | 181.826x |
| Cancel/valid evaluation | 0.04375% | 0.05373% | 0.814x |
| Cancels/hour | 7.666667 | 0.083336 | 91.997x |

The exact factor product is:

\[
0.62148 \times 181.82608 \times 0.81412 = 91.99668.
\]

The model's conditional crossing intensity is not larger by two orders of magnitude in live. The two-order-of-magnitude difference is almost entirely the replay valid-risk denominator.

## Native Path Audit

The replay validity loss is not caused by shallow depth or sequence failure:

- native queue seed lookups: 7,225;
- exact seeds: 4,383;
- known-zero seeds: 2,839;
- missing seeds: 3;
- activation seed support: 99.9585%;
- later path-invalidated orders: 139, or 1.9247% of supported seeds;
- native events consumed: 4,963,041;
- sequence gaps and invalid sequence messages: 0;
- Python/C++ q90 mismatches: 0.

Thus, neither exact-price activation coverage nor explicit path invalidation can explain a replay valid-evaluation rate of only 0.4785%.

## Root Cause

The replay mixes two clocks in one causal validity comparison.

The historical scheduler advances decisions on exchange time. Native level changes copy CryptoHFT provider `received_time` into the active-order path. The q90 runtime then constructs:

```text
feature_source_ts_ns =
    max(path.receive_ts_ns, last_trade_receive_ts_ns)
```

and requires:

```text
feature_source_ts_ns <= now_ns
```

where `now_ns` is the earlier exchange-time replay clock. The implementation is visible in `strategy/dynamic_fill_hazard_model.py` around the feature-source construction and validity gate, while native replay propagates `change.receive_ts_ns` in `models/backtest_tick.py`.

A four-hour 2026-07-25 source sample contained 393,358 unique native messages:

| Provider receive minus exchange event time | Milliseconds |
|---|---:|
| Positive fraction | 99.9997% |
| p10 | 117.506 |
| median | 124.115 |
| p90 | 129.797 |
| p99 | 130.791 |
| maximum | 434.518 |

Normal transport latency is therefore interpreted as a future feature. The runtime correctly fails closed under the values it receives, but the replay clock contract is internally inconsistent. Valid observations occur mainly during quiet intervals after exchange time catches up with the most recent provider receive timestamp.

This explains why same-date replay remains near the historical action rate:

- historical and same-date replays share the mixed-clock censoring;
- live uses one receive-time-visible clock and does not incur that censoring;
- later live days also show a higher conditional crossing rate than July 25, so current market state may add intensity after the clock defect is removed.

## Evidence Boundary

The correct conclusion is not "live q90 is firing 190 times too often."

The supported conclusion is:

> Historical q90 replay executed a much weaker treatment than live because its valid q90 risk set was nearly eliminated by a mixed exchange/receive clock. The historical ON/OFF terminal result is not transport evidence for the current live treatment intensity.

The local and remote q90-sensitive Python AST semantics match. The live action log also matches the live shadow cancel count exactly. No duplicate cancel, generation-reset, or evaluation-cadence bug was identified in the live path.

SELL adverse selection and multi-level SHORT campaign loss remain separate economic problems. Fixing replay parity cannot be assumed to fix either.

## Required Repair

Create a new replay identity with one coherent visibility clock:

1. Schedule native book and trade events at a single causal visible timestamp.
2. Either use provider receive-time as a sensitivity clock, or use exchange time plus a frozen environment-labelled latency draw and feature-ready delay.
3. Never compare provider receive timestamps directly with exchange-time `now`.
4. Share the same latency/random path across q90 ON/OFF arms.
5. Keep same-millisecond unresolved ordering fail-closed.
6. Require valid-risk denominator parity, role parity, score-distribution parity, cancel/valid parity, and Python/C++ lifecycle parity.
7. Re-run q90 ON/OFF at the corrected treatment intensity before any policy decision.
8. Retain AWS Tokyo receive-time shadow parity as the final live transport gate; CryptoHFT provider receive-time cannot substitute for AWS time.

No q90 threshold, recovery threshold, or live configuration may be changed from this diagnostic.

## Frozen Evidence

- Main audit report SHA256: `60b43a11f1bb58e30e646adabc46143bd8547adcea7c5a45e5c51e05d42d9737`
- Same-date mechanics SHA256: `799cd1ea6bbaf5adbeb5050017611788cc7b3dc3514e17c7c0f63f7e16c3d541`
- Same-date validity diagnostic SHA256: `8c3b9f0cf09674ed9df39a2ca6b5a62a5bfd53eb0c3bafffa92f1550071323d2`
- Main audit spec canonical SHA256: `cc24b910f7e5d898382b04afb6615bd08722d13bff5acc83ef77439e40ea80ae`
- Same-date mechanics spec canonical SHA256: `3b051670becb168f9b56b27146c5c1e98844c3c1239eccfb429085489cb077cb`
- Validity diagnostic spec canonical SHA256: `a1e1dac1c6fe4975b0ae27659986bc9ae2e12f1edea1d60dcc595ed5dcaf11c2`
