# Current Live-Held BER Strict-Native Latency Baseline 50-Day v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Date: 2026-08-10

Status: one-day strict mechanics passed; the 50-day panel has not run.

## Scope

This successor repairs the execution scope of the current 50-day compatibility denominator. It uses the same live-held global-BER control policy, but replaces the normalized top-20/100ms C++ execution approximation with:

- the raw CryptoHFT snapshot/delta tape and 24-hour D-1 warmup;
- the strict Python native-book scheduler and exact price-tick queue lookup;
- the frozen queue-calibration-v3 artifact;
- sampled AWS Tokyo new-order, cancel-order, and execution-book visibility latency distributions;
- the existing merged 100ms policy clock as the nominal live loop cadence.

The historical 50-day panel does not contain exact AWS receive-time callback, drop, reconnect, lock-wait, or private fill-visibility tapes. This identity is therefore a latency-calibrated transport simulation, not exact historical live reproduction.

## Frozen Inputs

- Spec SHA256: `4d9a227ec2a8e090db973759f74cc58e7ebf295927ddab18d196a369c31cf726`
- Runner SHA256: `a50b049958e74b7372eea268b992af95f5fd31bf2dd488d6d7d322123d59b7e7`
- Queue v3 SHA256: `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd`
- AWS latency profile SHA256: `4e346af66e9fe25605bddc3888de9a2f3dc79b2323e4947484a3d766d3ae674d`
- New-order samples: 6,869
- Cancel-order samples: 6,577
- Visibility samples: 9,551

Preflight found all 50 target days and all 50 natural D-1 warmup days present, for 2,400 raw source hours.

## One-Day Result

The added date `2026-06-29` passed strict mechanics:

| Metric | Result |
|---|---:|
| Native events consumed | 5,086,247 |
| Native events accepted | 3,436,333 |
| Queue lookups | 19,460 |
| Exact queue | 9,797 |
| Known-zero queue | 9,663 |
| Missing queue | 0 |
| Source gaps / invalid sequence / sequence gaps / time reversals | 0 / 0 / 0 / 0 |
| Visibility applications | 14,825 |
| Mean / max sampled visibility delay | 198.832ms / 5,237ms |
| Campaign accounting error | `-1.71e-13 USDC` |

The execution path changed materially:

| `2026-06-29` | Old top-20/100ms diagnostic | Strict-native + latency |
|---|---:|---:|
| Terminal MTM | -7.878888 USDC | -7.132700 USDC |
| Closed-campaign value | -7.866988 USDC | -7.121700 USDC |
| Fills | 489 | 921 |

This single date proves that raw queue and latency inputs are active and economically relevant. It does not establish the direction or magnitude of the 50-day correction.

Authoritative one-day artifacts are under `${NARROWGATE_DATA_ROOT}/reports/current_live_held_ber_strict_native_latency_baseline_50d_v1_20260810/days/2026-06-29/`. The summary SHA256 is `b9ee62c3dc158dc16a731438142b40c0f7706577cce08c3f4c7be1804a523375`.

## Permission Boundary

- 50-day result: not available.
- Exact historical receive-time authority: false.
- Action authority: false.
- Live authority: false.

The next admissible result is the frozen 50-day panel under this exact identity. Drop/reconnect/lock-wait and private user-stream visibility require a separate transport-sensitivity successor unless exact historical tapes become available.
