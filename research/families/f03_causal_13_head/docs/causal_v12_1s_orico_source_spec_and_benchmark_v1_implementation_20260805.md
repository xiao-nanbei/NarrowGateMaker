# Causal-v12 1s `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` Source Spec And Benchmark v1

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: exact `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` source-spec builder and small-panel benchmark executable; full-day materialization, training, and live authority remain closed.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

This implementation adds two F03-only utilities:

- `causal_v12_1s_orico_source_spec.py`: resolves and validates one exact `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` `DailySourceBundle` for a target UTC day;
- `causal_v12_1s_materialization_benchmark.py`: measures a small, deterministic canonical-cutoff panel without labels, predictions, PnL, or bulk output.

No label generator, training contract, live, strategy, C++, registry, or frozen Spec was modified.

## Fail-closed Source Profile

The first frozen profile is `provider_normalized_v1`. Given target day `D` and an explicit market-data root, it derives only the following exact authority:

| Group | Exact binding |
|---|---|
| Local trade tempo | `D-1` and `D` Parquet under `trade_features_causal_v5_expanded_20250801_20260725/BTCUSDC` |
| Local manifest | The profile's single `manifest.json`; its two daily records are validated by the existing physical source probe |
| L2 | `D-1` and `D` Parquet under `normalized_tardis_l2_100ms_v1/l2` |
| L2 quality | Matching `D-1` and `D` JSON under `normalized_tardis_l2_100ms_v1/quality` |
| Metrics | Matching raw CSV under `raw_metrics` |
| BTCUSDT reference | Matching `D-1` and `D` 1s Parquet under `bars_1s` |
| BTCUSDT authority | Matching `.parquet.meta.json` for both reference days |

The builder never globs for another source family and never substitutes an older warmup day. A missing exact path, path escaping the selected root, an unknown profile, or any failed `physical_materialization_eligible` probe aborts before a source spec is published.

The CLI writes the probe first and the source spec last through atomic file replacement. Existing files are reused only when their canonical payload is identical; a different artifact at the same path is not overwritten.

## Benchmark Contract

The benchmark consumes the generated source spec. It selects a deterministic, evenly distributed sample of canonical 1s target-day cutoffs. Every sampled cutoff remains inside `[D 00:00:00, D+1 00:00:00)` and passes the exact `cutoff-1s` predecessor-bar check.

Timing is split into:

1. complete physical source probe;
2. source loading and causal 1s reduction;
3. 173-feature row computation;
4. compressed Parquet construction.

The full-day estimate counts probe and source loading once, then linearly extends measured feature-compute and panel-write time to 86,400 rows. Peak RSS comes from process-level `getrusage` in a fresh process for each row count. These are engineering estimates, not training or economic evidence.

## Real `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` Result: 2025-08-02

The builder used `2025-08-01` as the exact natural-day warmup and produced:

- source-spec SHA256: `c56588464a5b6e36bae570f0310cfe5ff239eedbd8ab0d4aeea6dd6cf781e59a`;
- probe SHA256: `677ba5b782bce655dde0c126e42e8c41432b53bc18e26999556678afc32a72a5`;
- bundle identity: `bfda22b3b523ce0c4b36e25b2b4e2acdddf67f19c5f645682369fe51cd6b1986`;
- physical materialization eligible: true;
- fallback discovery used: false.

The two benchmark runs used separate processes and separate temporary output directories:

| Canonical rows | Source probe | Source load | Feature rows/s | Observed process wall | Full-day estimate | Peak RSS | Panel size |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 1.7697s | 5.6949s | 6.1268 | 23.9647s | 14,114.0s / 3.9206h | 1.2951 GiB | 147,349 bytes |
| 1,000 | 1.7270s | 5.5770s | 6.0671 | 172.2915s | 14,251.9s / 3.9589h | 1.3031 GiB | 1,121,771 bytes |

The estimates agree within about 1%. Parquet writing was greater than 18,000 rows/s in both runs; the current bottleneck is Python feature-row computation, not source probing or output I/O. A full day was not started.

Temporary diagnostic artifacts:

- `${NARROWGATE_EPHEMERAL_ROOT}/f03_1s_orico_spec_20250802_v1`
- `${NARROWGATE_EPHEMERAL_ROOT}/f03_1s_orico_benchmark_20250802_v1/rows_100`
- `${NARROWGATE_EPHEMERAL_ROOT}/f03_1s_orico_benchmark_20250802_v1/rows_1000`

Their benchmark report SHA256 values are respectively:

- 100 rows: `304fa84338b78b4404bbfaa4b46fe00ecbc579687a86b201082a41b2cda599eb`;
- 1,000 rows: `672a4ec576045ecd072110ce842f1298c1682a7dac1c51e669de414e93bd2899`.

The temporary outputs are not durable panel authority and must not be used for training.

## Verification And Remaining Boundary

Using `.venv/bin/python`:

```text
ruff check: passed
pytest: 34 passed
```

Tests cover exact D-1/D path construction, refusal to discover substitute quality authority, failed-probe rejection, atomic round-trip source-spec publication, canonical cutoff sampling, and fixed-cost-preserving full-day extrapolation. Existing physical source/materializer/full-schema tests are included in the 34-test result.

The path is ready to build source specs and run bounded engineering probes. It does not authorize full-day or multi-day materialization, model retraining, economic replay, or deployment. The measured roughly four-hour Python full-day estimate is itself a performance blocker for bulk generation until reusable node caches or equivalent compute acceleration are introduced under a separate identity.
