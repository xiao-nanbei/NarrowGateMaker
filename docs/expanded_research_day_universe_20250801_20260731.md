# Expanded Source-Aware Research-Day Universe

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Decision

The retained research range was rescanned from 2025-08-01 without modifying the canonical native good-day registry. The resulting immutable research view contains 112 target UTC days:

| Source authority | Target days | Earliest target | Permission |
| --- | ---: | --- | --- |
| `provider_normalized_causal` | 67 | 2025-08-02 | Causal features, model training, calculation, and provider-normalized sensitivity replay |
| `native_formal_lifecycle` | 45 | 2026-04-13 | Causal features, model training, and formal native lifecycle replay |
| Total | 112 | 2025-08-02 | Source-aware union only |

The scan starts on 2025-08-01, but 2025-08-01 is used only as the D-1 midnight warmup for the first target. It is not itself a target day because the required 2025-07-31 warmup book is unavailable. Every admitted target has an immediately preceding natural-day book under the same source authority.

The union manifest is:

```text
${NARROWGATE_DATA_ROOT}/
  normalized_l2_research_union_v1/manifest.json
SHA256 fe851cf4c8aeafd7bcc5312e41fbcaef4f81a9fc1faaa12cc309315ff62623f5
```

The view uses hard links and contains 148 target-or-warmup book days. It does not duplicate source payloads and does not overwrite `normalized_l2_100ms_v2`.

## Authority Boundary

Provider-normalized Tardis rows have a causal provider clock, 100ms normalized top-20 state, and cross-channel quality checks. They do not contain Binance native `U/u/pu` sequence identity and cannot support exact hidden-queue inference. The following boundary is mandatory:

| Capability | Provider-normalized | Native formal |
| --- | --- | --- |
| Causal feature generation | Yes | Yes |
| 13-head model training | Yes | Yes |
| C++ core replay calculation | Sensitivity only | Yes |
| Exact queue or lifecycle policy evidence | No | Only when the experiment's stricter native contract also passes |
| Action authorization | No | Not implied by data admission |
| Live authorization | No | Not implied by data admission |

No experiment may mix provider and native days inside one replay arm without a source-stratified estimand. Provider days cannot acquire native authority by being present in the same feature bundle.

## Data Audit

All 153 UTC dates from 2025-08-01 through 2025-12-31 have the 15 required non-CryptoHFT sources. Tardis raw `book_ticker` and `incremental_book_L2` archives are complete for all 153 dates. After normalization and provider quality gates, 93 dates are provider candidates; requiring a valid target and valid D-1 natural-day warmup leaves 67 target days.

The 2025-08-29 BTCUSDC and BTCUSDT metrics archives contain 285 rather than 288 five-minute rows, with missing rows around 06:25-06:35 UTC. That date is excluded by the book target/D-1 gate as well and is not admitted to the 112-day universe.

The 2026 native component is the explicit Grade-A lifecycle set whose target and D-1 source identities both resolve under the immutable native root. This produces 45 native replay days.

## Derived Artifacts

### P3-Specific Touch Universe

F02 does not need D-1 L2 warmup for its non-overlapping same-day 10-second touch windows. It therefore froze a separate 93-day 2025 provider manifest, rather than borrowing the cross-family 67-day denominator. The resulting source-aware expanded static curve failed historical 2026 proper-score transport and worsened the 44-day native full quote path, so it did not replace the current v2 artifact. The reusable day-level reach cache is only about 8.17 MiB; authoritative reports remain under `reports/p3_touch_source_aware_expanded_v3_20260803/` and `reports/p3_touch_source_aware_expanded_v3_quote_path_20260803/`.

This result demonstrates the intended source-aware rule: a research family may admit more 2025 dates when its own estimand does not need D-1 or exact queue, but expanded availability does not imply that pooled static parameters transport to the current market.

### Taker Tempo

```text
${NARROWGATE_DATA_ROOT}/
  trade_features_causal_v5_expanded_20250801_20260725/
```

This contains 294 symbol-day files and 252,125,631 rows. It is derived data, not source authority.

### Causal Features

```text
${NARROWGATE_DATA_ROOT}/
  features_btcusdc_causal_v11_expanded_source_aware_20260731/
```

The bundle contains 112 daily feature files. Its frozen chronological split is:

| Role | Days |
| --- | ---: |
| Development train | 66 |
| Embargo 1 | 1 |
| Validation | 22 |
| Embargo 2 | 1 |
| Test diagnostic | 22 |

The feature manifest SHA256 is `2dafb78d7a0b4b551d9e54be18fa7834db67454730577f4a458b050ff7723828`. The label quote calibration remains the explicit empirical P3 artifact with SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`.

### Causal-v11 Candidate Bundle

```text
${NARROWGATE_DATA_ROOT}/
  model_runs/causal_v11_expanded_source_aware_20260731/
```

The candidate contains 13 heads with 173 model features. The training-summary SHA256 is `5896c8609c66499127293be0491972687c755044c60499b713948da15ec8e908`. Selected test diagnostics are:

| Head | Metric |
| --- | ---: |
| `dir_10s` AUC | 0.5287 |
| `dir_30s` AUC | 0.5086 |
| `dir_60s` AUC | 0.5114 |
| `tox_bid_5s` AUC | 0.5692 |
| `tox_ask_5s` AUC | 0.5664 |
| `tox_bid_10s` AUC | 0.5473 |
| `tox_ask_10s` AUC | 0.5396 |
| `vol_10s` IC | 0.6338 |
| `vol_30s` IC | 0.7130 |
| `vol_60s` IC | 0.7478 |

These are prediction diagnostics, not action or live evidence.

## Internal Caches

Reproducible tick-window payloads are stored on the internal disk:

```text
~/Library/Caches/NarrowGate_BTCUSDC/window_cache/
```

The v13 source-bound cache set contains 112 daily windows. Provider-only windows occupy about 74.20 GiB; the 45 native windows with causal-v11 predictions occupy about 51.09 GiB. Cache generation enforces a 60 GiB internal free-space reserve. Cache manifests and all non-cache artifacts remain on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`:

```text
reports/cache_prewarm_provider_20250801_20251231_v13/manifest.json
SHA256 ba606ca6da354dacc97f7c01dbc40afe10e8008a90ffcc7f08f83fa741ca4f5c

reports/cache_prewarm_native_45d_causal_v11_ml_v13/manifest.json
SHA256 41f82a43f8d5c747413a6c7cda877325d293f0f4af2c7ccf69d563bdcf656828
```

Cache keys bind the book source contract, input files, queue mode, feature identity, model identity, and replay semantics. A provider cache cannot be reused as a native cache.

## C++ Replay Results

The source-aware runner executes the C++ core replay and deliberately disables the Python-only BUY q90 lifecycle action and independent BUY fill-selection gate in both arms. It is an incremental core-path comparison, not a full live stack reproduction.

### Provider Sensitivity Baseline

The 67 provider days produced 29,815 fills and aggregate daily-fresh-start PnL of `-374.0297 USDC`. This is a provider-normalized sensitivity baseline only; the queue approximation and daily terminal liquidation prevent it from being used as formal PnL or action evidence.

### Native Causal-v11 ML A/B

| Panel | ML-ON minus ML-OFF PnL | Day-bootstrap 95% CI per day | Fill retention | Inventory-time ratio |
| --- | ---: | ---: | ---: | ---: |
| Validation 22 | +14.3165 USDC | [-0.3335, +1.5619] | 89.50% | 1.1294 |
| Test diagnostic 22 | +14.0967 USDC | [+0.0543, +1.2184] | 86.51% | 0.9427 |

Validation does not establish a positive PnL lower bound and inventory time is worse. The later diagnostic is positive but loses 13.5% of fills and does not repair the missing full-live-stack comparison. Therefore causal-v11 remains a research candidate:

```text
action_authority=false
live_authority=false
deployment_authorized=false
```

## Reproduction Entrypoints

The supported commands are:

```bash
.venv/bin/python pipeline.py freeze-research-days --help
.venv/bin/python pipeline.py prewarm-tick-cache --help
.venv/bin/python pipeline.py source-aware-cpp-baseline --help
```

Every formal invocation must pass an exact day file and one book root. The runner rejects a day list containing more than one source authority.

## Verification

The source-aware universe, source-bound cache, C++ runner, metric preprocessing, tick A/B registry, and receive-time visibility tests pass:

```text
25 passed
```

Excluding the four closed historical F09/F10 producer modules described in the storage-relocation record, the repository suite passes:

```text
1123 passed, 4 skipped
```

Those four modules currently contain 11 expected fail-closed tests because their frozen contracts bind both the retired absolute path and old replay implementation hashes. They were not made green by weakening frozen identity checks.
