# Normalized L2 100ms v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Date: 2026-07-25

Status: data migration contract; no strategy or live-policy change.

## Purpose

`normalized_l2_100ms_v2` is the only default normalized BTCUSDC order-book view for new replay research. It contains 128 hardlinked top-20 states sampled at 100ms. It replaces the mixed-cadence identity of the historical top-level `bbo/` and `l2/` directories without overwriting any source file in place.

This dataset is a bounded policy-feature and descriptive replay view. It is not a copy of the full native book, and it cannot recover a price level outside its top 20. The authoritative source for exact visible price-level queue, cancellation, refill, and deep-order-path evidence remains the native CryptoHFTData snapshot/delta archive:

```text
${NARROWGATE_RETIRED_MARKETDATA_ROOT}/cryptohftdata/binance_futures/
```

The normalized view is:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/normalized_l2_100ms_v2/
├── bbo/
├── l2/
├── daily_quality.csv
└── manifest.json
```

Daily Parquet files are hard links to immutable, previously reconstructed 100ms artifacts. Hard links preserve the source bytes and SHA256 while avoiding another multi-gigabyte physical copy. Tools such as `du` may count the linked logical paths more than once; this does not mean the book bytes were duplicated.

At publication time the host had approximately 137 GiB available. Keep that headroom by retaining one native snapshot/delta archive, one hardlinked normalized view, and summary-only research outputs. Do not materialize another full-L2 or per-arm deep-book copy.

Frozen local identity:

- `manifest.json` SHA256: `f47e044b0607d135de713f8cbf13dad82decc051786dd4a896f21e014129b517`
- `daily_quality.csv` SHA256: `b4fa3d5b4f78d81f48bd6fb67badf53e41ffc2530f2cc1b16fb8925a23569bcc`
- good-day manifest SHA256: `2dc2748f5bb26a3269ff563c22563560bf680445f04b863b174a541a043b8c18`
- strict-day manifest SHA256: `8e820f3d9a59db10432533fa3c7118bc12327aa60822a717df57fb10c957bdab`

## Day Classes

The starting retained-good-day universe contains 128 UTC days through 2026-07-20. Every linked day has a top-20/100ms normalized view, but a file's existence is not evidence that its full queue path is causally reconstructable. Only 62 days have the frozen `formal_eligible=true` identity.

| Class | Days | Meaning |
|---|---:|---|
| Rebuilt 100ms | 128 | A normalized top-20/100ms artifact exists |
| Prior-day warmup valid | 109 | Target day and previous natural UTC day have complete source hours |
| Native sequence valid | 66 | Snapshot initialization and `U/u/pu` continuity pass |
| At least 99% normalized coverage | 117 | Descriptive cadence is usable over at least 99% of the UTC day |
| Formal eligible | 62 | Warmup, sequence, coverage, cadence, schema, and valid-spread gates all pass |

`daily_quality.csv` records at least:

- `rebuilt`
- `warmup_valid`
- `sequence_valid`
- `formal_eligible`
- `source_root`
- `reconstruction_mode`
- BBO/L2 file paths, sizes, and SHA256

The 19 days without a usable previous-natural-day warmup are retained only in the descriptive 128-day panel. They are not repaired by inventing state, and they cannot enter a formal queue, hazard, cancel, or action-uplift denominator.

`formal_eligible` is a target-day identity. A replay that additionally loads the previous normalized UTC day must require that context day to be formal as well. Under the current registry, 53 of the 62 target days form such a closed one-day context set; nine fail that stronger gate. The fixed-spread probe uses day-local variance/book state with `market_context_warmup_days=0`, so its formal normalized panel legitimately contains all 62 target days.

## Research Use

The two denominators must always be reported separately:

1. **Descriptive 128-day panel**
   - useful for large-sample fixed-spread volume and broad curve-shape checks;
   - includes `delta_converged` or otherwise non-formal reconstructions;
   - uses the top-20 state plus the declared calibrated queue fallback when a
     simulated order price is outside the visible depth;
   - cannot support exact native/deep queue, sub-second action, or causal
     keep/cancel claims.

2. **Formal 62-day panel**
   - requires `formal_eligible=true`;
   - is the only normalized panel allowed for formal cadence, sequence,
     spread-fill, queue-calibration, hazard, and cancel/re-enter checks;
   - remains a top-20/100ms view: an exact native/deep queue claim additionally
     requires causal replay of the raw CryptoHFTData snapshot/delta stream at
     the active order price;
   - loaders must fail before reading outcomes when any requested target day is
     outside the frozen formal set.

A report may show both panels, but it may not pool them into one confidence interval or silently use the 128-day result as formal confirmation. It must also identify whether a queue statistic came from `top20_calibrated_fallback` or `native_deep_exact_level`.

## Fixed-Spread Study

The controlled fixed-spread experiment uses the same replay matching engine for every arm:

- BUY quote: `best_bid - distance_ticks * tick_size`
- SELL quote: `best_ask + distance_ticks * tick_size`
- fixed order size and shared deterministic latency path
- current new/cancel/replace/TTL lifecycle
- no ML, inventory skew, guard, dynamic-cap, cooldown, or P3 Bernoulli policy layer

It has two evidence modes that must never be merged:

1. `top20_calibrated_fallback`
   - reads the 128-day hardlinked top-20/100ms view;
   - uses visible top-20 state when available and the frozen calibrated
     queue-ahead fallback outside that range;
   - supports descriptive volume and curve-shape results.
2. `native_deep_exact_level`
   - is a separate planned confirmation, limited to formal-eligible days;
   - replays the native snapshot/delta stream causally and queries the actual
     visible price level used by the active order;
   - supports exact visible-level/deep-queue sensitivity evidence, subject to
     the usual unobservable exchange-priority and hidden-liquidity limitations.

It reports three distinct quantities:

\[
P(\mathrm{touch}\mid d),
\qquad
P(\mathrm{fill}\mid \mathrm{touch},d),
\qquad
P(\mathrm{fill\ before\ lifecycle\ end}\mid d).
\]

The 128-day top-20/fallback panel answers the broad descriptive question. A 62-day formal normalized subset can test sensitivity to stronger data-quality gates, but it remains top-20/fallback. The native/deep result becomes the authoritative exact visible-level queue sensitivity only after a separate raw snapshot/delta replay has actually been run; it has not yet been generated by the current broad runner. Outputs must carry both `panel` and `queue_evidence_mode`; neither curve is a live action policy. Future keep/cancel work must additionally value queue loss, fill quality, and campaign outcome.

Daily and aggregate sufficient statistics are the durable fixed-spread output. Do not persist duplicated full-L2 states or per-order traces for every distance arm unless a small, predeclared diagnostic sample requires them.

## Migration And Removal

BTCUSDC replay and feature defaults now use v2. On 2026-07-25, 250 independent legacy files were removed from the old top-level `l2/`, releasing about 2.39 GiB. Six 100ms hard-link anchors remain because frozen strict symlink views still target them. The residual `l2/` directory is not a research input.

All 62 formal v2 days passed post-removal size and SHA256 verification. The old top-level `bbo/` remains temporarily for BTCUSDT bridge compatibility and superseded P3 evidence lineage. It must not be selected as BTCUSDC replay input.

Historical conclusions affected by the old mixed identity are classified in [Legacy L2 Evidence Revalidation](legacy_l2_evidence_revalidation_20260725.md). The native CryptoHFTData snapshot/delta archive must not be deleted.

To keep the boundary closed, order-book rebuilds now default to `replay_l2_retained100ms_staging`, P3 defaults to the v2 BBO root, formal replay validates every context day it loads, and BBO/L2 environment overrides must be supplied as one pair.
