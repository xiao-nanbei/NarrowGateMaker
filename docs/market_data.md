# Market Data

Last materially modified: 2026-09-10

> Publication note: `${NARROWGATE_*}` values are portable locators. Owner-side market data, run evidence and deployment identities are not distributed with this repository.

## Current Layout: Two Physical Folders

Market data lives outside the checkout in two real directories: `raw/` for unified daily source containers and `derived/` for reconstructed books, bars, features, quality manifests and results. Do not create project symlinks, provider aliases or a second raw copy for individual studies. Provider names belong in ingestion adapters and provenance, not research paths.

```bash
export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_RAW_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/raw"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/derived"
export NARROWGATE_CACHE_ROOT="${NARROWGATE_CACHE_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/NarrowGate_BTCUSDC}"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/backtest_results_btcusdc"
export MM_DATA_ROOT="$NARROWGATE_DATA_ROOT"
```

```text
${NARROWGATE_MARKETDATA_ROOT}/NarrowGate_BTCUSDC/
├── raw/
│   ├── binance_futures/
│   │   ├── BTCUSDC/YYYY-MM-DD/
│   │   │   ├── incremental_book_L2.parquet
│   │   │   ├── trades.parquet
│   │   │   ├── aggTrades.parquet
│   │   │   └── funding.parquet
│   │   └── BTCUSDT/YYYY-MM-DD/
│   │       └── trades.parquet       # separate reference market; not BTCUSDC execution
│   ├── binance_spot/SYMBOL/YYYY-MM-DD/
│   │   └── aggTrades.parquet        # separate spot reference; original clock unit retained
│   ├── history/binance_futures/SYMBOL/YYYY-MM-DD/
│   │   └── metrics.parquet          # original metrics observations, not execution book data
│   └── .incoming/                  # temporary acquisition inputs, not research sources
└── derived/
    ├── normalized_l2_100ms_v2/      # versioned derived views; inspect their actual manifests
    ├── bars_1s/
    ├── reference_bars_1s_trades_v1/
    ├── trade_features/
    ├── features_btcusdc/
    ├── external_venues/
    ├── reports/
    └── backtest_results_btcusdc/

${NARROWGATE_CACHE_ROOT}/
├── window_cache/
└── replay_dag/
```

The four BTCUSDC filenames are stable for every complete day. A missing source is explicitly missing; it is not replaced by an empty/zero file merely to satisfy the naming convention. Reference markets retain their own symbol and need only their declared channels. `data_paths.daily_market_path(day, symbol, channel)` resolves canonical inputs; an explicit `NARROWGATE_RAW_DATA_ROOT` is independent of the derived `NARROWGATE_DATA_ROOT`.

### Daily Format and Clocks

All canonical channels are Parquet. Their UTC date is in the parent directory. The book uses the Tardis-compatible eight core fields plus nullable native extensions: exchange/symbol, exchange `timestamp` in microseconds, separate `local_timestamp`, snapshot flag, side, decimal price/amount, and original native sequence/message/order fields when present. Conversion preserves row order, decimal strings and native identifiers. Nullable sequence fields remain unknown; a common schema does not invent native sequence or exact queue evidence.

Individual `trades` preserve each published matching-event ID, quantity and buyer-maker side. `aggTrades` preserve aggregate ID and first/last individual IDs; they are events, not fixed one-second bars. Funding keeps the actual settlement timestamp, signed rate and observed mark when available. Same-timestamp trades are not duplicates solely because they share a timestamp.

Exchange time is the alignment axis. Local receive time remains separate provenance and never substitutes for a missing exchange time. Replay applies its frozen visibility/latency model separately. Derived carry-forward advances output time, not the last real observation; unknown volume/count, source gaps and stale age remain explicit. No future snapshot may fill an earlier bucket.

### Ingestion and Retirement

The default Binance Vision downloader converts verified staging CSV into the corresponding daily Parquet, then removes its temporary source copy. Perpetual trades/aggTrades use the main symbol/day tree; spot and historical metrics use the separate paths above. Auxiliary conversion preserves source timestamp units and all original columns; preprocessing performs the existing causal clock normalization. The order-book updater collects complete hourly input in ingestion staging, exports a verified lossless daily container, publishes the canonical day, and only then retires verified hourly/staging duplicates. Repeated invocation reuses the canonical file. Explicit legacy output overrides remain ingestion/debug interfaces, not the default research layout. Bar preprocessing's legacy CSV cleanup flag does not remove unified raw Parquet.

An optional historical archive downloader stages under `${NARROWGATE_RAW_DATA_ROOT}/.incoming/tardis`; that provider hierarchy is not a second research root. Existing historical manifests retain their original provenance strings. They must not be made reachable with symlink aliases or relabelled as new canonical observations.

Source-container equality and reader checks do not prove a complete day or grant economic use. Freeze a continuous calendar in advance, keep every day in its denominator, and inspect observed/carry/unknown coverage plus previous-use independently. Old retained-day counts below describe historical experiments, not the current calendar or an instruction to select isolated good days.

Physical volumes and deployment profiles are owner-local. See [Path Conventions](path_conventions.md) and the [data guide](../data/README.md). Caches are disposable; canonical raw data and frozen evidence are not automatically deletable caches. The portable cache default follows the bilingual README [Data Layout](../README.md#data-layout).

## Source Provenance

| Source | Role | Important limitation |
| --- | --- | --- |
| Binance Vision | Official Binance public daily trades/aggTrades/spot/metrics | `bookDepth` is coarse percentage-depth aggregation, not exact L2 |
| CryptoHFTData | Third-party acquisition adapter for Binance BTCUSDC native snapshot/delta data; verified output uses the common daily raw path | It is not an official archive; days, hours or updates may be absent. Native IDs are preserved when actually supplied. BTCUSDT reference bars do not require this book source |
| Tardis delivery | Historical Binance Futures BTCUSDC book input, converted into the same daily schema | Original normalized events lack Binance `U/u/pu`; conversion must keep those fields null rather than invent native sequence. Auxiliary ticker coverage is separate, and L2 read access does not prove cross-channel admission |
| Bitget data download/public API | Independent BTCUSDT perpetual reference | Historical archive dates may use UTC+8 naming; every row must be re-cut into UTC days |
| Bybit public contract-trade archive | Independent BTCUSDT linear perpetual reference | Historical trades are not historical order book or queue data |
| OKX history download/public REST | Independent `BTC-USDT-SWAP` and `BTC-USDT` spot references | Download-page ZIPs use UTC+8 days; swap size is contracts and must use `ctVal`; spot and perpetual are separate factors, not two independent votes |
| Binance `USDCUSDT` spot | Local stablecoin conversion anchor | Symbol is USDC base / USDT quote, so `BTCUSDT / USDCUSDT` converts the bridge to BTCUSDC; it is not an external venue vote |
| Live receive-time capture | Executable latency/freshness evidence | Coverage begins only when the recorder is running and cannot repair old history |

Files do not become complete because they exist. Inspect actual channels, timestamps, sequence capability, gaps, source age and usable horizons. Re-downloading may repair transport failure but cannot manufacture events a source never captured. A day with gaps remains in the current continuous calendar; its limitations and research-use status remain explicit.

### Historical Acquisition and Retained Registries

The counts and named roots in this historical subsection describe the 2026-07/08 deliveries and then-frozen studies. They are not a current inventory, universal admission rule or current default reader path. Historical 128/62, 141/76 and 46-day cohorts must not replace the continuous calendar for new research.

The 2026-07-30 Tardis raw admission downloaded and fully validated 420 files (55,996,845,705 compressed bytes) for 2026-01-01 through 2026-07-29. All 67 historical CryptoHFTData bad-day candidates now have the two Tardis raw book sources and all seven required trade sources. This is `raw_repair_ready`, not formal-good-day promotion. The subsequent source-separated reconstruction has now completed 153 technical 2025 targets and 62 frozen 2026 dual-source overlap days; only 93 and 18, respectively, pass the full provider gate. Tardis still cannot establish native sequence or exact-queue identity. See [Tardis Bad-Day Raw Repair](tardis_bad_day_repair_20260730.md) and the [2025 backfill and dual-source audit](marketdata_backfill_20250801_20251231.md). This acquisition is closed as a one-time historical delivery. Subsequent incremental UTC days continue through the pre-existing Binance Vision, CryptoHFTData, Bitget, Bybit, and OKX pipelines; Tardis is not scheduled as a daily source.

`trades` and `aggTrades` are not interchangeable replay clocks. Binance individual trades preserve each published matching event; `aggTrades` may merge multiple fills at the same taker order/price. Neither stream contains passive order-book cancellation or refill events. Use individual trades for queue consumption and aggressive-flow timing, and use CryptoHFTData price-level L2 deltas for depletion, cancellation, refill, and book-state timing.

## Current Download Entry Points

Download official Binance trade data through the pipeline:

```bash
python pipeline.py download-agg-trades \
  --symbol BTCUSDC --start YYYY-MM-DD --end YYYY-MM-DD

python pipeline.py download-raw-trades \
  --symbols BTCUSDC --start YYYY-MM-DD --end YYYY-MM-DD
```

Use `aggTrades` for legacy baseline parity. Use `raw-trades` for an event-resolution study that explicitly declares `execution_trade_source=trades`; do not switch the source silently inside an existing experiment identity.

CryptoHFTData requires its own credentials and is optional for public users:

```bash
export CRYPTOHFTDATA_API_KEY="..."
python pipeline.py download-orderbook \
  --symbols BTCUSDC \
  --start YYYY-MM-DD --end YYYY-MM-DD
```

The updater's hourly inputs are temporary ingestion files. Complete verified output goes to `${NARROWGATE_RAW_DATA_ROOT}/binance_futures/BTCUSDC/YYYY-MM-DD/incremental_book_L2.parquet`; research does not select a supplier directory. Raw price-level snapshots/deltas are not fixed-interval matrices. Normalization produces bounded top-20/100ms views while native fields, when present, remain available to the native decoder. Absence of native IDs cannot be cured by converting file formats. The strict snapshot/sequence decoder and executable freshness checks are unchanged. Historical BTCUSDT bridge bars remain a distinct reference, not execution book substitutes.

## Historical Retained Event-L2 Workflows (2026-07/08)

Everything in this section through "Good-day identities and coverage" documents older frozen roots and commands. Counts, provider paths and thresholds are historical, not current defaults. Do not restore their aliases, pick their isolated dates for new experiments, or reset account state at a data gap. Use the common daily reader and current continuous-calendar manifests for new work.

The historical top-level `bbo/` and `l2/` containers were `legacy_mixed_v1`: they mixed approximately one-second and newer 100ms states. The independent legacy `l2/*.parquet` files were removed on 2026-07-25 after formal-v2 hash verification. Six hard-link compatibility anchors remain for frozen strict views, so the directory is intentionally incomplete and must never be globbed as a research input. The old `bbo/` remains only for frozen historical lineage and compatibility; active hierarchical-reference builds no longer read `BTCUSDT-bbo-*`. It is not a valid default for new BTCUSDC research.

See `docs/legacy_l2_evidence_revalidation_20260725.md` for the evidence classes whose exact values were withdrawn and the required rerun order.

The canonical normalized view is:

```text
${NARROWGATE_DATA_ROOT}/normalized_l2_100ms_v2/
```

It is an immutable registry of 128 hardlinked top-20/100ms artifacts assembled from versioned reconstruction roots. The source bytes are not duplicated. `daily_quality.csv` freezes the reconstruction and quality identity, and only its 62 `formal_eligible=true` dates may enter formal normalized replay.

Two reconstruction identities are supported:

- `snapshot`: formal strict mode. Output begins only after a complete native source snapshot and stops after any `U/u/pu` or source-hour gap.
- `delta-converged`: research mode for retained days whose raw delta tape is complete but whose first native snapshot predates the retained interval. It anchors the first update at its real `pu`, starts from an empty book, enforces all subsequent update IDs, requires a complete top-N on both sides, and emits only after an explicit convergence burn-in. It is valid for top-of-book and top-20 path features after validation; it is not exact deep-queue truth.

Build retained top-20 states at 100 ms without downloading omitted bad days:

```bash
python data/download_cryptohft_orderbook.py \
  --symbols BTCUSDC \
  --retained-manifest <retained-manifest.csv> \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --timestamp-source transaction \
  --levels 20 --snapshot-ms 100 \
  --sequence-bootstrap delta-converged \
  --delta-convergence-ms 120000 \
  --warmup-hours 0 --reuse-raw-only --force-rebuild \
  --raw-root "$NARROWGATE_MARKETDATA_ROOT/cryptohftdata" \
  --target-root "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging" \
  --process-workers 8 \
  --sequence-audit-json \
    "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging/sequence_audit.json"
```

This is a staging reconstruction root, not a second canonical dataset. Publish validated files into `normalized_l2_100ms_v2` as hard links through the registry workflow; do not copy the whole staging tree.

Repair only rows classified as recoverable by a bad-day audit CSV through the same canonical downloader. Inspect the plan first:

```bash
python pipeline.py download-orderbook \
  --repair-audit-csv <cryptohft-bad-days.csv> \
  --repair-causes missing_raw_hours normalized_gap_with_snapshots \
  --dry-run
```

Remove `--dry-run` to execute it. `missing_raw_hours` rows fetch missing source objects; normalized-gap rows reuse cached raw data and force an independent-day rebuild with the configured warmup. A classified `refresh_raw_and_rebuild` action is honored automatically. Use `--repair-refresh-raw listed` or `day` to override other rows explicitly. Refreshes download and decode-check one hourly file at a time before atomic replacement, so a failed fetch preserves the existing cache and temporary disk use is bounded to one hourly source at a time. Rows marked non-fixable remain excluded unless `--include-nonfixable` is supplied; any failed post-repair eligibility audit makes the command exit non-zero.

Audit only the retained UTC dates at 500 ms freshness:

```bash
python data/download_cryptohft_orderbook.py \
  --symbols BTCUSDC \
  --retained-manifest <retained-manifest.csv> \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --raw-root "$NARROWGATE_MARKETDATA_ROOT/cryptohftdata" \
  --target-root "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging" \
  --coverage-freshness-s 0.5 --min-day-coverage 0.99 \
  --sequence-audit-json \
    "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging/sequence_audit.json" \
  --eligible-manifest \
    "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging/eligible_days.csv" \
  --audit-only \
  --audit-csv "$NARROWGATE_DATA_ROOT/replay_l2_retained100ms_staging/coverage.csv"
```

Run a study with the merged causal clock and individual trades:

```bash
python -m research.families.f07_active_order_continuation.audit.local_order_value_replay \
  --execution-trade-source trades \
  --bbo-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/bbo" \
  --l2-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/l2" \
  ...
```

The generic tick runner exposes the same dataset identity explicitly:

```bash
python -m models.backtest_tick \
  --day YYYY-MM-DD \
  --execution-trade-source trades \
  --replay-event-clock merged \
  --bbo-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/bbo" \
  --l2-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/l2"
```

Both directory arguments are required together. Omitting them intentionally uses the runner's declared default identity; a formal experiment must still record and verify the v2 manifest and daily file hashes.

The compatibility registry `normalized_l2_100ms_v2` contains 128 top-20/100ms dates through 2026-07-20, of which 62 are `formal_eligible=true`. The latest immutable candidate `normalized_l2_100ms_v2_minimal141_20260727` contains 141 raw-complete dates through 2026-07-25 and 76 historical-formal dates. New experiments must record which root and day ledger they use; neither registry's descriptive row count is a valid formal denominator. The normalized wide state remains a 100ms artifact; 10-50ms studies and exact deep queue work must consume the raw native event deltas.

For active-order queue studies, consume those native messages directly rather than building another wide deep book or a strategy-specific watch tape:

```bash
python -m research.families.f07_active_order_continuation.audit.local_order_value_replay \
  --execution-trade-source trades \
  --bbo-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/bbo" \
  --l2-dir "$NARROWGATE_DATA_ROOT/normalized_l2_100ms_v2/l2" \
  --exchange-book-raw-root "$NARROWGATE_MARKETDATA_ROOT/cryptohftdata" \
  --exchange-book-mode strict \
  --exchange-book-warmup-hours 24 \
  ...
```

The delayed top-20 stream remains the policy-feature input. The native stream is the authoritative exact visible-level source and uses exchange transaction time only for queue/fill mechanics. Orders query state strictly before activation; gaps, out-of-range prices, snapshot resets, and same-millisecond ambiguity are never converted into a fitted queue.

The merged replay consumes individual trades, BBO/L2 state changes, and timer events in timestamp order. A zero-quantity BBO/L2/timer event advances state and lifecycle clocks but cannot consume queue or create a fill. Cache identity includes the execution-trade source and BBO/L2 artifact paths, so event-L2 runs cannot silently reuse a legacy one-second window.

These normalized files use Binance transaction time `T`. For executable action evidence, do not let the strategy observe them at `T`: queue matching remains on exchange time, while feature visibility must be delayed by the frozen live host's receive/feature profile. An exchange-time run is an ideal-latency diagnostic only.

### Storage boundary

Retain data in this order:

1. immutable raw source data;
2. shared canonical intermediates reused by multiple studies, such as admitted normalized market data;
3. frozen final research outputs, manifests, locks, and the minimum diagnostics needed to audit them;
4. reproducible cache and staging data only when capacity remains.

Do not create per-arm full-L2 copies or duplicate deep-book datasets. Multi-arm studies should reuse shared inputs and write summary-only sufficient statistics; full per-order traces are retained only for small, predeclared diagnostics. Concrete volume limits and cleanup schedules remain owner-local configuration.

For fixed-spread research, every output must distinguish `top20_calibrated_fallback` descriptive evidence from `native_deep_exact_level` evidence. A formal-eligible top-20 file alone does not make a native/deep result.

### Good-day identities and coverage

Do not define a universal good day by intersecting every source ever stored. Freeze the denominator required by the estimand:

- `reference_good_day`: all declared official Binance and external-reference inputs are causally available; BTCUSDT CryptoHFT order book is not required;
- `btc_usdc_native_strict_good_day`: BTCUSDC target and D-1 warmup source are complete, snapshot-seeded, sequence-valid, and pass normalized coverage;
- `segment_eligible`: a diagnostic-only interval between explicit source gaps; queue, order, label, inventory, and campaign state reset or censor at gaps.

The whole-day formal coverage floor remains 99%. Lowering it to 95% would allow up to 72 minutes of missing state per UTC day. In the 2026-07-27 minimal-source reaudit it adds only seven dates, and those dates contain about 14.4-31.5 missing minutes with single gaps of roughly 10.1-21.1 minutes. Such gaps break queue and campaign continuity; they are not ordinary sampling noise. A 95% threshold may be used only for explicitly segmented diagnostics and must never be relabelled as whole-day formal evidence.

Coverage is necessary but not sufficient. The minimal-source registry contains 141 raw-complete dates, 84 native-sequence dates and 76 rows labelled formal. Of those formal rows, 29 have a maximum internal timestamp gap above 10 seconds, 17 above 60 seconds, and seven above 300 seconds; the existing p99-gap statistic hides rare long outages. New continuous order/campaign evidence must therefore either add a maximum-contiguous-gap gate or reset and censor lifecycle state at every invalid interval. The historical `formal_eligible` bit alone does not authorize a campaign to cross a source gap.

The stricter 2026-07-27 day ledger assigns 46 dates Grade A under a five-second maximum-gap budget. See [`minimal_marketdata_good_day_reaudit_20260727.md`](minimal_marketdata_good_day_reaudit_20260727.md) and `${NARROWGATE_DATA_ROOT}/reports/minimal_good_day_reaudit_20260727/`.

## Independent Venues

Bitget archives first go through the UTC-aware importer:

```bash
python pipeline.py import-bitget \
  --manifest <retained-manifest.csv> \
  --archive-dir <bitget-download-directory> \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bitget/perp/BTCUSDT/trades"
```

The same importer can query Bitget's public download catalog and normalize spot transaction-history ZIPs without API credentials. Archive dates use UTC+8; the importer combines source days `D` and `D+1`, then re-cuts rows by event timestamp into one UTC retained day.

```bash
python pipeline.py import-bitget \
  --manifest <retained-manifest.csv> \
  --archive-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bitget/spot/BTCUSDT/archive_source_utc8" \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bitget/spot/BTCUSDT/trades" \
  --instrument-type spot --product-type SPOT \
  --download-missing --download-workers 2 --cleanup-archives
```

Bybit official contract trades can be downloaded directly:

```bash
python pipeline.py download-bybit \
  --manifest <retained-manifest.csv> \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bybit/perp/BTCUSDT/trades" \
  --workers 3
```

Bybit spot uses a different archive path and CSV schema; select it explicitly:

```bash
python pipeline.py download-bybit \
  --manifest <retained-manifest.csv> \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bybit/spot/BTCUSDT/trades" \
  --instrument-type spot --workers 4
```

OKX download-page ZIPs are also UTC+8 daily files. The importer requires D and D+1, clips rows to UTC D, converts swap contracts with `ctVal=0.01 BTC`, and deletes source ZIPs only after every selected day succeeds:

```bash
python pipeline.py import-okx \
  --manifest <retained-manifest.csv> \
  --archive-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/okx/perp/BTCUSDT/archive_source_utc8" \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/okx/perp/BTCUSDT/trades" \
  --instrument-type perp --contract-multiplier 0.01 \
  --workers 3 --cleanup-source

python pipeline.py import-okx \
  --manifest <retained-manifest.csv> \
  --archive-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/okx/spot/BTCUSDT/archive_source_utc8" \
  --out-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/okx/spot/BTCUSDT/trades" \
  --instrument-type spot --workers 3 --cleanup-source
```

Download only retained UTC days for the Binance stablecoin anchor, then remove raw CSVs after successful one-second aggregation:

```bash
python pipeline.py download-agg-trades \
  --symbol USDCUSDT --market-type spot \
  --retained-manifest <retained-manifest.csv> \
  --output-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/binance/spot/USDCUSDT/raw" \
  --workers 4

python pipeline.py bars \
  --symbol USDCUSDT --market-type spot --data-type aggTrades \
  --input-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/binance/spot/USDCUSDT/raw" \
  --output-dir "$NARROWGATE_DATA_ROOT/external_venues/binance/spot/USDCUSDT/bars_1s" \
  --workers 4 --cleanup-input
```

Build the same causal 1s schema for each venue. Events in `[t,t+1s)` are first visible at `t+1s`.

```bash
python pipeline.py external-features \
  --venue bitget --instrument-type perp --manifest <retained-manifest.csv> \
  --trades-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bitget/perp/BTCUSDT/trades" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/bitget/perp/BTCUSDT/features_1s"

python pipeline.py external-features \
  --venue bybit --instrument-type perp --manifest <retained-manifest.csv> \
  --trades-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/bybit/perp/BTCUSDT/trades" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/bybit/perp/BTCUSDT/features_1s"

python pipeline.py external-features \
  --venue okx --instrument-type perp \
  --manifest "$NARROWGATE_RAW_DATA_ROOT/.provenance/external_venues/okx/perp/BTCUSDT/manifests/okx_BTCUSDT_retained_available.csv" \
  --trades-dir "$NARROWGATE_RAW_DATA_ROOT/external_venues/okx/perp/BTCUSDT/trades" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/okx/perp/BTCUSDT/features_1s"
```

Repeat those feature commands with `--instrument-type spot` and the matching `spot/BTCUSDT` directories. Build independent consensuses for each instrument, then a causal cross-instrument state:

```bash
python pipeline.py external-consensus \
  --manifest <retained-manifest.csv> --instrument-type spot \
  --venue-dir bitget "$NARROWGATE_DATA_ROOT/external_venues/bitget/spot/BTCUSDT/features_1s" \
  --venue-dir bybit "$NARROWGATE_DATA_ROOT/external_venues/bybit/spot/BTCUSDT/features_1s" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/spot/BTCUSDT/features_1s" \
  --min-venues 2 --max-source-age-s 2 --workers 4

python pipeline.py external-consensus \
  --manifest <retained-manifest.csv> \
  --perp-consensus-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/perp/BTCUSDT/features_1s" \
  --spot-consensus-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/spot/BTCUSDT/features_1s" \
  --cross-out-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/spot_perp/BTCUSDT/features_1s" \
  --max-source-age-s 2 --workers 4
```

The cross-instrument table contains `perp_minus_spot_bps`, `spot_perp_agreement`, `venue_divergence_bps`, and quote-time-only states for `spot_leading`, `perp_only`, `confirmed`, `divergent`, and `neutral`. These are evidence labels, not live quote switches.

Build a consensus only when both independent venues are fresh:

```bash
python pipeline.py external-consensus \
  --manifest <retained-manifest.csv> \
  --venue-dir bitget "$NARROWGATE_DATA_ROOT/external_venues/bitget/perp/BTCUSDT/features_1s" \
  --venue-dir bybit "$NARROWGATE_DATA_ROOT/external_venues/bybit/perp/BTCUSDT/features_1s" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/perp/BTCUSDT/features_1s" \
  --min-venues 2 --max-source-age-s 2 --workers 4
```

For the historical seven-day OKX incremental panel, all three perpetual venues were required to be fresh, with output separate from retained111 dual consensus:

```bash
python pipeline.py external-consensus \
  --manifest "$NARROWGATE_RAW_DATA_ROOT/.provenance/external_venues/okx/perp/BTCUSDT/manifests/okx_BTCUSDT_retained_available.csv" \
  --venue-dir bitget "$NARROWGATE_DATA_ROOT/external_venues/bitget/perp/BTCUSDT/features_1s" \
  --venue-dir bybit "$NARROWGATE_DATA_ROOT/external_venues/bybit/perp/BTCUSDT/features_1s" \
  --venue-dir okx "$NARROWGATE_DATA_ROOT/external_venues/okx/perp/BTCUSDT/features_1s" \
  --out-dir "$NARROWGATE_DATA_ROOT/external_venues/consensus/perp_bitget_bybit_okx/BTCUSDT/features_1s" \
  --min-venues 3 --max-source-age-s 2 --workers 4
```

Historical trade-time consensus supports second-scale shadow sorting. It does not prove that a 50/100ms cancel is executable; that requires synchronized live receive-time tapes and gateway latency.

The live receive-time path is separate from those causal 1s artifacts. Public Bitget, Bybit, and OKX spot/perpetual WebSockets plus Binance local callbacks write lossless `market_tape.v1` rows as `.jsonl.gz`. Events preserve exchange, local-receive, and feature-ready timestamps; external sources need no API key. Top-of-book events support L1 flow/depletion/refill proxies only, not exact-L2 queue or cancel attribution.

```bash
python scripts/configure_public_reference_ws.py --config live/config.yaml --dry-run
python scripts/preflight_external_venues.py --config live/config.yaml --duration-s 15

python research/families/f05_fill_quality_quote_ev/audit/fill_toxicity.py \
  --input 'logs/market_tape/*.jsonl.gz' \
  --input 'logs/external_venues/*.jsonl.gz' \
  --fills logs/trades.csv \
  --output-prefix logs/audit/global_flow_fill_toxicity
```

For exchange-time replay, first freeze an environment-specific market-data latency profile in the private evidence store. The profile records provider, region, instance class, OS, compute profile, transport, and an exact receive-time window; it is not a universal "exchange latency" constant and its raw bytes are not public.

```bash
python research/system_engineering/audit/market_data_latency.py \
  --input logs/market_tape \
  --input logs/external_venues \
  --output-json "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/latency/<profile-id>.json" \
  --output-md "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/latency/<profile-id>.md" \
  --profile-id <environment-profile-id> \
  --window-seconds 3600 \
  --transport websocket \
  --environment cloud=<provider> \
  --environment region=<region> \
  --environment location=<location>
```

Frozen host-bound profiles remain valid only for their exact private historical environment and may be selected as explicitly named transport sensitivities. Matching CPU/RAM size does not permit relabelling a predecessor profile as a current measurement.

Replay modes are deliberately separate:

```text
captured          recorded feature-ready time; no extra delay
exchange_zero     idealized exchange-time visibility
profile_p50       host-profile median visibility delay
profile_empirical deterministic seeded inverse-CDF sampling
profile_p99       host-profile tail stress
```

The measured transport lag includes exchange-clock offset, public-network delivery, WebSocket handling, and callback scheduling. Binance spot `bookTicker` does not carry an exchange event timestamp and therefore cannot calibrate millisecond transport latency; it remains a slow anchor only.

The fill audit enforces `feature_ready_ts_ns <= fill_ts` and reports a missing sub-second label when Binance BBO cadence/age cannot support the requested horizon. It never fills a 10ms label from a 100ms snapshot.

## Continuous-Calendar Rule

1. Declare a continuous UTC interval before examining results; ongoing UTC days are partial, not complete days.
2. Keep all declared dates, including unavailable or stale segments, in the denominator. Restore actual same-market sources where possible.
3. Align actual source exchange timestamps. Carry only past valid state, preserving last observation and increasing age; unknown trade volume/count is not zero.
4. Check the same reader across UTC boundaries. Data-layer book continuity does not by itself prove account, order, FIFO or campaign continuity.
5. Record channel coverage, longest gap/age, unknowns, causal constraints and use permissions separately. Data repair does not erase Development/Validation/holdout previous-use.
6. B0 and candidates share the same fixed calendar, source/repair rules, clocks, latency, fees/funding and initialization. Each arm maintains its own full continuous execution state.

### Historical Retained-Day Rules and Receipts

The following rules, intersections, reset/censor semantics and counts belong to old frozen experiments. They do not authorize dropping days, forbidding age-labelled causal carry, or resetting positions at gaps in new continuous research.

1. Download or import one UTC day.
2. Normalize it without crossing the UTC boundary.
3. Run source-specific completeness and gap audits.
4. Intersect execution, reference, label-horizon, and feature availability.
5. Freeze one versioned manifest for each declared evidence scope and never substitute a broader manifest silently.
6. Never replace a missing day with a calendar-month file or bridge a gap with forward-filled prices.

Keep separate manifests for separate evidence scopes:

- **descriptive local replay days** require the Binance execution/reference chain, the declared normalized top-20 identity, enhanced features, and replay-loader preflight;
- **formal local replay days** additionally require `normalized_l2_100ms_v2/daily_quality.csv:formal_eligible=true`; an exact native/deep queue claim also requires the raw snapshot/delta stream;
- **historical external-reference days** additionally require every declared Bitget/Bybit/OKX spot/perpetual input and its causal feature build;
- **receive-time days** require same-host captured tapes, gzip/hash integrity, feature-ready timestamps, and the declared chronological/LOO/late row denominators.

A date passing the first scope must not be appended automatically to the other two. The 2026-07-12 local-replay extension is stored under `${NARROWGATE_DATA_ROOT}/reports/good_day_extension_20260712/`; it preserves the older retained111 manifest as an immutable experiment input.

The 2026-07-20 alignment audit supersedes the old extension manifest for new local replay work. It found 126 candidate dates through 2026-07-18. The initial 2026-07-13 build had only one hour of warmup and missed a native snapshot from 2026-07-12 22:26 UTC. Strict cross-day U/u/pu replay with the new 24-hour default warmup restores 98.54% fresh coverage, so the corrected 126-day market-data manifest is:

```text
${NARROWGATE_DATA_ROOT}/reports/good_day_alignment_20260720/
  minimal_complete_local_replay_marketdata_good_days_2026_through_2026-07-18.csv
```

This manifest governs local market-data availability only. Frozen feature, external-venue, strict event-L2, and receive-time studies continue to use their own narrower manifests. In particular, the third-party source has no events from 2026-07-13 06:36:54 through 06:58:00 UTC. That day passes the minimal >=90% gate but is not gap-free event-L2 evidence.

Binance daily futures metrics use two historical timestamp conventions. `preprocess-metrics` treats interval-start rows as available only after the five-minute interval closes and leaves interval-end rows unchanged. Formal feature generation must use the normalized `metrics_5m/` outputs, not join `raw_metrics/` directly.

For public demos, no market data is required:

```bash
narrowgate quote-demo
python examples/order_level_score_demo.py
```
