# Offline Data Tooling

Last materially modified: 2026-09-13
Last materially synchronized: 2026-09-13

[English](README.md) | [简体中文](README.zh-CN.md)

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../docs/public_private_documentation_contract.md).

This package contains offline data acquisition, import, normalization, and quality-registry entrypoints. It contains code, not market-data payloads.

## External-model base features are explicit

The external-venue trainer's 10s path requires `--base-feature-dir` pointing to compatible daily `features_YYYY-MM-DD.parquet` inputs. It no longer assumes the retired unversioned feature directory. Its old 10s cache has no input binding, so this path rebuilds rather than reusing that cache. The fast1s path is unchanged; selecting a folder does not establish feature-schema or research compatibility.

## Queue calibration is explicit

Replay no longer loads a shared daily queue file from the data root or an ambient environment variable. The retired single-day calibration is not a current baseline input. Pass an explicit calibration artifact when that experiment requires one; an explicit missing file is an error, and strict calibration still requires its declared artifact. The calibration builder requires `--output` and does not recreate a shared default directory. This cleanup does not recalibrate or validate a new queue model.

## Shared legacy BBO directory retired

On 2026-09-13 the owner authorized deletion of `${NARROWGATE_DATA_ROOT}/bbo`: 256 derived files, 128 dates per symbol for BTCUSDC and BTCUSDT, spanning 2026-01-01 through 2026-07-20. Do not restore this shared legacy directory merely to satisfy old reference-feature defaults or historical P3 locators. New reference-book features must be reconstructed from the selected BTCUSDT L2 inputs; reference trade bars come from actual individual trades. Purchased archives, canonical raw records and separately registered normalized BBO/L2 directories are not part of this deletion.

The existing optional reference-BBO reader finds no files at the retired path. Its Bar-only fallback is not a rebuilt L2-derived reference-book feature set or evidence of unchanged predictions. This cleanup does not switch that reader to purchased archives, rebuild features, retrain models or rerun economics. Bind the replacement feature inputs before the next training round. See the [historical distortion and retirement register](../docs/legacy_l2_evidence_revalidation_20260725.md#retirement-and-research-impact-2026-09-13) for withdrawn values, conditional historical results and unaffected evidence; deletion alone does not invalidate every past result.

## Current dataset: one continuous calendar

The owner-maintained BTCUSDC perpetual dataset covers 2025-08-01 through 2026-09-05 UTC: 401 dates, all current. There is no separate “old 142-day dataset”. Current catalogs and research interfaces organize it by market, date and channel, with raw records and derived products in separate directories. Supplier names and past processing classes are not current dataset categories. The actual owner market files are private and are not distributed with either source repository.

Use the daily paths below and the current owner manifest, not a dated supplier registry or a historical selected-day list. The inventory retains gaps and actual observation ages. During a gap, do not produce new unsupported strategy quotes; continue time and account-state handling, including existing orders, inventory, fees and funding. Silence does not mean a known absence of fills or a reset to flat. A best-bid/ask observation can improve the top quote but cannot refresh unknown deeper levels or establish queue progress.

Historical experiments describe their original inputs and behavior. They do not establish performance on the current dataset, and data repair alone does not rerun or reverse a historical research result. See the [current E/C scope](../research/families/f05_fill_quality_quote_ev/docs/risk_selection_scope.md) for supported actions and past-only rolling-validation guidance. Dataset organization, observation support and prior research use are separate concerns.

## Infoway: select before spending quota

[`download_infoway.py`](download_infoway.py) previews one crypto-product request without a key or network. Only `--execute` sends it. It supports product `info`, one historical `candles` page, or a single current `depth`/`trade` sample. It never subscribes, paginates, retries, fills gaps or imports a response into canonical replay data. Infoway's [candles contract](https://docs.infoway.io/rest-api/http-endpoints/get-candles) documents a minimum interval of one minute and at most 500 candles for one product; historical reach depends on the subscription. Multi-product historical batching is deliberately absent. [Depth](https://docs.infoway.io/rest-api/http-endpoints/get-depth) and [trade](https://docs.infoway.io/rest-api/http-endpoints/get-trade) endpoints describe current data, not historical archives. The documented trade response does not contain a reliable exchange trade ID.

The owner-supplied product catalog reviewed on 2026-09-09 lists `BTCUSDT` as spot and `BTCUSDT.P` as a crypto contract, but contains no `BTCUSDC` product. This is a catalog observation, not an authenticated enumeration of all current entitlements. The [basic-info contract](https://docs.infoway.io/rest-api/basic-info/get-symbol-basic-info) explicitly leaves `exchange` null for crypto. Do not infer Binance, USD-M perpetual identity, base-volume units, matching-engine timestamps or historical BTCUSDC book support from a code. Generic marketing examples contain conflicting depth layout/side descriptions and label epoch timestamps as UTC+8; samples remain raw, with no depth normalization or automatic eight-hour timestamp shift. Venue, contract, units and timestamp semantics require provider confirmation before any integration.

Recommended selection: spend no quota trying to repair BTCUSDC historical books with these documented endpoints. If an independent reference sample is desired, first inspect `BTCUSDT.P` basic info (one request), then optionally take 120 one-minute candles (one request) to inspect schema and timestamp alignment. This is not a new alpha experiment or a replacement for existing reference data. Ask the provider, without consuming data calls, about the exact BTCUSDC perpetual venue, historical L2/trade availability, historical reach and account billing/remaining quota before a larger plan. The official [HTTP limits](https://docs.infoway.io/getting-started/api-limitation/http) describe plan-dependent request frequency; they do not establish this account's remaining credit balance or billing units.

Offline previews:

```bash
.venv/bin/python -m data.download_infoway info --code BTCUSDT.P
.venv/bin/python -m data.download_infoway candles --code BTCUSDT.P \
  --interval 1m --count 120 --end 2026-09-08T00:00:00Z
```

After the owner selects the sample and budget, set `INFOWAY_API_KEY` locally through a hidden terminal prompt or secret manager, not chat, command arguments, Git or a tracked config. In zsh, `read -rs 'INFOWAY_API_KEY?Infoway API key: '; export INFOWAY_API_KEY` hides the input. The program puts the key only in the `apiKey` header. Keep provider samples separate under `${NARROWGATE_MARKETDATA_ROOT}/infoway/`; create that parent directory first. An example single authorized metadata call is:

```bash
.venv/bin/python -m data.download_infoway info --code BTCUSDT.P \
  --execute --max-requests 1 \
  --budget-file "$NARROWGATE_MARKETDATA_ROOT/infoway/request-budget.jsonl" \
  --output-dir "$NARROWGATE_MARKETDATA_ROOT/infoway/<new-sample-directory>"
```

Reuse the same budget file for every call with this key. `--max-requests` caps cumulative local attempts, including failed/uncertain calls; raising it is an explicit additional spending decision. A locked, durable append occurs before dispatch, and calls sharing the ledger are spaced at least 1.1 seconds apart at reservation time. A corrupt ledger stops, never resets. Do not delete/change ledgers or output directories to retry automatically. The cap does not account for other clients or provider credit weights. Existing output directories are refused before a request; redirect, rate-limit, timeout, authentication or schema failure never triggers an automatic retry. Successful output contains the provider JSON, request description and `SAVED_NOT_ADMITTED` receipt; this means saved and attributed to the requested code, not content validation or economic eligibility. Candle duplicates and missing values are retained, not silently merged or filled. No authenticated Infoway request was made during implementation; tests use synthetic responses.

## Full-calendar readability inventory

An owner-pinned dataset can set `calendar_window_policy: "owner_fixed"` with explicit `start_day` and `end_day` in the private source manifest. The inventory then keeps that continuous window instead of extending it to yesterday; a mismatched start or an incomplete UTC end is rejected. Dates inside the window remain present even when their files are missing. This is an explicit dataset-scope change, not a retrospective improvement to historical results or permission to erase previous use.

`.venv/bin/python -m data.quality.calendar_readability --owner-manifest <private-studio-source-manifest> --dataset <registered-source-id> --start-day 2025-08-01 --usage-config <metadata-only-use-map> --output <new-private-report.json>` inventories every complete UTC day through yesterday; repeat `--dataset` for each raw or prepared channel. Use the project virtual environment. The current UTC day is listed separately as partial. The output is create-only with private permissions. It reads registered file metadata, Parquet footers and recorded quality metadata, not economic results or full raw streams. Missing dates remain in the denominator. Unknown observed/secondary/carried coverage, freshness, deduplication and use rights are not replaced with zero; historical quality and current readability remain separate. A usage map supplies explicit metadata day fields and roles, not permission to inspect sealed outcomes. File readability never grants training or economic admission, and this inventory does not replace bounded reader tests or full-calendar source-clock validation.

### Daily content acceptance

For historical native-parent reconciliation, add `--trade-mapping-only` instead of `--relationships-only`. It reads individual and native aggregate trades without requiring bars, retains structured mismatch counts, and uses adjacent days registered in the same manifest as ID context. Every aggregate belongs to its own UTC file day; borrowed children do not inflate the individual-trade denominator. Keep adjacent dates in the manifest even when auditing a sub-window. The report separates uncovered individuals, parent price/side/quantity mismatches, timestamp-span differences and ID jumps; an ID jump alone does not prove missing captured volume. Retired or unregistered native input is explicitly unavailable; derived 100ms groups never stand in for native parent messages.

For full content acceptance, use `.venv/bin/python -m data.quality.calendar_content --manifest <readability.json> --start-day YYYY-MM-DD --end-day YYYY-MM-DD --output-dir <new-private-directory> --workers 2`. This streams every registered row and column, hashes the files, checks values, timestamps, trade IDs, depth ordering and BBO/L2 equality, and reads identity-bound observation-clock sidecars where available. Each date is saved separately, including missing or corrupt inputs. With `--relationships-only`, a registered current 100ms-derived dataset is recomputed from individuals to check group membership, exact quantity/count conservation and causal bucket clocks; its one-second OHLC/volume/count are checked against ordered individuals. Historical native datasets retain their separate parent-ID mapping and legacy bar checks. Neither mode runs a strategy or changes raw files. `CONTENT_READABLE` means the implemented content checks completed without those findings, not complete capture, fresh quotes, causal features, exact queue reconstruction or economic admission. Timestamp-bucket occupancy is not freshness coverage; unavailable source clocks remain unknown. Feature nulls, long observation gaps, source-clock binding, and previous use must be interpreted separately. Output directories are create-only; retain partial day reports after interruption.

Large raw and derived datasets live outside the repository. The canonical machine-local roots are:

```text
NARROWGATE_MARKETDATA_ROOT=<local-marketdata-root>
NARROWGATE_RAW_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC/raw
NARROWGATE_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC/derived
NARROWGATE_CACHE_ROOT=$HOME/Library/Caches/NarrowGate_BTCUSDC
# Optional: NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_CACHE_ROOT/replay_dag
# Capacity fallback for reusable DAG cache only:
# NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_DATA_ROOT/cache/replay_dag
```

`data_paths.py` owns runtime path resolution; new code must not embed a user home-directory path. Frozen evidence may retain the pre-migration `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` provenance string and is resolved at the filesystem boundary without rewriting the frozen bytes. No compatibility symlink is created at the old location.

`raw/` and `derived/` are real directories, not aliases. Project-maintained data paths must not use symbolic links. Raw execution data has one supplier-neutral daily layout:

```text
NarrowGate_BTCUSDC/
  raw/binance_futures/BTCUSDC/YYYY-MM-DD/
    incremental_book_L2.parquet
    trades.parquet
    funding.parquet
  derived/binance_futures/BTCUSDC/YYYY-MM-DD/
    trade_aggregates_100ms.parquet
  derived/bars_1s/          individual-trade 1s bars
  derived/                 other features, replay inputs, caches and reports
```

BTCUSDT reference days use the same naming for their actual `trades` and `aggTrades` channels; they do not require an execution book. The physical contract is identical columns, Arrow types, units and meanings across all dates **within each channel**; books, trades, bars and features are different channels, not one artificial shared schema. Prices and quantities retain decimal strings, `timestamp` is exchange microseconds, and unavailable receive/native sequence values stay null. Reference 1s bars are rebuilt from individual trades, so `trade_count` always counts individual executions and `last_event_ts_ms` is the actual last execution time in milliseconds. Stored feature indexes and the five-minute metrics `create_time` index use UTC microseconds. Metrics conversion preserves its six double-valued columns, row order and existing readiness convention; sub-microsecond observations are rejected rather than truncated. `data.unify_daily_fields metrics` checks and converts existing files, and `features/preprocess_metrics.py` produces that same precision. Current registration does not classify suppliers or historical processing. A format conversion does not invent observations or exact queues.

### Individual union and derived trade flow

The maintained BTCUSDC trade source is the union of actual Binance Vision and CryptoHFT individual executions, keyed by native trade ID. Matching timestamps do not establish duplicate identity. Shared IDs must agree on exact price, quantity and maker side; an exchange-clock discrepancy is recorded, with Vision owning the shared ID's canonical timestamp and UTC date. Adjacent-day IDs resolve receive-partition boundaries. Non-positive secondary records are retained as source findings, not added as executed volume. An unavailable source hour is unknown, not an empty successful download.

`pipeline.py download-secondary-trades --help` exposes hourly individual-trade acquisition. `pipeline.py aggregate-trades --help` prepares the union and verified daily derived files from its source manifest without replacing canonical inputs. For an ongoing download manifest, wait for every required receive-hour context to reach a known terminal state before preparing a complete day; a file list alone does not establish source completeness.

Self aggregation uses fixed UTC 100ms `[left,right)` buckets grouped by exact price and maker side. It sums quantity and counts individual executions, with synthetic `group_id`, event-time bounds and `feature_ready_ts_ms` at the right edge. First/last event IDs are not contiguous membership ranges. No empty buckets or zero activity are invented for unknown collection intervals. These groups are flow features, not exchange-native `aggTrade` packets, execution ticks, reconstructed queue events or observed network arrivals. Build 1s OHLC/volume/count directly from ordered individual trades so grouping by price cannot reorder the open/close. Bar metadata binds the individual source hash and count unit; legacy bars are not reused simply because a filename exists.

Native BTCUSDC `aggTrades` is retired after union, conservation/readback checks and current-reader/catalog cutover. Do not download it again: the acquisition entrypoint rejects BTCUSDC perpetual native aggregates, including explicit output-directory overrides. Download individual sources and generate the local flow buckets instead. This is not lossless native-message reconstruction; historical `f..l` parent contracts stay input-unavailable, not silently rebound or used to trigger reacquisition. BTCUSDT and spot reference aggregates are outside this retirement. Cached features and frozen model inputs retain their old identity and cannot automatically be relabelled union-derived or live-aligned.

Keep the latest complete routine validation report per market/channel/calendar scope, including the latest required per-day records. Remove superseded, duplicate and obsolete failed/smoke reports after verifying their replacement and updating current links. Retain current data/source identities, reader clock sidecars, previous-use and unique current quality evidence; frozen research and live/account records are not disposable routine checks. Update the existing current summary instead of producing a new cleanup report for each pass.

`.venv/bin/python pipeline.py unify-raw --manifest <current-readability.json> --output-root <project-storage-root> --workers 2` is the existing source-format importer through `data.daily_raw`, with streaming conversion, atomic publication and logical round-trip checks; historical manifests may still include native aggregates. Existing compatible Parquet bytes are retained. Source retirement is explicit (`--retire-source`) and follows verification; an interval funding source is not removed for a partial-day selection. Default Binance futures trade downloads publish these daily Parquet files and remove extracted ingestion CSVs. Book downloads use the fusion path below, not whole-day provider replacement. An explicit alternate output directory remains an ingestion/diagnostic override, not the research default.

### One fused daily orderbook

The current physical contract is `narrowgate.daily_book.v1`, declared by `DAILY_BOOK_SCHEMA` in `data/daily_raw.py`: 24 identical fields on every date. It retains exact decimal prices/quantities, actual exchange clocks, optional native IDs, and anonymous event-stream/reset semantics, without supplier or processing-class columns. `timestamp` is the causal output/event ordering time; `observed_timestamp_us` is the real exchange observation and `original_timestamp_us` preserves the original event time. Receive and native clock fields are not substituted for the exchange clock. An output row never refreshes an older observation. Legacy layouts are accepted only at the import boundary and projected into this single contract.

`data.unify_book_calendar` converts and verifies one date at a time, staging the daily raw file, derived BBO/clock and all affected current indexes before publication. Its durable transaction resumes after interruption without relabelling old model verification as newly validated; affected features remain `REFRESH_REQUIRED` until rebuilt. The physical calendar migration is not complete merely because this code or catalog labels exist: consult the current operation's per-day `PUBLISHED_VERIFIED` entries and final whole-calendar schema check.

Useful independent BBO observations are retained inside the same daily book as `top_only` bid/ask pairs. They may update the BBO clock, never the depth clock or queue progress. The derived clock has explicit BBO observation time, age, kind and usability. Invalid/unknown observations remain marked unusable rather than disappearing and exposing an older apparently valid quote. Missing native facts remain null, not invented zero. Independent BBO files are deleted only after verified retention of useful observations and current-reader publication. No source switch generates fabricated raw deletion/re-addition rows, and timestamps alone are not duplicate identities.

Derived 100ms BBO/L2/clock files reconstruct independent streams and select the newest valid exchange observation. A persisted numeric stream-priority contract resolves equal-clock ties; the compatibility boundary translates the old named preference once. New anonymous stream identifiers do not need supplier spellings in the selection loop. The selected view does not materialize all depth differences on every switch. A shallower stream's absent levels are unknown, not observed zero quantity. The replay reader treats view switches and snapshot resets as queue-lineage rebases, not cancellations; only a real update within the same selected stream can deplete the modeled queue. This does not establish an exchange-native global sequence or exact queue. If no usable new state exists, the last good view only ages. Equal-clock full-book conflicts are not scanned in this mode and are reported as unchecked, not zero.

The footer binds separate initial and end-of-day book continuations. A standalone reader may use only the declared initial state, never the end-of-day state to seed an earlier time. Cross-day inheritance carries book/clock state only, not account inventory, orders or FIFO. Internal snapshot/reset and observation-only events protect reconstruction and queue behavior; these are message semantics, not old/new date classes. Do not strip their existing fields until the replacement event interface preserves their behavior. The owner has closed unavailable-original recovery; it is not a standing research prerequisite or a reason to remove dates from the current calendar.

The default CryptoHFT downloader converts a newly ingested day into the uniform 24-field contract. Its capture-boundary workflow uses actual pre-midnight exchange events from the next receive-day source; missing context stays `boundary_pending`, not complete. The current uniform-format update adapter is not yet complete: repeating an already unified day, finalizing its pending tail on a later invocation, or adding another source is explicitly refused before replacement. The old capture-hour/receipt reuse checks do not prove idempotence for the new footer. Only explicitly compatible anonymous stream slots can seed a newly ingested successor; a single-stream continuation is not silently treated as a three-stream state. The historical Tardis supplement entrypoint also still requires migration to the uniform-output option. Do not promise automatic tail finalization or retire its necessary scratch until these paths are implemented and tested against real uniform-schema fixtures. These acquisition limitations do not prevent `data.unify_book_calendar` from converting the existing calendar. `--legacy-provider-normalize` remains an explicit diagnostic, not the maintained daily path. Acquisition alone does not refresh dependent books/features: publish the verified normalized outputs and rebuild affected products before marking them current.

For the owner-authorized historical migration, verify round-trip output, source inclusion, observation-clock semantics and current-reader/catalog cutover, then delete the superseded provider books. Keep compact input identities and current diagnostics, not redundant source folders or symlinks. This retirement authorization does not include unique unconverted records, unrelated channels, frozen research or account logs. Raw daily containers and generated BBO/L2/features remain under physical `raw/` and `derived/` directories respectively.

Runtime WebSocket state belongs under `live/`; the Binance execution-market snapshot-plus-diff book is implemented in `live/orderbook/binance_usdm.py`. State attached to NarrowGate's own active orders belongs under `execution/`.

The ownership rule is:

```text
data/             offline ETL and data-quality tools
live/orderbook/   live public-book reconstruction
execution/        active-order and queue-path state
$NARROWGATE_RAW_DATA_ROOT/   canonical observed daily records
$NARROWGATE_DATA_ROOT/        normalized/generated NarrowGate datasets
$NARROWGATE_CACHE_ROOT/       default disposable cache tier
$NARROWGATE_DATA_ROOT/cache/  explicit removable tier for reusable DAG cache
```

Reusable source/feature materializations are declared by `models/replay_cache_dag.py`. Native CryptoHFT logical messages are cached per source hour, so target days, D-1 warmups and experiment arms reuse one parser output. Strategy-dependent order, queue, fill, inventory and campaign paths are explicitly non-cacheable across arms.

Do not add live socket state machines or strategy logic to this package.

## Public Synthetic Replay Demo

The redistributable replay demonstration is the one intentional small data fixture outside this package's external-market-data rules. Its hand-authored, non-economic JSONL tape lives under [`examples/replay_demo/`](../examples/replay_demo/) so it cannot be mistaken for canonical provider data or admitted research evidence.

Run the complete synthetic market-to-evidence path without network access, private evidence, or external order submission:

```bash
narrowgate replay-demo \
  --output-dir results/replay_demo \
  --verify-reference
```

The contract binds the public tape by SHA256 and the receipt binds the tape, contract, replay code, accounting contracts, denominators, campaign terminal value, gate status, and output artifacts. The passing status is `passed_demo_mechanics_only`; economic, promotion, live-action, and real-market fidelity claims remain ineligible.

Provider-specific formats are ingestion only. For a customer delivery page, download into a separate incoming directory without changing current research inputs:

```bash
.venv/bin/python pipeline.py download-tardis \
  --delivery-config data/private/<delivery>/config.json --archive-only
```

The private JSON (mode `0600`, never committed) supplies `base_url`, `output_root`, inclusive `start`/`end` UTC dates, and a `contracts` list of `VENUE,DATASET,SYMBOL` strings. Use `incremental_book_L2` and `trades` for L2 and individual trades. Requests always use the stable `.csv.xz` route; the delivered filename and compressed content identify XZ, Zstandard `.zst`, or `.zstd`. Temporary redirect URLs are not saved. HTTP 202 stays pending; transient errors use bounded backoff. Default concurrency is four, with disk reserve, an exclusive run lock, resumable object-identity checks, and complete-frame validation before atomic completion. Compressed archives remain compressed; downloading is not data admission.

`--archive-only` never fuses, publishes or retires canonical inputs, including when resuming. Without it, the legacy Tardis downloader remains a bounded historical acquisition tool: BTCUSDC perpetual L2 automatically enters the fusion path, while other channels remain acquisition archives for explicit import. That path records remote size, ETag, SHA256 and zstd integrity in an atomic manifest. This is a historical acquisition example, not a second research root:

```bash
.venv/bin/python pipeline.py download-tardis \
  --start 2026-01-01 --end 2026-07-30 \
  --contract binance-futures,book_ticker,BTCUSDC \
  --contract binance-futures,incremental_book_L2,BTCUSDC \
  --allow-missing
```

Do not add Tardis to the recurring daily updater. New daily data continues to use the existing Binance Vision, CryptoHFTData, Bitget, Bybit, and OKX download/import commands and their existing source contracts.

Downloading a file does not make its UTC day research-eligible. Tardis L2 is a new provider identity and must pass its own bootstrap, causal timestamp, cross-channel BBO, coverage, and gap gates before it can repair a historical CryptoHFTData bad day.

Audit a completed boundary-aware manifest against a frozen candidate-day list and the source-separated external venue roots:

```bash
.venv/bin/python pipeline.py audit-tardis \
  --manifest "$NARROWGATE_MARKETDATA_ROOT/tardis/manifests/binance_futures_btcusdc_20260101_20260730_download.json" \
  --candidate-days "$NARROWGATE_DATA_ROOT/reports/marketdata_repair_20260727/cryptohft_bad_days_after_repair_20260727.csv" \
  --output-csv "$NARROWGATE_DATA_ROOT/reports/tardis_bad_day_repair_20260730/raw_admission.csv" \
  --output-json "$NARROWGATE_DATA_ROOT/reports/tardis_bad_day_repair_20260730/summary.json"
```

The default five-second boundary envelope validates event-driven daily file handoff only. It is explicitly not the normalized 100ms freshness or maximum contiguous-gap gate.

Build a source-separated Tardis product when the selected day's primary L2 file is readable and matches its manifest identity. An incomplete batch or missing auxiliary bookTicker does not prevent L2 reconstruction; absent or unreadable bookTicker remains `unavailable` and cannot pass cross-channel admission:

```bash
.venv/bin/python pipeline.py normalize-tardis \
  --manifest "$NARROWGATE_MARKETDATA_ROOT/tardis/manifests/<manifest>.json" \
  --day 2025-08-01 \
  --output-root "$NARROWGATE_DATA_ROOT/normalized_tardis_l2_exchange_100ms_v1" \
  --workers 3
```

Normalization defaults to Tardis exchange `timestamp`; `local_timestamp` is retained as provider-receive provenance. Replay adds its modeled visibility and execution delays separately, not on top of provider delay already embedded in event timestamps. `--timestamp-source provider` remains an explicit diagnostic option with a separate output root; the older provider-clock contract is documented in `docs/tardis_normalized_l2_contract_20260731.md`. The legacy single-source CryptoHFT normalizer uses transaction time, with event time when absent; the maintained two-source fusion instead compares actual exchange E as described above and preserves T separately. Neither silently substitutes receive time. Missing exchange time requires real-source repair, not relabelling. Neither a common clock nor complete output rows establish exact queue position or live transport parity.

### Local gaps and consecutive UTC days

The default `--gap-policy missing` preserves sparse, source-updated BBO/L2 rows. The clock sidecar records `last_observation_timestamp_us` on the selected provider/exchange clock and `observation_age_us = output_timestamp_ms * 1000 - last_observation_timestamp_us`. A right-boundary timestamp is an output time, not a new source observation. `source_observed` means a valid book with a source message in that bucket, not proof that every upstream update arrived.

Use `--gap-policy carry_forward` only for a separately located derived view. It emits valid last-known book states with `observation_kind=carried_forward` and `update_coverage=unknown`; source time stays unchanged and age grows until a new observation. It never carries future values backward, fabricates trades/volume or advances a queue. Missing or invalid-book intervals remain in `unobserved_intervals`; no-message buckets cannot distinguish collection loss from genuine silence. Carried rows do not increase measured source freshness coverage. The dataset identity has a `_carried_view_v2` suffix and replay-admission flags stay false; existing strict admission auditors are not readability checks for this view.

Add `--continuous --workers 1` with adjacent UTC days to retain the reconstructed book and observed clocks. Symbol/clock mismatches, nonadjacent days and regressing selected clocks are rejected. A failed day stops the batch and leaves later days explicitly `not_run_days`. This carries data-reconstruction state only, never inventory, orders or strategy state. The Python API additionally accepts cadence-aligned `output_start_us`/`output_end_us` within one day; it reads the required causal prefix to initialize the book and emits only that half-open window. A partial day's continuation cannot initialize the following UTC day.

Clock/gap/continuation modes cannot overwrite each other in one output root. Legacy sidecars lacking the observation schema are rebuilt, and continuous mode does not restore state from a cached daily output. Each day is staged with its audits, then outputs and the quality marker are published before committing continuation. Ordinary publication failures restore prior outputs; a process crash between filesystem renames still requires verification of the quality marker's output hashes. Do not read an unverified mixed generation. Raw archives are never modified.

### Whole-calendar observation and feature refresh

`data.quality.calendar_content --book-continuity-only --book-root <selected-book-root> --workers 1` validates the entire declared calendar. It fully reads retained BBO/L2, checks their common time/top-price axis, compares source clock cells with the canonical adjacent raw inputs, and exercises causal 100ms as-of reads across midnight. A resampled book from an explicitly different retained source needs its original quality/BBO/L2/clock hash binding; unmatched canonical timestamps remain reported, not silently zeroed or treated as reconstruction proof. Use the normal `--manifest`, `--start-day`, `--end-day`, and `--output-dir` arguments; `--resume` reuses only a verified successful prefix. Validator-only upgrades must recheck prefix file hashes and recompute the causal grids, retaining the original execution identity. Interrupted temporary publication files fail closed and may need inspection. It is not an economic replay or a new raw orderbook reconstruction. Keep missing/aged periods in the denominator.

Resampled books require their original hash-bound observation sidecar. A producer that stamps outputs with its last applied exchange message may use that time only with retained producer/file identity and raw-clock verification, never merely because it resembles a grid. The resulting quality/clock/BBO/L2 binding is consumed by Python and native replay. Freshness uses the observation clock; carrying another output row does not reset age or draw another receive latency. Old unbound inputs remain explicitly `UNKNOWN`, not newly verified fresh books.

Capture partitions can put pre-midnight exchange events in the next daily file. The audit may verify those events against the adjacent file's bound identity, but accepts only clocks before the target UTC day's end. It does not import future book state. Full loaded rows (including retained warmup) and rows inside the target UTC day are counted separately.

After individual trades or 1s Bars change, rebuild taker-tempo with adjacent D-1 context, then use `features.feature_engineer --features-only --warmup-days 7` over the same complete calendar. Bind the exact individual-Bar sources and reference-Bar root; do not substitute retired native aggregate data. Features-only does not fit a model or generate labels. Preserve missing warmup/undefined ratios as missing values instead of backfilling from future observations. Check every model bundle's actual column schema and predictions separately from market capture completeness, economic quality, and research-use permission.

Existing dense-second count/volume-zero conventions describe no record in the retained tape, not independently confirmed exchange inactivity. Without a capture-gap mask they cannot distinguish silence from missing messages. A schema/finite-prediction check does not establish equivalence to the historical training distribution or retrain the frozen model weights.

Freeze an immutable source-aware research-day view only after provider, non-CryptoHFT, and native quality ledgers are complete:

```bash
.venv/bin/python pipeline.py freeze-research-days \
  --start 2025-08-01 --end 2026-07-25 \
  --provider-quality-csv "$NARROWGATE_DATA_ROOT/reports/<provider-quality>.csv" \
  --non-cryptohft-csv "$NARROWGATE_DATA_ROOT/reports/<source-audit>.csv" \
  --native-quality-csv "$NARROWGATE_DATA_ROOT/<native-root>/daily_quality.csv" \
  --output-root "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1"
```

The builder requires same-source D-1 natural-day warmup, creates an immutable hard-link view, and never edits the canonical `normalized_l2_100ms_v2` registry. Provider-normalized dates are labelled sensitivity-only and cannot be promoted to native sequence, exact-queue, action, or live authority.

Prewarm reproducible tick windows on the internal cache volume while keeping the manifest on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`:

```bash
.venv/bin/python pipeline.py prewarm-tick-cache \
  --days-file "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1/provider_replay_days.csv" \
  --book-root "$NARROWGATE_DATA_ROOT/normalized_l2_research_union_v1" \
  --cache-dir "$NARROWGATE_CACHE_ROOT/window_cache" \
  --manifest-json "$NARROWGATE_DATA_ROOT/reports/cache_prewarm_provider_v13/manifest.json" \
  --workers 2 \
  --reserve-gib 60
```

Use `--with-ml`, `--feature-dir`, and `--model-dir` only when the model and feature identities are frozen. The command binds those hashes into the cache key. Market data, features, model artifacts, manifests, and reports stay on the external volume; only disposable cache payloads belong under `NARROWGATE_CACHE_ROOT`.

Run source-separated C++ core calculations through:

```bash
.venv/bin/python pipeline.py source-aware-cpp-baseline --help
```

One invocation may contain only one source authority. Provider-normalized results remain sensitivity evidence. This runner also excludes Python-only BUY q90 and independent BUY fill-selection behavior, so it cannot claim full-live-stack or deployment parity.
