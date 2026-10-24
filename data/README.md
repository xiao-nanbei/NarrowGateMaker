# Data

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-24
Last materially synchronized: 2026-10-24

The current historical market-data source is **Tardis only**, for both execution and reference-market books/trades. Additional **funding rates and metrics come from Binance Vision when needed**, as separate auxiliary inputs, never as replacement market data. One public `data` layer owns acquisition, facts, observations and input contracts. Delivery addresses and account configuration stay private. Licensed market records are not distributed with the source repository. This current policy does not relabel historical mixed-source manifests or results as Tardis.

## Code organization

Current acquisition entry points are [Tardis purchased archives](downloaders/tardis_archive.py) and [Binance Vision auxiliary data](downloaders/binance_vision.py). `data download` downloads the configured Tardis purchase only; it never falls back to another provider. Old OKX/Bybit/Bitget/Infoway/CryptoHFT acquisition CLIs and external-reference import/build CLIs are retired. Their importable format/quality helpers remain only where existing parsers or historical evidence depend on them; their presence is not current source support. Parsing/facts, observation scheduling and quality checks stay outside the downloader package.

BTCUSDT reference data purchased from Tardis remains supported; “reference” does not mean obsolete. Owner-retired external-venue and Binance spot/bridge reference downloads are no longer research inputs. If those markets are needed again, obtain a newly authorized purchase rather than silently restoring the retired downloads. Retained funding/accounting evidence keeps its actual original lineage. Missing Binance Vision auxiliary coverage stays missing; do not silently switch to an exchange REST endpoint or invent zero funding. Metrics are optional and do not automatically become model features. The selected experiment still owns split, visibility, calibration and accounting admission.

## Commands and storage

```bash
python3 -m data --help
python3 -m data download --config <private-delivery-config.json>
python3 -m data inventory --root <purchased-archive-root> --output <private-calendar.json>
python3 -m data normalize --plan <private-input-plan.json> --output <new-fact-bundle>
python3 -m data validate --bundle <fact-bundle> --output <private-acceptance.json>
python3 -m data.downloaders.binance_vision --dataset metrics --symbol BTCUSDC --start YYYY-MM-DD --end YYYY-MM-DD --output-dir <auxiliary-working-root> --dry-run
```

The Vision CLI currently implements metrics acquisition. Funding acquisition must use a separately implemented and verified Vision archive route when needed; this guide does not claim a funding downloader already exists. Old trade/aggTrade acquisition is not offered by the current Vision CLI. No download is started by changing this policy.

`narrowgate data ...` calls this implementation. Download always uses the private resumable archive-only configuration: no implicit fusion, source retirement or alternate-source fallback. A 202 stays pending with backoff while other due files progress; it is not an empty successful file. Compression and configured content checks precede completion. Existing per-file/assignment locks and durable receipts remain in use, including by already running jobs.

Retain completed purchased compressed originals under **raw/<retained-batch>**, preserving the batch's internal paths and delivery state. `.incoming` is for unfinished transfers, not the permanent archive; its name never grants deletion authority. The download configuration selects an explicit batch root; the non-delivery entry requires `--output-root` or the configured purchased root, with no implicit `.incoming` destination. Determine XZ/Zstandard by actual compression. Facts, observations, Bars and features belong under the separate **derived** root. Use real directories, no supplier aliases or symlinks. Temporary work may use another volume, but atomic publication occurs on the destination filesystem. Never publish private delivery configuration or purchased records.

Relocate a retained batch only after its download, mirror and queued raw readers reach a safe boundary. Same-volume rename preserves bytes but does not atomically update configuration. Frozen facts/consumer manifests retain their historical bytes and paths. Calendar operations use the explicit current `--root` (or `validate-calendar --raw-root`), not a redirect or historical-path search. Reuse preserves every non-location plan field and ordered source identity; moved sources must match their recorded size, mtime and content digest. Raw relocation does not require rebuilding models or imply freed disk space. Delete verified, completed transport wrappers only after checking their retained source and pending users; reproducible caches remain protected while active or needed for recovery. Reports may be unique private evidence, not disposable cache.

Calendar builds use explicit `facts_schema_version` and `parser_contract_version` for parser compatibility. Bump versions only for semantic changes; comments, source location and recompilation do not invalidate reuse. Existing daily bundles must still satisfy ordered source, symbol/channel, original-content and structural checks. `validate-calendar --raw-root <current-archive-root>` validates selected source locations without rewriting frozen daily bundles.

[dataset_scope.json](dataset_scope.json) fixes **407 UTC dates, 2025-08-01..2026-09-11**: Binance USD-M perpetual BTCUSDC execution and BTCUSDT reference, each with L2 and trades. Context files do not enlarge the denominator. Missing/stale/difficult dates stay present. Availability, content acceptance, model support, previous-use and economic admission remain separate.

## Facts and observations

[facts.py](facts.py) creates source-bound Parquet bundles through the shared exact parser. A private plan declares the authorized `source_profile`, ordered `files`, actual paths, symbol/channel, optional expected checksums and explicit clock evidence. Legacy mixed-source Parquet is not admitted. The selected full input set shares one disk-backed trade identity index; include required adjacent context. Separate bundles do not prove cross-bundle deduplication.

Each stored row is one complete logical book message or one deduplicated trade. Parquet row groups do not split snapshots. Decimal values stay exact strings. Metadata binds input contract and original checksum; the reader verifies schema and shard/source identities. Failed builds publish no admitted bundle; existing bundles are not overwritten. Checksums identify bytes, not economic correctness.

For ordinary selection, replace `--plan` with `--root <archive-root> --start YYYY-MM-DD --end YYYY-MM-DD --symbol BTCUSDC` (repeat `--symbol` for the reference; optional repeated `--channel`). It selects every required date without source-ranking or good-day filtering and fails before building if a selected file is missing or ambiguous. Unknown mapping evidence remains unknown. `validate` reads the complete selected fact bundle and reconstructs books continuously across its files; invalid states and source gaps remain findings, not deleted dates.

Preserve contiguous original order and message boundaries across batches/fragments, without global timestamp sorting/grouping. Supplier receive time is technical grouping/provenance only, never strategy time, host receipt or latency calibration. Preserve source timestamp meaning and regressions; unproven mapper provenance stays unknown, not exchange-exact. Units cannot create missing time evidence.

Snapshots atomically replace the known book; deltas set absolute quantities, zero deletes, last write wins inside a message. Validate the whole message before mutation. Immutable Top20/BBO views share a version and all known levels remain available. Absent depth is unknown without coverage evidence. Rebases create no cancellation flow, free queue advancement or own-order cancellation. New unchanged source messages can refresh observation time, not state-change time; timer sampling refreshes neither.

Trades use market/trade ID and exact economic-content comparison: equal duplicates count once, conflicts fail, same-time distinct IDs survive. Individual-event, native-packet and derived-group counts remain distinct. Missing normal-trade classification and native aggregate membership are not synthesized.

[observation.py](observation.py) implements immutable observations, receive-order processing, an explicit derived depth publisher, ready-time trade windows and a minimal shared `FeatureFrame`. Historical records and live aggregate packets use explicit adapters; nine ID-range-derived children are delivered once as their observed packet. Strict historical adapters require explicit exchange-clock evidence; unknown/future clocks are rejected. Carried books retain real source observation time.

Publication, receive and ready clocks are separate. Window closure has an explicit lateness deadline and cannot backdate actual timer service. Late contributions are reported without rewriting closed Bars or past decisions. Observed empty windows have null OHLC and observed zero volume/count, not proof of exchange-wide inactivity; unknown capture stays unknown. Carried valuation prices are separate. Reference data cannot enter frames before readiness.

For ordered WebSocket simulation, `ObservationProfile.depth_connection_id` and `trade_connection_id` explicitly identify connection epochs. Declare both: equal IDs share send-order FIFO, while distinct IDs permit cross-connection overtaking. `DeliveryQueue.publish(connection_id=...)` projects sampled receive time to `max(publish + sampled_delay, previous_receive_on_connection)`, then applies the existing serial processing cost once. Equal receive times retain publication order without epsilon. This is a minimal head-of-line waiting approximation, not a packet-level congestion model or a reconstruction of measured delay correlations. Source/version order is not repaired; sender order is preserved. A new connection epoch is a new ID, not an implicit reset at midnight or an execution cut; stale data across connections/reconnects still needs its own consumer validity rule.

Both connection IDs absent preserve the historical independent-message scenario; this legacy mode is not an ordered-WS claim. Existing frozen manifests/results are not rewritten. New ordered scenarios must bind these fields and regenerate affected observations/features and prepared derivatives rather than relabel old cached ready times. Raw facts remain reusable. The read-only F08 clock diagnostic accepts `--transport-order separate_ws` or `shared_ws` as an explicitly reported scenario override; its default `bound` uses the original profile. Input-adapter throughput does not certify strategy replay speed or economic equivalence.

This minimal feature contract is **not the retired 13-head schema**. Production signal/replay consumers are not switched merely by importing it. Full consumer migration, declared scenarios and full-calendar acceptance remain explicit work.

## Label and economic preparation

[contracts.py](contracts.py) defines 203 independent full 48-hour accounts and a separately labelled final 24-hour day. Each starts with identical declared capital, flat inventory and no orders. Market/feature warmup remains separate; midnight inside a shard does not reset state. Terminal MTM is not free liquidation: retain inventory, valuation identity/age, fees and signed funding before account reset.

Label eligibility uses every required head's actual outcome end strictly before the split boundary, rejecting missing/censored outcomes. It generates no labels. Terminal accounting retains null all-in PnL for unavailable funding/unrealized value. These functions do not replace formal fill/queue assumptions, the execution ledger or lawful split/use manifests.

## Implementation boundaries

This table describes component responsibilities, not running experiment progress. Actual completion requires the selected experiment's receipts.

| Layer | Implemented | Still required |
| --- | --- | --- |
| Source/parser | 407-day inventory, exact parser, disk dedup, atomic bound facts | All purchases, full-calendar scans, historical clock mapping evidence |
| Book | Atomic updates and immutable known-depth views | Full-calendar gap/regression/depth acceptance |
| Observation/features | Delivery, windows, minimal shared frame | Complete new-model schema, production consumer migration and end-to-end parity |
| Labels | Actual-end eligibility | Generation and lawful split/use manifest |
| Economics | Slice/MTM/null-funding preparation | Experiment-specific fill/queue/fee/funding and ledger integration |
| Training/replay/live | No activation through data tools | Separate execution, validation and deployment authorization |

Tests use synthetic fixtures. Real engineering checks are labelled by their actual file/window scope, not full-calendar acceptance. Unscanned coverage, age, deduplication and future-fill fields stay unknown. Fact-only no-fill checks do not certify later publication. Contract parity is not native packet/receipt/queue parity. Old labels/models/caches are not new-contract inputs.

## Historical tools and cleanup

The shared [local maintenance and file index](../docs/maintenance.md) provides explicit preview/apply commands for verified terminal runtime locks. It does not remove market payloads or replace the data owner's prepare-cache lifecycle. Persistent and unknown-role locks remain protected; no-change maintenance does not rebuild the full index.

Netdisk backups retain original filenames and directory structure; transport adapters must not publish opaque split `.part` files or bundle otherwise separate inputs solely for transfer. Per-file identity checks, bounded concurrency and destination verification remain required. The provider may internally upload a file in multiple requests while exposing one original-named file. Dataset shards and active atomic cache-construction directories are not disposable transport fragments.

Ordinary operation uses `data` and the explicitly scoped Vision auxiliary entry above. The historical pipeline dispatcher and its aliases have been removed. Retired external acquisition commands exit with a retirement explanation rather than downloading new data.

The former market-data overview is retired and recoverable from the retained private consolidation snapshot, not distributed with the public repository. Its mixed-source layouts, coverage counts and good-day filters are historical, not current defaults. Binance Vision depth summaries are not incremental L2; Tardis L2 does not supply native own-order queue identity. No alternate venue/spot/bridge fallback is enabled. Funding storage is defined in [path conventions](../docs/path_conventions.md#private-runtime-configs), separately from books/trades.

Remove only verified superseded/reproducible outputs not owned by running jobs, updating current references. Retain purchased originals, funding, previous-use, locked research, unique live/account records and historical mechanism documents. Artifact retirement does not invalidate an entire research idea. See the [historical evidence register](../docs/legacy_l2_evidence_revalidation_20260725.md) and [path/privacy conventions](../docs/path_conventions.md).
