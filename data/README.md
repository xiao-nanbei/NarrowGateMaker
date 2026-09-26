# Data

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-26
Last materially synchronized: 2026-09-26

One public `data` layer owns acquisition, facts, observations and input contracts. Users select market/channel/date, not a supplier. Delivery addresses and account configuration stay private. Internal adapters and manifests retain real origin; neutral naming does not erase lineage. Licensed market records are not distributed with the source repository.

## Code organization

All eight acquisition adapters are in [downloaders/](downloaders/): purchased archives, Binance Vision, CryptoHFTData books/trades, Bitget, Bybit, OKX and quota-bounded Infoway. Binance and CryptoHFTData tools are retained, not deleted. The current `data download` remains the purchased-archive workflow with no alternate-source fallback. Internal adapter names are implementation details, not user-facing data directory names. Parsing/facts, observation scheduling and quality checks stay outside the downloader package. Existing historical helper logic inside adapters is retained unchanged in this directory-only migration.

Retaining a tool does not admit its output into an experiment. Current F03 books/trades remain Tardis-only, with existing real funding as separately authorized accounting input. The selected experiment plan owns its controls: this guide neither cancels authorized ML-OFF nor restores old B0. Metrics supplementation is not a prerequisite. Existing metrics permission does not automatically add features to new models. This reorganization does not itself download data or establish model compatibility.

## Commands and storage

```bash
.venv/bin/python -m data --help
.venv/bin/python -m data download --config <private-delivery-config.json>
.venv/bin/python -m data inventory --root <purchased-archive-root> --output <private-calendar.json>
.venv/bin/python -m data normalize --plan <private-input-plan.json> --output <new-fact-bundle>
.venv/bin/python -m data validate --bundle <fact-bundle> --output <private-acceptance.json>
```

`narrowgate data ...` calls this implementation. Download always uses the private resumable archive-only configuration: no implicit fusion, source retirement or alternate-source fallback. A 202 stays pending with backoff while other due files progress; it is not an empty successful file. Compression and configured content checks precede completion. Existing per-file/assignment locks and durable receipts remain in use, including by already running jobs.

Retain completed purchased compressed originals under **raw/<retained-batch>**, preserving the batch's internal paths and delivery state. `.incoming` is for unfinished transfers, not the permanent archive; its name never grants deletion authority. The download configuration selects an explicit batch root; the non-delivery entry requires `--output-root` or the configured purchased root, with no implicit `.incoming` destination. Determine XZ/Zstandard by actual compression. Facts, observations, Bars and features belong under the separate **derived** root. Use real directories, no supplier aliases or symlinks. Temporary work may use another volume, but atomic publication occurs on the destination filesystem. Never publish private delivery configuration or purchased records.

Relocate a retained batch only after its download, mirror and queued raw readers reach a safe boundary. Same-volume rename preserves bytes but does not atomically update configuration. Frozen facts/consumer manifests retain their historical bytes and paths; calendar reuse currently also checks the original source paths and full plan, so do not move a batch until that operational dependency is addressed explicitly. Raw relocation does not require rebuilding models or imply freed disk space. Delete verified, completed transport wrappers only after checking their retained source and pending users; reproducible caches remain protected while active or needed for recovery. Reports may be unique private evidence, not disposable cache.

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

Ordinary operation uses `data`. The historical pipeline dispatcher and its aliases have been removed. External acquisition adapters retain their distinct protocol responsibilities; they do not grant alternate-source admission.

The former market-data overview is retired and recoverable from the retained private consolidation snapshot, not distributed with the public repository. Its mixed-source layouts, coverage counts and good-day filters are historical, not current defaults. Binance Vision depth summaries are not incremental L2; Tardis L2 does not supply native own-order queue identity. Separate venue/spot/bridge inputs require their own source and visibility evidence. Funding storage is defined in [path conventions](../docs/path_conventions.md#private-runtime-configs), separately from books/trades.

Remove only verified superseded/reproducible outputs not owned by running jobs, updating current references. Retain purchased originals, funding, previous-use, locked research, unique live/account records and historical mechanism documents. Artifact retirement does not invalidate an entire research idea. See the [historical evidence register](../docs/legacy_l2_evidence_revalidation_20260725.md) and [path/privacy conventions](../docs/path_conventions.md).
