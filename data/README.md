# Offline Data Tooling

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../docs/public_private_documentation_contract.md).

This package contains offline data acquisition, import, normalization, and quality-registry entrypoints. It contains code, not market-data payloads.

Large raw and derived datasets live outside the repository. The canonical machine-local roots are:

```text
NARROWGATE_MARKETDATA_ROOT=<local-marketdata-root>
NARROWGATE_DATA_ROOT=<local-marketdata-root>/NarrowGate_BTCUSDC
NARROWGATE_CACHE_ROOT=$HOME/Library/Caches/NarrowGate_BTCUSDC
# Optional: NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_CACHE_ROOT/replay_dag
# Capacity fallback for reusable DAG cache only:
# NARROWGATE_REPLAY_DAG_CACHE_DIR=$NARROWGATE_DATA_ROOT/cache/replay_dag
```

`data_paths.py` owns runtime path resolution; new code must not embed a user home-directory path. Frozen evidence may retain the pre-migration `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` provenance string and is resolved at the filesystem boundary without rewriting the frozen bytes. No compatibility symlink is created at the old location.

Runtime WebSocket state belongs under `live/`; the Binance execution-market snapshot-plus-diff book is implemented in `live/orderbook/binance_usdm.py`. State attached to NarrowGate's own active orders belongs under `execution/`.

The ownership rule is:

```text
data/             offline ETL and data-quality tools
live/orderbook/   live public-book reconstruction
execution/        active-order and queue-path state
$NARROWGATE_MARKETDATA_ROOT/  provider-level raw archives
$NARROWGATE_DATA_ROOT/        normalized/generated NarrowGate datasets
$NARROWGATE_CACHE_ROOT/       default disposable cache tier
$NARROWGATE_DATA_ROOT/cache/  explicit removable tier for reusable DAG cache
```

Reusable source/feature materializations are declared by `models/replay_cache_dag.py`. Native CryptoHFT logical messages are cached per source hour, so target days, D-1 warmups and experiment arms reuse one parser output. Strategy-dependent order, queue, fill, inventory and campaign paths are explicitly non-cacheable across arms.

Do not add live socket state machines or strategy logic to this package.

Provider archives remain source-separated. The Tardis downloader is a bounded, one-off historical acquisition tool. It writes directly below `${NARROWGATE_MARKETDATA_ROOT}/tardis/` and records remote size, ETag, SHA256, and zstd integrity in an atomic manifest:

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

Build a source-separated Tardis candidate after the download manifest is complete:

```bash
.venv/bin/python pipeline.py normalize-tardis \
  --manifest "$NARROWGATE_MARKETDATA_ROOT/tardis/manifests/<manifest>.json" \
  --day 2025-08-01 \
  --output-root "$NARROWGATE_DATA_ROOT/normalized_tardis_l2_100ms_v1" \
  --workers 3
```

This uses the Tardis provider-local clock, not AWS receive time. Output is `policy_visible=false`, `live_transport_eligible=false`, and `exact_queue_policy_eligible=false`. The full bucket, gap, bookTicker, and CryptoHFTData overlap contract is documented in `docs/tardis_normalized_l2_contract_20260731.md`.

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
