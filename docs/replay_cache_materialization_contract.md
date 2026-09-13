# Replay Cache Materialization Contract

Last materially modified: 2026-08-29

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Purpose

The replay/feature DAG is used to choose reusable materialization boundaries, not merely to document call order. A persistent cache node must be completely determined before any strategy action can change orders, queue position, fills, inventory or campaign state.

The current machine-readable authority is `models/replay_cache_dag.py::REPLAY_WINDOW_CACHE_GRAPH_V2`. The v1 graph and its SHA256 remain exported because existing `native_exchange_book_hour_v1` artifacts bind that identity. Adding v2 components must not invalidate the already reusable native-hour layer.

## Storage

Raw data and frozen evidence remain on `${NARROWGATE_MARKETDATA_ROOT}`. Disposable, reproducible cache artifacts use `${NARROWGATE_CACHE_ROOT}`. Its portable XDG resolution is defined once in the bilingual README [Data Layout](../README.md#data-layout) section; this contract does not introduce another platform-specific default:

```text
${NARROWGATE_CACHE_ROOT}/
├── window_cache/                 # legacy monolithic tick_window v10-v13
│   └── components_v2/
│       ├── market_context_day_v2/
│       └── model_overlay_day/
└── replay_dag/
    └── native_exchange_book_hour_v1/
```

`NARROWGATE_REPLAY_DAG_CACHE_DIR` may override only the new DAG cache root. When the internal capacity gate fails, it may explicitly select `${NARROWGATE_DATA_ROOT}/cache/replay_dag` on the removable volume. It must not redirect raw evidence or frozen reports, and a missing removable volume is a hard error rather than a fallback to another path. Cache keys remain content/semantics identities and do not include the storage tier.

## Current Audit

The read-only audit on 2026-08-03 found 269 legacy pickle files across 132 dates with 278.60 GiB of logical file size:

| Version | Files | Size |
|---|---:|---:|
| v10 | 17 | 17.06 GiB |
| v11 | 1 | 1.13 GiB |
| v12 | 98 | 88.98 GiB |
| v13 | 153 | 171.43 GiB |

Forty-two dates have more than one cache variant, producing 137 files beyond a single file per date; the maximum is six variants on one date. v13 contains 153 files for 129 distinct dates. Its cache key includes several gate/config identities and each pickle embeds trades, rolling arrays, BBO, L2 and optional model data together. This duplicates large strategy-independent arrays when only a downstream gate or model mode changes. Existing v13 files remain read-compatible, but new misses no longer publish another monolith by default.

No legacy cache was deleted by this migration. Exact basename scanning found 152 v13 files, 170.34 GiB, referenced by frozen/evidence manifests on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`. The remaining 117 files, 108.27 GiB, are only unreferenced candidates and still require user approval before deletion. See `docs/replay_cache_dag_v2_governance_20260803.md`.

## Materialization Classes

### Persistent and reusable

- one source hour of native snapshot/delta rows converted into ordered logical exchange-book messages;
- normalized BBO/L2 and rolling market-context arrays, once split from the legacy monolith;
- causal feature blocks whose cutoff, ready clock, source identity and feature semantics are frozen;
- model prediction overlays bound to a feature artifact and model bundle.

### Ephemeral assembly

The in-memory replay window should compose reusable nodes. It must not create another full copy merely because an experiment changes a support gate, scorecard, action name or outcome definition.

### Never shared across arms

- submitted, active, pending-cancel and terminal order state;
- action-dependent queue position and queue reset path;
- cancel request/ACK and ACK-before-fill race after path divergence;
- fills, inventory, cooldown lineage and campaign state;
- reward, terminal PnL and action-specific labels.

Persisting these as a shared `window_cache` would leak the control path into a candidate arm and invalidate the counterfactual.

## Implemented Native-Hour Node

`native_orderbook_logical_hour` is the first fully implemented DAG cache node. `CryptoHFTExchangeBookTape` now materializes each source hour once as compressed Parquet and then composes the requested target day plus D-1 warmup from those hour partitions.

The key contains:

- source path, size and mtime;
- exchange, symbol, market id and tick size;
- raw parser identity;
- exchange-event mapping and event-schema versions;
- replay-cache DAG identity and clock contract.

It deliberately excludes strategy configuration, P3, model bundle, queue artifact, latency sampler, random seed, arm and experiment identity. Those do not change the public exchange event stream.

Writes use a per-artifact file lock and atomic Parquet/manifest publication. The manifest records row/level counts, first/last exchange timestamps, file size and SHA256. A source identity change selects a new artifact. Cache failure falls back to source parsing with an explicit warning; it cannot change event semantics.

## Three Persistent Layers

Only these strategy-independent boundaries may persist:

1. `native_book_hour`: one source hour of logical native book messages;
2. `market_context_day_v2`: execution trades, rolling arrays and source refs;
3. `model_overlay_day`: model-bound prediction arrays.

`WindowData` is assembled only in memory. Normalized BBO/L2 Parquet is not copied into `market_context_day_v2`. Each `source_references.json` entry keeps the absolute path, size and mtime only as a mutable locator/audit record. Cache identity uses only stable `role + logical_source + content SHA256`; relocating the same bytes to `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` or touching mtime therefore preserves the cache hit. Changing source bytes selects a new identity. Large inputs reuse producer or dataset manifest hashes; direct hashing is a bounded fallback for small files.

Each market-context artifact is an atomically published directory:

```text
market_context_day_v2/<symbol>/<day>/<identity_sha256>/
├── manifest.json
├── rolling_arrays.npz
├── trades.parquet
└── source_references.json
```

Trades use Zstandard Parquet compression. Rolling arrays and model overlays use compressed NPZ without object arrays. The manifest binds the canonical identity SHA256, schema SHA256, DAG SHA256, source-reference identity and every materialized file's size and SHA256. A cache hit verifies those hashes before use. Publication uses a per-identity file lock and a same-filesystem temporary directory followed by atomic rename.

Changing a model or feature identity only creates a small `model_overlay_day` artifact. Changing a support gate, scorecard, action name, random seed or outcome definition invalidates neither persistent component. Changing source content or the market-context transform selects a different content-addressed directory. A locator-only path or mtime change does not.

Existing monolithic v10-v13 and `components_v1` pickle artifacts remain read-compatible. New misses publish v2 directories by default. New v1 writes require `legacy_component_v1_write_enabled=true`; monolithic writes still require `legacy_monolithic_window_cache_write_enabled=true`.

Legacy monolithic writes require the explicit `legacy_monolithic_window_cache_write_enabled=true` compatibility flag. The default path is component materialization plus ephemeral `WindowData` assembly.

## Remaining Migration

The remaining work is deliberately incremental:

- migrate one small source-compatible fixture/day at a time;
- compare every DataFrame column, ndarray dtype/shape/value, source identity and replay output against the legacy window;
- measure compressed size and load time before selecting a migration batch;
- preserve every frozen-referenced v13 file;
- ask for user approval before deleting any unreferenced legacy file.

Do not prewarm all 132 days while internal free space is close to the safety reserve. The audit command is read-only unless `--output` is explicitly set:

```bash
.venv/bin/python scripts/audit_legacy_replay_cache.py \
  --cache-root "${NARROWGATE_CACHE_ROOT}/window_cache" \
  --reference-root . \
  --reference-root ${NARROWGATE_DATA_ROOT}/reports
```

Cache invalidation and research identity remain different concerns: a cache can be regenerated, while a frozen input path/hash must remain reproducible.
