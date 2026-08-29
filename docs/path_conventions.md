# Path Conventions

Last materially modified: 2026-08-29

Status: Current public path and privacy contract.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

NarrowGate documentation uses public placeholders instead of personal machine paths or private research output directories.

## Placeholders

| Placeholder | Meaning |
| --- | --- |
| `${NARROWGATE_ROOT}` | Local clone of this repository |
| `${NARROWGATE_MARKETDATA_ROOT}` | Parent directory that contains local market-data workspaces |
| `${NARROWGATE_DATA_ROOT}` | Local market-data root |
| `${NARROWGATE_RETIRED_MARKETDATA_ROOT}` | Historical pre-relocation market-data root; provenance only, not a runtime default |
| `${NARROWGATE_RETIRED_DATA_ROOT}` | Historical pre-relocation NarrowGate data root; provenance only |
| `${NARROWGATE_STORAGE_ROOT}` | Machine-local physical storage root configured outside the public repository |
| `${NARROWGATE_LOCAL_HOME}` | Current user's local home directory; never publish the literal owner path |
| `${NARROWGATE_EPHEMERAL_ROOT}` | Disposable local temporary-work root; never evidence authority |
| `${NARROWGATE_CACHE_ROOT}` | Default root for disposable, reproducible caches |
| `${NARROWGATE_REPLAY_DAG_CACHE_DIR}` | Explicit tier override for reusable replay-DAG materializations |
| `${NARROWGATE_RESULTS_DIR}` | Backtest / audit / evidence output root |
| `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` | Owner-side evidence store that is not distributed with the public repository |
| `${NARROWGATE_PRIVATE_RESEARCH_ROOT}` | Explicit owner-side root for frozen research inputs omitted from the public clone; no public default |
| `${NARROWGATE_MODEL_DIR}` | Model bundle directory used for a specific run |
| `${NARROWGATE_MAIN_MODEL_DIR}` | Main quote-model bundle for a specific run |
| `${NARROWGATE_QUOTE_EV_MODEL_DIR}` | Quote-EV model bundle for a specific run |
| `${NARROWGATE_LIVE_REMOTE_POINTER}` | Ignored repository-wide current-host pointer; the sole local resolver for current remote authority |
| `${NARROWGATE_LIVE_CONFIG}` | Stable ignored alias for the exact current live config; never a replay-default alias |
| `${NARROWGATE_PRIVATE_CONFIG_ROOT}` | Ignored owner-local directory containing create-only versioned live and replay configs |
| `${NARROWGATE_REMOTE_ROOT}` | Repository root on the current private live host; resolved from the ignored current-host pointer |
| `${NARROWGATE_REMOTE_HOME}` | Home directory on a private remote host; resolved locally and never published literally |
| `<current-live-host>` | Logical name for the current private live host; the public repository does not publish its address |
| `<current-live-instance>` | Logical name for the current cloud instance; the public repository does not publish its instance ID |
| `<current-live-ssh-target>` | Private SSH target resolved from the ignored current-host pointer; never a public endpoint |
| `<current-live-eip-allocation>` | Logical current public-address allocation identity; never publish the allocation ID |
| `<current-live-epoch>` | Mutable current-epoch locator in current-facing documents; inside a frozen dated contract it means the epoch current when that contract was frozen and must not be rebound |
| `<current-live-epoch-start>` | Start of the epoch resolved by the current private pointer; never infer it from a public address or an older report |
| `<admitted-predecessor-epoch>` | One prior private runtime epoch with an owner-side availability boundary; never a current endpoint |
| `<retired-live-host>` | Logical name for a retired deployment epoch; it is not a reachable endpoint |
| `<retired-runtime-archive>` | Owner-private retirement archive for an unavailable predecessor runtime; not a public path |
| `<tag>` | User-chosen experiment tag |
| `<symbol>` | Lowercase symbol suffix, for example `btcusdc` |

## Suggested Local Setup

```bash
export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC"
export NARROWGATE_CACHE_ROOT="${NARROWGATE_CACHE_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/NarrowGate_BTCUSDC}"
# When the internal storage gate fails, reusable DAG cache only:
# export NARROWGATE_REPLAY_DAG_CACHE_DIR="$NARROWGATE_DATA_ROOT/cache/replay_dag"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/backtest_results_btcusdc"
export NARROWGATE_PRIVATE_EVIDENCE_ROOT="$NARROWGATE_DATA_ROOT/reports"
# Set only when running an owner-private research integration:
# export NARROWGATE_PRIVATE_RESEARCH_ROOT="<owner-private-research-root>"
```

Cross-project private runtime pointers remain under `docs/private/`. Component-local unpublished evidence is owned by the ignored `live/private/`, `data/private/`, `models/private/`, or `execution/private/` root defined in [Non-Research Private Evidence Owners](non_research_private_evidence_owners.md). Each concrete research unit also owns an ignored `private/` directory for its artifact catalog and owner-only research context; see [Public Research and Private Evidence Layout](../research/PRIVATE_EVIDENCE.md). None of these private surfaces is published, and a component-private root may not duplicate or override repository-wide current authority.

The physical storage volume, capacity policy, and current private-host locator are machine-local configuration, not public documentation. In a dated frozen report, a `<current-live-*>` placeholder means the private deployment that was current when that report became effective; it must not be rebound to today's host. Mutable current pointers may advance, while owner-side immutable evidence retains its original private runtime identity. The default cache root follows the bilingual README [Data Layout](../README.md#data-layout) section. Cache is reproducible and disposable; raw inputs, shared canonical data, and frozen evidence never inherit deletion authority merely because they are old. Private evidence paths are resolved through an owner-side locator without publishing their bytes or identity.

Repository package names do not identify data-storage roots. `data/` contains offline acquisition and normalization code. `live/orderbook/` contains the in-process execution-market book reconstructed from REST snapshot plus WebSocket diff depth. Actual market-data files belong only under `${NARROWGATE_DATA_ROOT}`. Tick replay and mechanics caches belong under `${NARROWGATE_CACHE_ROOT}`; `NARROWGATE_TICK_WINDOW_CACHE_DIR` may override the legacy tick-window subdirectory without changing the data root. `NARROWGATE_REPLAY_DAG_CACHE_DIR` may separately override the component cache; its default is `${NARROWGATE_CACHE_ROOT}/replay_dag`. An external override must remain below `${NARROWGATE_DATA_ROOT}/cache`, not a raw-data or evidence directory. Strategy-dependent order, queue, fill, inventory and campaign paths must never be shared through either cache root.

## Market-Data Tree

The BTCUSDC execution source keeps stable raw/feature names such as `raw_trades`, `bars_1s`, and `features_btcusdc`. Formal normalized top-book replay uses the versioned `normalized_l2_100ms_v2/{bbo,l2}` root. The old flat `bbo/` and `l2/` paths are legacy compatibility views and must not be globbed for new research. New external sources must use exactly:

```text
${NARROWGATE_DATA_ROOT}/external_venues/<venue>/<instrument>/<symbol>/<dataset>
```

Current values are `<instrument>=perp` and `<dataset>=trades|features_1s`. Exchange product names such as `linear` and `USDT-FUTURES` belong in metadata, not in physical directory names. Do not create parallel aliases such as `usdt-futures`, `linear`, `swap`, or venue-specific top-level data roots.

See [the market-data guide](market_data.md) for source provenance, downloads, normalization, UTC handling, and retained-day rules.

## Private Runtime Configs

The tracked `live/config.yaml` is a public template. Resolve live authority through the ignored pointer and pass the stable live alias explicitly:

```bash
export NARROWGATE_LIVE_REMOTE_POINTER="<owner-local-current-pointer>"
export NARROWGATE_LIVE_CONFIG="<owner-local-current-live-config>"
bash live/run.sh start
```

`make deploy` refuses to deploy a file marked `PUBLIC TEMPLATE`; use `NARROWGATE_LIVE_CONFIG` for private deployment. The repository distributes neither a current operational identity nor a backtest authority identity. Both are owner-private, `private_not_distributed`, and must be supplied and verified explicitly. Missing or mismatched private bytes fail closed, and a current live alias may never substitute for backtest authority.

## Documentation Rule

Public docs should not include:

- personal absolute paths such as `/Users/<name>/...`;
- private live hostnames, SSH targets, account paths, or process IDs;
- raw live PnL / position / order-count snapshots;
- full one-off result filenames when `<tag>` is sufficient;
- dated model bundle names unless the bundle is intentionally shipped as a public artifact.

Use placeholders and command arguments instead. Public reports must also follow the [public/private documentation and evidence contract](public_private_documentation_contract.md): a SHA256 value identifies bytes but is not a reader-accessible location.
