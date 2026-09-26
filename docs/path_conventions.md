# Path Conventions

[English](path_conventions.md) | [简体中文](path_conventions.zh-CN.md)

Last materially modified: 2026-09-26

Last materially synchronized: 2026-09-26

Status: Current public path and privacy contract.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

NarrowGate documentation uses public placeholders instead of personal machine paths or private research output directories.

Runtime paths are explicit. `MM_DATA_ROOT`, historical-root prefix maps, and the two `NARROWGATE_RETIRED_*` provenance placeholders are not runtime aliases. Migrate required locators offline; a missing selected input does not authorize borrowing a file from another root. Historical evidence retains its original identity.

## Placeholders

| Placeholder | Meaning |
| --- | --- |
| `${NARROWGATE_ROOT}` | Local clone of this repository |
| `${NARROWGATE_MARKETDATA_ROOT}` | Parent directory that contains local market-data workspaces |
| `${NARROWGATE_RAW_DATA_ROOT}` | Physical unified daily raw directory, normally `${NARROWGATE_MARKETDATA_ROOT}/NarrowGate_BTCUSDC/raw` |
| `${NARROWGATE_DATA_ROOT}` | Physical derived-data directory, normally `${NARROWGATE_MARKETDATA_ROOT}/NarrowGate_BTCUSDC/derived` |
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
| `${NARROWGATE_LIVE_REMOTE_POINTER}` | Ignored human-facing current-release selector; it never grants startup or deployment authority |
| `${NARROWGATE_LIVE_CONFIG}` | Stable ignored alias for the exact current live config; never a replay-default alias |
| `${NARROWGATE_PRIVATE_CONFIG_ROOT}` | Ignored owner-local directory containing create-only versioned live and replay configs |
| `${NARROWGATE_REMOTE_ROOT}` | Repository root on the selected private live host; supplied by owner routing configuration, not by release authority |
| `${NARROWGATE_REMOTE_HOME}` | Home directory on a private remote host; supplied locally and never published literally |
| `<current-live-host>` | Logical name for the current private live host; the public repository does not publish its address |
| `<current-live-instance>` | Logical name for the current cloud instance; the public repository does not publish its instance ID |
| `<current-live-ssh-target>` | Private SSH target resolved from owner routing configuration; never a public endpoint |
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
export NARROWGATE_RAW_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/raw"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/derived"
export NARROWGATE_CACHE_ROOT="${NARROWGATE_CACHE_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/NarrowGate_BTCUSDC}"
# When the internal storage gate fails, reusable DAG cache only:
# export NARROWGATE_REPLAY_DAG_CACHE_DIR="$NARROWGATE_DATA_ROOT/cache/replay_dag"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/backtest_results_btcusdc"
export NARROWGATE_PRIVATE_EVIDENCE_ROOT="$NARROWGATE_DATA_ROOT/reports"
# Set only when running an owner-private research integration:
# export NARROWGATE_PRIVATE_RESEARCH_ROOT="<owner-private-research-root>"
```

Cross-project private runtime pointers remain under `docs/private/`. Component-local unpublished evidence is owned by the ignored `live/private/`, `data/private/`, `models/private/`, or `execution/private/` root defined in [Non-Research Private Evidence Owners](public_private_documentation_contract.md#evidence-owners-and-local-catalogs). Each concrete research unit also owns an ignored `private/` directory for its artifact catalog and owner-only research context; see [Public Research and Private Evidence Layout](public_private_documentation_contract.md#evidence-owners-and-local-catalogs). None of these private surfaces is published, and a component-private root may not duplicate or override repository-wide current authority.

The physical storage volume, capacity policy, and current private-host locator are machine-local configuration, not public documentation. In a dated frozen report, a `<current-live-*>` placeholder means the private deployment that was current when that report became effective; it must not be rebound to today's host. Mutable current pointers select an existing release independently of activation; they are not startup authority. Actual activation evidence remains separate. Live startup independently requires the deployment-envelope root and stopped-exchange reconciliation root. Owner-side immutable evidence retains its original private runtime identity. The default cache root follows the bilingual README [Data Layout](../README.md#data-layout) section. Cache is reproducible and disposable; raw inputs, shared canonical data, and frozen evidence never inherit deletion authority merely because they are old. Private evidence paths are resolved through an owner-side locator without publishing their bytes or identity.

Repository package names do not identify data-storage roots. `data/` contains offline acquisition and normalization code. `live/orderbook/` contains the in-process execution-market book. Raw files belong under `${NARROWGATE_RAW_DATA_ROOT}` and processed files under `${NARROWGATE_DATA_ROOT}`. Both are real directories: no project symlinks or supplier aliases. Tick replay and mechanics caches belong under `${NARROWGATE_CACHE_ROOT}`; `NARROWGATE_TICK_WINDOW_CACHE_DIR` may override the legacy tick-window subdirectory without changing either data root. `NARROWGATE_REPLAY_DAG_CACHE_DIR` may separately override the component cache; its default is `${NARROWGATE_CACHE_ROOT}/replay_dag`. An external override must remain below `${NARROWGATE_DATA_ROOT}/cache`, not a raw-data or evidence directory. Strategy-dependent order, queue, fill, inventory and campaign paths must never be shared through either cache root.

## Market-Data Tree

The current public interface is `data`; the [data guide](../data/README.md) owns its input and storage contract. Download endpoints and account details stay in private configuration. Completed licensed compressed purchases belong in `raw/<retained-batch>` with their internal layout and delivery state unchanged; `.incoming` is reserved for unfinished transfers, not permanent retention. Directory names never grant deletion authority. Existing raw readers and frozen-source reuse must be accounted for before relocation; no compatibility symlink or global path-prefix fallback is introduced. Normalized facts and later observations, Bars and features belong under the separate derived root.

```text
<market-data-workspace>/
    raw/
        <retained-purchase-batch>/
    derived/
        <source-bound-fact-bundle>/
        <accepted-observation-or-feature-bundle>/
```

The diagram is a logical storage contract, not an instruction to rename an active download directory. Use `python -m data inventory`, `normalize` and `validate` with explicit real roots. Each fact bundle binds its original files, exact parser contract and schemas; later consumers cannot infer compatibility from a folder name alone. Delivery-specific paths belong to private manifests. Execution BTCUSDC and reference BTCUSDT are separate markets and must never alias each other.

Earlier `raw/binance_futures/<SYMBOL>/YYYY-MM-DD/*.parquet`, mixed-source daily containers, auxiliary spot/metrics layouts and versioned normalized caches are historical conventions. `data_paths.daily_market_path` remains a historical utility, not the new fact-bundle locator or a fallback. Existing external-venue data/evidence stays under its existing private owner until explicitly retired; it is not an automatically admitted source for the current contract. Do not recreate supplier aliases or bind old labels/models to new data by reusing their directory names.

Use the fixed current calendar manifest for availability and retain previous-use records for research rights. File presence, successful conversion and restored coverage do not establish model support, causal parity, funding completeness or economic admission. Raw inputs, unique runtime records and frozen evidence are not disposable caches.

## Private Runtime Configs

Retained funding settlement inputs live at `${NARROWGATE_RAW_DATA_ROOT}/accounting/funding/<SYMBOL>/YYYY-MM-DD.parquet`, separate from purchased books/trades and derived products. `daily_market_path(..., "funding")` resolves this accounting layout; replay accepts the workspace root or the accounting symbol directory. Migration preserves file bytes, updates the current daily index and writes a private funding manifest. Historical conversion receipts remain historical locators, not current reader indexes. No symlink or old-layout fallback is created.

The tracked `live/config.yaml` is a public template. Resolve the current private locators through the ignored selector and pass the stable live alias explicitly; startup authority still comes from the separately supplied deployment envelope and stopped-exchange reconciliation roots:

```bash
export NARROWGATE_LIVE_REMOTE_POINTER="<owner-local-current-pointer>"
export NARROWGATE_LIVE_CONFIG="<owner-local-current-live-config>"
bash live/run.sh start
```

`make deploy-preflight` refuses to admit a file marked `PUBLIC TEMPLATE`; use `NARROWGATE_LIVE_CONFIG` to select a private deployment config for that local check. `make publish-source-dry` and `make publish-source` transport only a clean public Git checkout and never inspect that config. The repository distributes neither a current operational identity nor a backtest authority identity. Both are owner-private, `private_not_distributed`, and must be supplied and verified explicitly. Missing or mismatched private bytes fail closed, and a current live alias may never substitute for backtest authority.

## Documentation Rule

Public docs should not include:

- personal absolute paths such as `/Users/<name>/...`;
- private live hostnames, SSH targets, account paths, or process IDs;
- raw live PnL / position / order-count snapshots;
- full one-off result filenames when `<tag>` is sufficient;
- dated model bundle names unless the bundle is intentionally shipped as a public artifact.

Use placeholders and command arguments instead. Public reports must also follow the [public/private documentation and evidence contract](public_private_documentation_contract.md): a SHA256 value identifies bytes but is not a reader-accessible location.
