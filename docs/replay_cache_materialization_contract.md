# Replay Cache Materialization Contract

Last materially modified: 2026-09-20
Last materially synchronized: 2026-09-20
Chinese: [缓存物化契约](replay_cache_materialization_contract.zh-CN.md)

## Scope and retirement

Caches remain reusable implementation components, not a source-policy override. The current experiment selects its input contract; historical native-hour consumers do not authorize using an old supplier in a new experiment.

On 2026-09-20 the SQLite access ledger, LRU hot/cold migration and deletion planner, management CLI, legacy v10–v13 inventory/prune chain, fixed node-policy audit and date-bound reference-graph scripts were retired. Their dedicated tests were removed with the functionality. Cache consumers no longer register reads or writes with a ledger.

No on-disk cache was deleted or moved by this source change. Existing aliases and their cold targets remain readable; deleting management code does not authorize deleting those targets. Ordinary cache read/write, integrity checks, locking and atomic publication remain. Integration tests retain roundtrips, misses, invalid-artifact handling and existing symlink-alias reads.

The byte-identical 2026-08-03 inventory was consolidated into the [2026-08-04 historical report](replay_cache_legacy_reference_audit_20260804.json), and its projection index was updated. Historical findings remain in the [governance report](replay_cache_dag_v2_governance_20260803.md); they are not a current disk inventory, current consumer policy or deletion authorization.

## Materialization boundaries

The replay/feature DAG chooses reusable boundaries. A shared persistent node must be independent of strategy actions. The machine-readable graph lives in `models/replay_cache_dag.py`; existing artifact-bound graph identities remain supported.

Reusable classes include parsed source messages, normalized market context, causal feature blocks and model-bound prediction overlays. Their identity must include applicable source, transformation, schema, clock and model semantics.

Never share submitted/active/canceling orders, queue position after action divergence, fills, inventory, cooldown/campaign state, reward, terminal PnL or action-dependent labels across arms. Assemble replay windows in memory rather than duplicating market arrays for each downstream policy.

## Storage and identity

Raw data and frozen evidence remain in their declared roots. Disposable caches use `${NARROWGATE_CACHE_ROOT}`; portable resolution is defined in the [README data layout](../README.md#data-layout). Explicit cache-root overrides do not relocate raw inputs or evidence. Missing required volumes must not silently select another source.

Market-context and overlay identities use stable source roles, logical identities and content hashes. Locator-only path/mtime changes do not invalidate these content-addressed components. Reuse producer/manifest hashes rather than repeatedly scanning large inputs. The legacy native-hour identity additionally binds its source path, size and mtime, parser, event mapping, market and tick size; do not confuse these different cache contracts.

Market-context v2 stores trades as Zstandard Parquet, arrays as compressed NPZ without objects, plus a manifest and source references. Model overlays are separately bound to features and model identity. BBO/L2 source files are referenced rather than copied into each market component. File sizes/hashes and schema/identity bindings are checked on load. Per-identity locking and same-filesystem atomic publication prevent partial hits.

A model change invalidates its overlay, not otherwise identical market context. A source or transform change selects a new identity. Strategy parameters that do not affect a component's contents must not enter its key. Native-hour cache failures retain explicit source-parsing fallback and warnings; removing the ledger does not change this behavior.

Legacy reads and explicit compatibility write flags remain where implemented. This does not admit old inputs into a new source contract. New component materialization and ephemeral window assembly remain separate from legacy monolithic writes.

## Maintenance and validation

Retired tools are not supported cleanup entry points. Before any separate disk cleanup, inspect actual active users, aliases and their targets, unique contents and frozen references; historical inventory dates alone are insufficient.

For cache changes, compare small controlled inputs across fresh generation and cache hits: frames, array values/dtypes/shapes, identities, misses and corrupted-artifact handling. Where replay behavior changes, additionally compare orders, fills, inventory and PnL under identical inputs. Engineering cleanup is not a new economic experiment, and passing cache tests is not proof of profitability or full research-source migration.
