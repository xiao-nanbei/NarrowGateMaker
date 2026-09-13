# Data workflow

Last materially modified: 2026-09-19

Read [dataset_scope.json](../../../../data/dataset_scope.json) for the version's calendar, markets, source restrictions, accounting exceptions and migration status, then the bound source/quality manifests. The [data guide](../../../../data/README.md) owns interfaces; verify actual CLI help before use.

Use quality tied to the current files and parser. Historical `data_quality.py` exclusions or old supplier bad-day reports do not admit/exclude new inputs; consult them only for matching historical identities. Keep declared dates, unknown support and previous-use status. A known exchange interruption is not a recurring download obligation: retain measured gaps/staleness and unsupported outcomes.

Preserve purchased originals and separate derivatives. Honor the actual source policy; retained alternate downloaders do not grant fallback. Match venue/contract/symbol/units/channel, never silently substitute reference or spot markets.

Restore contiguous complete messages before atomic snapshots; no non-contiguous grouping or partial snapshot publication. Retain all known depth before Top-N and same-version BBO. Rebase is not market cancel flow or free queue reset. Missing native sequence/order/queue fields remain unavailable.

Separate source clock meaning, output time, observation age and modeled readiness. Provider receipt is provenance/grouping, not local strategy time or network calibration. Unit conversion does not prove clock origin. Carry only past valid state, retaining observation age; no future-fill or sampling-driven freshness.

Deduplicate by reliable market/trade identity and equal content; conflicting IDs fail, distinct IDs at one timestamp survive. Individual events, native packets and derived groups differ. Unknown quantities/counts cannot become zero; observed-empty is not proof of exchange-zero. Carried valuation prices are not new trades/OHLC.

Test atomic messages, chunk/shard invariance, cross-day context, conservation, identity, missing/stale and future visibility. Whole-calendar acceptance needs actual daily results; small fixtures are not full acceptance. Rebuild affected derivatives after source/schema changes and reject old-artifact fallback.

Cleanup needs authorization and checks for active users, replacement, indexes and pinned evidence. Age/quality alone does not authorize raw/evidence deletion. During authorized cache cleanup, reproducible unpinned unused caches older than 14 days may be removed; this is not a raw or frozen-evidence TTL.

Output input/derivative identity, coverage/support, actual versus pending tests, changed consumers and blockers. Data-only work needs no quote decomposition or economic-gain claim.
