# Live Host And Historical Data Access

Last materially modified: 2026-08-25

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Exact endpoints, instance IDs, owner archives, and private receipts are resolved through the ignored private evidence layer and are not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values, host placeholders, and deployment-epoch names are logical locators. See the [public/private documentation contract](public_private_documentation_contract.md) and [path conventions](path_conventions.md).

Status: `private_current_pointer_v13_live_v12_backtest_split_fail_closed_history`

## Current and retired hosts

The current NarrowGate BTCUSDC live authority is AWS Tokyo `<current-live-host>` / `<current-live-instance>`, reached only through `<current-live-ssh-target>`. Its repository root is `${NARROWGATE_REMOTE_ROOT}` and its current bound epoch is `<current-live-epoch>`. Operational scripts must resolve `${NARROWGATE_LIVE_REMOTE_POINTER}` or an explicitly supplied `NARROWGATE_LIVE_REMOTE`; they must never copy an address, instance identifier, epoch identifier, or repository path from a public or dated report.

The owner-active BUY E3 release-v3 is resolved only after the private current-host pointer is validated against the stable live-config alias and private release/evidence chain. Its admitted lifecycle and post-lifecycle receipts establish a frozen operational identity and health observation with all shadow and companion surfaces disabled. They do not establish the process's latest status, prove that a nonbaseline action occurred, prove an economic effect, or authorize a replay baseline. Re-check the live process independently before any remote mutation.

Historical authority includes earlier admitted epochs on the current cloud instance plus the original AWS Tokyo host, the intermediate Vultr Tokyo host, and the reactivated AWS Tokyo predecessor `<retired-reactivated-aws-host>` / `<retired-reactivated-aws-instance>`. None may be used for current status, deployment, capture, write, delete, or SSH fallback. Routine historical queries use exact epoch-bound evidence from `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` and fail closed across unadmitted intervals. The dated [Vultr migration receipt](../research/system_engineering/docs/vultr_tokyo_live_host_migration_v1_20260811.md), [AWS reactivation receipt](../research/system_engineering/docs/aws_tokyo_live_host_reactivation_v1_20260811.md), and [AWS host-migration receipt](../research/system_engineering/docs/aws_tokyo_live_host_migration_v2_20260820.md) remain immutable event records. A dated document's `<current-live-*>` placeholder means the deployment current when that document became effective; it must not be dynamically reinterpreted as today's host or epoch.

## Query routing

| Requested scope | Authoritative source |
| --- | --- |
| `[<current-live-epoch-start>, now)` | Current AWS `<current-live-host>`, after resolving and revalidating `${NARROWGATE_LIVE_REMOTE_POINTER}` |
| Earlier admitted epochs on `<current-live-instance>` | Exact epoch-bound private receipts and archives; never the current network endpoint as a substitute |
| Reactivated-AWS predecessor interval | `<retired-reactivated-aws-archive>` and its private manifest |
| Intermediate-Vultr predecessor interval | Its verified retirement archive in `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` |
| Original-AWS predecessor interval | Its verified retirement archive in `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` |
| Historical receive-time capture | Immutable v1 ledger plus source-bound v2 ledger and admitted tape directories |
| Frozen research conclusion | The report/spec and exact artifact named by the family registry |

For fill or trade queries, normalize to `[start_utc,end_utc)`, resolve the private chronology, split at every host and runtime-epoch boundary, read each intersecting source, preserve `source_host` and `epoch_key`, preserve gaps, then filter and union. Only the currently resolved partition may use SSH. The same address or cloud instance may have hosted multiple incompatible epochs; rows from a neighboring epoch must never substitute for missing rows inside another epoch.

The following early migration gaps remain historical and must still be preserved:

- `[2026-08-11T11:43:10Z, 2026-08-11T11:47:12.288Z)` between original AWS and Vultr;
- `[2026-08-11T15:17:38Z, 2026-08-11T15:48:02Z)` between Vultr and reactivated AWS;
- `[2026-08-19T18:55:39Z, 2026-08-19T18:59:09Z)` between the reactivated-AWS predecessor and the current AWS host.

The Vultr signed API first returned `-2015` at `2026-08-11T15:11:22Z`; preserve `[2026-08-11T15:11:22Z,2026-08-11T15:17:38Z)` as a degraded subinterval rather than deleting fills from already-resting orders. The original AWS `logs/trades.csv` begins at `2026-06-26T16:52:45.937Z`; earlier fill requests are unsupported by that archive.

Use `logs/trades.csv` as the economic fill table, excluding `trade_type=SYNC_ADJUST` from fill counts and fill-PnL analysis. Use `logs/order_outcomes.csv` for order identity, age, reason, target, and lifecycle context. Do not sum a per-row `realized_pnl` compatibility field as incremental reward.

## Historical archives

The original-AWS and intermediate-Vultr archives remain exactly as verified in their dated migration receipts. The reactivated-AWS predecessor archive is resolved through `<retired-reactivated-aws-archive>` and contains the stopped logs, formal collection, and sanitized runtime source admitted during the 2026-08-19 cutover. Its private manifests and verification receipts, not this public summary, are the byte authority.

Credentials, process environments, SSH material, and secret-bearing rollback tarballs are not public archive artifacts. A retired host's local archive is the normal query surface even when the cloud instance temporarily remains available for rollback. Cloud-instance termination and EIP release are control-plane cleanup events; they do not transfer current authority back to a predecessor.

## Evidence reuse after migration

Do not infer the current host's resource class from an older public migration receipt. Current hardware and process identity are private-pointer facts that must be revalidated before operational work. Equal application bytes across two epochs do not make resource pressure, network transport, kernel scheduling, WebSocket stalls, or REST latency identical.

Historical market, live, fill, and latency evidence may be reused only with its original provider, host/instance or logical host identity, capture/runtime epoch, profile hash, and availability boundary. Original AWS, Vultr, reactivated AWS, and current AWS are four distinct source strata. Historical latency profiles are sensitivities; current-host transport claims require measurements bound to the current host and current runtime/config epoch.

## Receive-time capture and F04 boundary

The immutable legacy ledger is `${NARROWGATE_DATA_ROOT}/receive_time_tape/capture_ledger.v1.jsonl`; the source-bound ledger is `${NARROWGATE_DATA_ROOT}/receive_time_tape/capture_ledger.v2.jsonl`. The completed program contains 31 valid full windows over 30 distinct UTC days: 21 original-AWS days, one intermediate-Vultr day, and eight reactivated-AWS days. A duplicate UTC day counts once; captures shorter than 3,500 seconds remain diagnostic.

The collection automation `collect-vultr-tokyo-bounded-market-tapes` is `COMPLETED_DELETED`. Its closed source is `<retired-reactivated-aws-epoch>`. The current host has no inherited capture authority, and the old automation must not be recreated, resumed, or silently pointed at the successor host.

Reaching 30 distinct days closes only the collection-count gate for `first_add_external_incremental_value_m0_m1_v1`. M0/M1 is still blocked until the unknown-submit-ACK lifecycle successor is deployed, a new exact-lifecycle session is admitted, and every chronological, side-specific, Grade-A/Grade-B, common-row, true-LOO, causal-clock, source-transport, and late-panel gate passes. The frozen target, M0/M1 definitions, prediction-only permission, and no-action/no-live boundary remain unchanged.

The frozen F04 transport-source amendment v2 used `<current-live-epoch>` to mean the reactivated-AWS host that was current when the amendment was written. That token is now a historical source label, not authorization to relabel successor-host rows. A new host capture stratum would require a new amendment and explicit owner authorization.

## Current operational identity

The mutable [`operational_baseline_current.json`](../research/families/f10_live_replay_attribution/docs/operational_baseline_current.json) resolves the public [`operational_baseline_identity_20260825_v13`](../research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260825_v13.json). v13 is a governance locator, not a source of live authority. Its `current_live` binding summarizes the private owner-active BUY E3 release-v3 with BUY E3 enabled, the SELL owner policy unchanged, and no active shadow or companion. Its `backtest_default` binding separately retains immutable [`operational_baseline_identity_20260820_v12`](../research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260820_v12.json) and the create-only v12 private config until an exact BUY E3 replay baseline exists. The current live alias cannot replace that replay control.

The exact current endpoint, instance, epoch, host-key pin, archive locators, current process status, and lifecycle admission receipts live only in `${NARROWGATE_LIVE_REMOTE_POINTER}` and the private catalogs. Public automation and analysis code must resolve that pointer; public prose and v13 never grant remote-control authority. The frozen post-lifecycle capture is explicitly not latest-liveness evidence, and neither v13 nor the live E3 chain grants economic or backtest authority.
