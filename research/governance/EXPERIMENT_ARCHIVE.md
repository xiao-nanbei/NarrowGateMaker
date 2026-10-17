# Minimal experiment archive standard

[English](EXPERIMENT_ARCHIVE.md) | [简体中文](EXPERIMENT_ARCHIVE.zh-CN.md)

Last materially modified: 2026-10-17

Last materially synchronized: 2026-10-17

Status: Current retention and closure standard. This grants no execution, publication or deletion authority and does not weaken frozen contracts.

## Minimum retained evidence

Retain the smallest set that explains the question, identifies the actual run, allows the reported accounting/comparison to be recomputed and supports its limitations. Theoretical reproducibility is not grounds to delete unique results, model bytes or original inputs. A digest without an accessible retained object is not a retained dependency. Size, age, a `COMPLETE` label or a filename is not deletion evidence.

| Role | Required evidence, in this archive or through a verified canonical reference |
| --- | --- |
| Contract | Original question, exact baseline/arms, account calendar/initialization, split/previous-use, seeds, stop decision, metrics/statistics and complete effective configuration, including authorized changes |
| Provenance | Actual source snapshot or recoverable commit plus dirty/private overlay; runtime/dependency/native identities; input/model/calibration/restoration bindings. Training also retains support, weighting, selection/refit configuration, model artifacts and acceptance |
| Results | Original per-account formal results, accounting, daily ledger/equity increments, fees/funding/terminal MTM, fills and risk/action diagnostics required by the contract; paired comparison, coverage and missingness; partial/failed evidence explaining exclusions |
| Acceptance | Formal/durable identity/completion checks, original task/Attempt and exit receipts, real recovery/parity tests, unique failure diagnosis and repair evidence; not just screenshots or scheduler counts |
| Conclusion | Readable findings with units/denominators, planned versus actual coverage, losses/uncertainty, historical-use/model/simulator limitations, promotion/non-promotion decision and closure receipt |

There is no blanket “keep only accounting and summary” rule. If a formal result embeds fills/diagnostics, keep its original bytes unless a separately authorized lossless conversion preserves required evidence, identity mapping and reader compatibility. Preserve explicitly pinned parity evidence even when it resembles a checkpoint. Storage cleanup never rewrites trade outcomes or historical labels.

One experiment has one immutable identity, one research owner and one maintained `archive-manifest.json`. Its existing family catalog points to this manifest; the existing private `experiment-storage.current.json` is the physical-location index, not a second economic authority. Reference existing bound manifests/receipts instead of copying their complete contents or creating per-event inventory records.

## Shared versus disposable material

Raw purchases, facts, accepted observations, funding, models and source snapshots have one canonical owner plus required backups, not a full duplicate per experiment. Each reference records location, original identity, availability check and dependency reason. Reused B0 results retain their original identity. Unknown dependencies block minimal-archive certification, not automatic copying, regeneration or deletion.

Reusable prepare/reference-feature/prediction caches belong to shared versioned data storage, not permanent experiment evidence. Follow the [data workflow](../../.agents/skills/mm-as-glft-ml-spread/references/data-workflow.md): destination/peer reuse first; on the workstation check the external archive; stage bounded account/version copies on the working disk and remove them after the last accepted consumer exits. Keep the owner's required external-disk and Netdisk copies. Cache acceptance compares exact relative names and per-file sizes without rehashing; it is not cryptographic equality. No new hardlinks, symlinks, whole-calendar prefill or external-disk replay.

Do not permanently archive progress spam, duplicate scheduler payloads, obsolete recovery checkpoints, transfer fragments, temporary credentials/tools, compiled caches/build intermediates or finished one-off launchers merely for history. First preserve unique diagnostics and required exit metadata in their existing owner. Filter runtime locks by role; keep dependency lockfiles. A launcher containing the only actual parameter mapping remains evidence until that mapping is retained and verified elsewhere. Explicitly retained historical records are exceptions, not blanket disposable files.

## Layout

New experiments use this layout under their existing private owner and retained storage root; omit empty directories:

```text
<experiment-id>/
  archive-manifest.json       Roles, canonical references, gates and exceptions
  contract/                   Plan, calendar, effective config and identity bindings
  results/<arm>/<account>/    One canonical accepted output per original path
  evidence/                   Acceptance, essential failure/repair and training evidence
  report/                     Conclusion and original comparison tables
```

Shared model/source dependencies need not be duplicated here. Temporary work, staging, scheduler Attempts and transport material remain outside. Existing frozen archives use `layout=legacy_in_place`: map actual paths to the same five roles without renaming frozen paths, copying trees, rewriting results or moving active dependencies. Logical consistency comes first.

## Closure gates and honest status

`economic_status` is independent of storage status: `active`, `closed_complete`, `closed_partial`, `closed_failed` or `unknown`. Owner-stopped research may be archived as partial; storage organization never restores missing coverage or fresh-holdout status.

`archive_status` progresses from `inventoried` to `reviewed_with_exceptions` to `verified_minimal`. The last requires all five roles reviewed (inapplicable roles have reasons), resolved dependencies, no unresolved exceptions and evidence for every gate:

1. **payload_integrity:** verify retained formal outputs and identities under the original contract. Preserve old receipts; append a current check without changing historical counts.
2. **dependency_resolution / restore_read_check:** locate source/model/input/result objects and required backups; read or restore-check them. No new economic replay is needed for storage closure.
3. **writers_exited / no_active_or_recovery_consumers / no_open_files:** confirm original writers exited and proposed removals have no active, queued, recovery, pinned or cross-experiment consumers/open files. Unknown means retain.
4. **scheduler_residue_review:** review experiment-specific Attempt copies, checkpoints, cache blobs, staging/releases, one-off tools and temporary accounts through existing maintenance interfaces. Shared state and supervision metadata are not blanket disposable. Never change scheduler state to make cleanup pass.
5. Remove only the verified disposable/redundant set. Record exact paths/classes, reason, retained counterpart where applicable, logical removed bytes and separately measured free-space change. Recheck retained files and update existing storage/cache/file inventories. Zero-delete review is valid; unperformed review is not.

The validator checks declarations, gates and optional path presence; it does not certify hashes, economics, remote availability, process exit or deletion safety. It has no delete/move command. Schema validity is not evidence acceptance.

```bash
python -m research.governance.experiment_archive inventory <archive-root>
python -m research.governance.experiment_archive init <archive-root> --experiment-id <original-id> --owner <existing-unit> --write
python -m research.governance.experiment_archive validate <archive-root>/archive-manifest.json --check-paths
```

`init` creates a private `inventoried` draft and refuses overwrite. Inventory excludes its own generated manifest, counts logical bytes and never hashes payloads. Review references and exceptions explicitly. Keep physical paths and detailed receipts private under the [publication boundary](../../docs/public_private_documentation_contract.md). Family documentation and the single research state index link to actual conclusions and archive entries; unknown gates prevent claims that all history has been minimized.

## Replay recovery payloads

This archive contract owns checkpoint and scheduler-residue retention: during each active offline replay task/arm, retain only its latest valid recoverable checkpoint and required metadata. Publish a replacement atomically and validate it before removing the previous one; never remove the sole valid recovery point while the task is incomplete or failed. Do not accumulate hourly checkpoint generations. After the task's full account economics, terminal cut and accounting are verified, its worker has exited, and no active, queued or recovery consumer or open file depends on the checkpoint, delete its checkpoint payloads and obsolete checkpoint sidecars without asking again. Preserve formal results, metrics, accounting and completion receipts, model/input bindings, and explicitly pinned research evidence; record the cleanup and measured released space. A PID exit or scheduler COMPLETE alone is not sufficient. This policy concerns disposable replay recovery checkpoints, not trained model checkpoints or unique experimental evidence. Apply cleanup through existing execution/maintenance entry points; do not silently hot-edit frozen economic code or restart active tasks merely to change retention.
