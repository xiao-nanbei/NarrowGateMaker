# Local maintenance and file index

[English](maintenance.md) | [简体中文](maintenance.zh-CN.md)

Last materially modified: 2026-10-24

Last materially synchronized: 2026-10-24

`narrowgate maintenance` owns one bounded local cleanup pass and the existing offline file index. It is not another scheduler: it does not submit research, restart processes, edit PoolRun state, access remote hosts or operate live. Physical roots and receipts remain in a private configuration directory, not in published source.

## Commands

```bash
export NARROWGATE_MAINTENANCE_CONFIG="<private-maintenance-config.json>"
python3 -m narrowgate maintenance scan
python3 -m narrowgate maintenance run --apply
python3 -m narrowgate maintenance index
```

`scan` and `run` without `--apply` are previews. They read file metadata and existing completion authority but do not delete source files or rebuild the full index. `run --apply` removes only currently verified terminal runtime locks; a real removal triggers one combined index refresh. `index` explicitly rebuilds the metadata view without deleting source files. `--config` may replace the environment variable. Missing volumes, inconsistent identities and unavailable open-file inspection do not authorize deletion.

The private JSON contains `roots` (objects with absolute `path` and unique display `label`), `pool_state` (the existing authoritative PoolRun JSON), `index_directory` (the existing generated review directory) and optional absolute `excluded_directories`. Roots must not overlap. Exclusions affect cleanup; the read-only index still describes the configured workspaces, excluding its own generated directory, symlinks and every `.git` entry. Restrict roots to project-owned workspaces, not a whole operating-system disk or home directory.

## What is automatically removable?

| Object | Required evidence |
| --- | --- |
| Empty `runner.lock` inside an original Attempt directory | Matching terminal Task/Attempt, original exit receipt confirming the process tree exited, original runner and child PIDs absent, no open file in the Attempt directory, nonblocking exclusive lock and unchanged file identity |
| Empty account `execution.lock` | Exact COMPLETE Task/Attempt result identity, matching economic completion and terminal durable cut, lock excluded from formal payloads, no unfinished/held/recovery identity or open file in that account directory, exclusive lock and unchanged file identity |

Only single-link regular files are eligible. A changed PoolRun snapshot stops deletion of remaining candidates. Required result files, identities, receipts and payloads are not removed or rehashed. This is lock cleanup, not certification that an entire historical archive is disposable.

An unlocked lock file is not generally garbage: a new writer or waiter can already hold its inode open. Therefore persistent controller/Agent, preparation, publication, result-retirement and unknown-role locks stay in place. Dependency lockfiles also remain. File age, zero size, missing PID metadata or a filename alone is never deletion evidence. A reused PID is conservatively retained. No process is killed to make cleanup pass. New cleanup classes must be implemented through their actual owner with dependency and concurrency tests; do not add a wildcard age-based deletion rule.

## Results, indexing and scheduling

The current private `maintenance-latest.json` records removed/retained paths and reasons. `maintenance-last-change.json` keeps the last actual removal. Empty runtime locks normally reclaim file entries, not meaningful payload capacity; logical removed bytes are reported separately. A persistent `.maintenance.lock` prevents concurrent maintenance and is itself intentionally retained.

The file index retains its JSONL inventory, Chinese HTML pages and offline keyword search. It scans metadata, not market payload contents, and displays lock retention reasons. Search filters use every configured volume label. Historical cleanup fields remain historical; index categories are not deletion permission. If index generation fails, successful removals remain recorded and the next applied pass retries the pending index without replaying those removals. No-change passes do not rebuild the full index.

An operator-authorized hourly heartbeat can invoke the same `run --apply` command. Keep it quiet on an unchanged result and notify only after actual cleanup or a failure. It must not change rules, expand roots, clean live data or start research. There is no extra long-running daemon, second file catalog or automatic experiment promotion.

See [module ownership](architecture.md), [data lifecycle](../data/README.md) and the [archive standard](../research/governance/EXPERIMENT_ARCHIVE.md) for larger cache/checkpoint retirement. Those owners remain responsible for their payloads.
