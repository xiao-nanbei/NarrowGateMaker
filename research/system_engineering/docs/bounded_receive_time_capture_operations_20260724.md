# Bounded Receive-Time Capture Operations

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

Last materially modified: 2026-08-11

Status: Current operations guide; the dated migration result below is historical evidence.

## Scope

This workflow collects system and market-data evidence only. It must not create, enable, or test a strategy arm.

The old daily task stayed attached to a 3,600-second recorder process, opened one transfer session per tape, and then repeated gzip/SHA/event validation in the same foreground run. A normal collection therefore occupied the task for more than one hour, while a Tokyo-to-local backlog could add tens of minutes.

## Two-stage workflow

The capture lifecycle is now deliberately asynchronous:

1. `collect --background` starts a detached local supervisor and returns immediately.
2. The supervisor checks the current deployment-host capture lock and starts the 3,600-second recorder with `nohup` when the current UTC day has no completed full window.
3. The remote recorder disables every recording flag after the bounded window, writes one self-contained v2 summary, and releases the lock.
4. The supervisor polls the lightweight status endpoint and transfers marker files once plus all seven tape paths in one `rsync --files-from` session.
5. The background validator streams each local gzip once, verifies remote and local SHA256/event counts, checks queue drops and strategy-hash invariance, appends the ledger under a file lock, and only then removes remote payloads.

The status command is read-only:

```bash
.venv/bin/python scripts/bounded_receive_time_capture.py status
```

Current defaults resolve AWS Tokyo `<current-live-ssh-target>` with `provider=aws`, `region=ap-northeast-1`, `city=tokyo` from `NARROWGATE_LIVE_REMOTE` or `docs/private/live_remote.current.local.json`; do not copy either retired host IP from a dated report. The resolver accepts a pointer only when `status=current_active` and refuses mixed remote/source provenance. The original AWS and intermediate Vultr endpoints have no capture, write, deletion, or admission authority.

The Codex heartbeat automation remains `PAUSED` after cutover. Do not run the normal cycle below through automation until its prompt/source identity is updated to the current AWS epoch and the owner explicitly resumes it.

Run the normal end-to-end daily cycle:

```bash
.venv/bin/python scripts/bounded_receive_time_capture.py collect \
  --background \
  --duration-s 3600
```

The lower-level commands remain available for recovery:

```bash
.venv/bin/python scripts/bounded_receive_time_capture.py start-remote --duration-s 3600

.venv/bin/python scripts/bounded_receive_time_capture.py sync \
  --background \
  --max-captures 1
```

## Admission gates

A remote capture is eligible for automatic sync only when:

- the summary names exactly seven unique tape paths;
- every remote gzip passed finalization;
- all seven payloads still exist remotely;
- recording flags were disabled after the window.

Local ledger admission additionally requires:

- local gzip parsing and SHA256 match;
- remote/local event-count agreement when the v2 summary provides it;
- unchanged strategy hash between enable and disable markers;
- zero market-tape and external-recorder drops.

Invalid historical remnants are never selected by default. A completed capture already present in the ledger is idempotently skipped. Capture and sync locks prevent duplicate workers.

The AWS `capture_ledger.v1.jsonl` and its admitted original-AWS directories are immutable historical evidence. The admitted intermediate-Vultr capture remains in v2 with prefix `vultr_tokyo_<retired-intermediate-vultr-epoch>_*`. Prospective current-AWS captures must also use v2 but with the host-bound prefix/source key derived from `<current-live-host>`; they must not reuse either historical prefix or mutate v1.

Only captures with a requested or observed duration of at least 3,500 seconds count toward the 30-distinct-UTC-day M0/M1 denominator. Short wiring captures remain valid diagnostics but do not advance that research gate.

The source-aware panel is governed by the outcome-blind [`transport-source amendment`](../../families/f04_external_market_alpha/docs/first_add_external_incremental_value_m0_m1_v1_transport_source_amendment_20260811.md). It preserves AWS rows as a historical stratum and requires Vultr host/runtime provenance. The original F04 target/model/gates remain unchanged and no action or live permission is created.

Routine queries for both retired epochs use their separate verified local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` archives. Neither retired predecessor may be reached by SSH or treated as a remote fallback. See [`docs/live_host_and_historical_data_access_20260811.md`](../../../docs/live_host_and_historical_data_access_20260811.md).

## 2026-07-24 migration result

The pending `20260722T171913Z` window was migrated through the new path:

- 7 unique gzip tapes;
- 2,542,341 events;
- 34 maker fills;
- 60 HEALTH rows;
- zero recorder queue drops;
- unchanged strategy hash;
- local SHA256 and event counts matched for every tape.

After ledger admission, all seven remote payloads were removed. The ledger then contained nine distinct valid full-window UTC days, still below the 30-day M0/M1 threshold.
