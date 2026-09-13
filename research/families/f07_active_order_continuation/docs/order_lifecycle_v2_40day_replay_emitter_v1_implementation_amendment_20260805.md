# F07 Journal-v2 40-Day Replay Emitter Implementation Amendment v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`implemented_and_fixture_verified_not_executed`

The F07 execution-only emitter is implemented at `research/families/f07_active_order_continuation/audit/order_lifecycle_v2_40day_replay_emitter.py`. It retains the frozen v1 ordered 40-day denominator and the v1.2 journal-v2 authority. No real 40-day replay, lockstep, CIF training, economic evaluation, q90 action, transport audit, or live deployment was started.

The machine-readable amendment is `order_lifecycle_v2_40day_replay_emitter_v1_implementation_amendment_20260805.json`, canonical SHA256 `63a623e4233c42189efc8f3933f6d239491952bc189eec8f09d362d342215dce`.

## Execution Shape

The emitter has two public phases:

1. `prepare` resolves and hashes every frozen input into an explicit execution plan. It does not run a replay.
2. `run` executes one isolated child process per requested UTC day. Every child starts from a fresh strategy state, invokes the existing authoritative Python tick replay, and emits only journal-v2 mechanics.

Every day writes beneath a unique staging directory. The parent validates the journal, writer health, identity bindings, part manifests, and Parquet payload hashes before publishing the directory with `os.replace`. A completed day is resumed only when its full day identity and artifacts still validate. A panel manifest is published only after all 40 frozen days are present in their original order.

## Identity

The execution plan binds SHA256 identities for:

- the frozen v1 contract and source replay contract;
- each day's window cache and D-1/target native book files;
- operational config, model bundle, P3 artifact, and Feature DAG;
- replay, lifecycle, writer, adapter, and supporting runtime code;
- compiled C++ module and event-stream ABI;
- latency profile and queue calibration.

Any source, runtime, or ABI drift fails closed before the day is admitted.

## Journal Gates

Each day manifest carries:

- visibility-time and exchange-time coverage counters;
- writer rows, callbacks, drops, errors, close state, and formal-valid state;
- lifecycle/event/terminal counts and terminal reasons;
- cancel-reject continuation counts for `ACTIVE` and `PARTIALLY_FILLED`;
- sub-lot partial remainder and full-fill exact-zero counts;
- content identities for runtime metadata, health, part manifests, and Parquet payloads.

Writer drops and errors must be zero. Missing or reversed exchange clocks, invalid exchange exposure, terminal cardinality drift, or a positive remainder on full fill rejects the day.

## Economic Firewall

The runner disables decision, quote, fill, first-add, and first-opener economic traces. It selects only `_order_lifecycle_journal_v2_health` from the replay result and then discards that result. Persisted plan and manifest trees reject keys containing `pnl`, `reward`, `markout`, `campaign_economic`, `profit`, or `value_usdc`. Replay stdout and stderr are not persisted.

This implementation does not read or write PnL, reward, markout, or campaign economics.

## Fixture Verification

The new mock-runner test covers:

- the exact frozen 40-day denominator;
- worker command construction and explicit staging;
- source/global identity propagation;
- parent-directory date classification for D-1 and target native tapes;
- atomic day publication and identity-safe resume;
- rejection of mutated journal payloads;
- cleanup after worker failure;
- fail-closed rejection of PnL, reward, markout, and campaign-economic fields.

No fixture invokes the authoritative market replay.

## Permissions

```text
formal_40day_replay_execution = false
formal_40day_lockstep = false
cif_training = false
economic_evaluation = false
q90_action = false
prospective_live_epoch_transport = false
live_deployment = false
```
