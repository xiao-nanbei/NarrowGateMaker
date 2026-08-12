# Prospective Lifecycle Remote Release Orchestrator v1

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: `implemented_local_not_executed`

## Scope

The orchestrator consumes the already frozen 19-file prospective lifecycle narrow release and the exact remote v9 baseline identity. It adds no strategy, model, P3, Feature DAG, C++, or live configuration change. In particular:

- q90 shadow remains enabled and q90 action remains disabled;
- BUY fill-selection shadow and action remain disabled;
- all existing strategy parameters remain byte-identical to v9;
- `make deploy` is forbidden;
- no SSH, upload, deployment, restart, or rollback was executed by this work.

The supplied read-only evidence confirms the v9 process and code identity, but also finds PyArrow 21.0.0 in the active environment. This is recorded as a release blocker, not normalized into a pass. The active environment must not be upgraded in place. Isolated staging instead requires a new content-addressed successor venv built from a hash-locked wheelhouse containing PyArrow 24.0.0. The known native extension is bound to `343d92127a80cb6fefe10cddd21e70f8c1bf22674c19874c7fb0971d052b45f0`.

The current source tree has already moved beyond the uploaded release: `execution/order_lifecycle_journal_v2.py` and `execution/order_lifecycle_journal_writer_v2.py` no longer match their payload hashes. That staging tree is therefore import/runtime diagnostic evidence only. Before deployment, `rebuild-validate` must create and validate a fresh deterministic release, and the caller must rerun the orchestrator with the rebuilt exact `release_manifest.json`. An old manifest cannot deploy merely because it is internally self-consistent.

The isolated path is `.releases/prospective_lifecycle_journal_v2_20260805`; its PyArrow 24 import smoke is useful diagnostics. It is not a passed staging gate because only the `df47525c` manifest-file hash prefix is available, the exact successor-venv path is not bound, targeted lifecycle tests are not bound, and the payload is already stale.

Implementation: `scripts/orchestrate_prospective_lifecycle_remote_release.py`

Tests: `tests/test_orchestrate_prospective_lifecycle_remote_release.py`

Machine contract: `prospective_lifecycle_remote_release_orchestrator_v1_contract_20260805.json`

## Stage Boundary

The default invocation is a dry-run plan. Each later stage has a distinct authorization boundary:

| Stage | Required execution flag | Production mutation |
|---|---|---:|
| local rebuild and validation | `--execute-local-rebuild` | no |
| runtime evidence | `--execute-read-only-ssh` | no |
| isolated upload/import validation | `--execute-isolated-stage` | no |
| deploy/restart | `--execute-production-mutation` plus exact owner token | yes |
| one-hour performance collection | `--execute-read-only-ssh` | no |
| evidence admission | `--execute-admission` | no |
| rollback drill | `--execute-production-mutation` plus separate owner token | yes |

Deploy/restart and rollback use different owner tokens. Both tokens bind the stage name, release manifest SHA256, remote-v9 identity SHA256, and v9 baseline ID. A token for one stage cannot authorize the other.

The isolated stage remains fail-closed until both `--successor-requirements-lock` and `--successor-wheelhouse` are supplied. The lock must contain `pyarrow==24.0.0` with a SHA256 requirement hash. The active `.venv-active` path is never a pip installation target.

## Fail-Closed Gates

The machine evaluator requires Python 3.12.13, PyArrow 24.0.0, a loaded native extension path and SHA256, remote runtime hashes, staged import/test success, all 13 initial-state domains, and zero pre-epoch native events.

The bounded performance window must last 3500-3700 seconds and satisfy zero drops/errors, producer enqueue p99 at most 100 microseconds, enqueue maximum at most 1000 microseconds, quote-loop p99 regression at most 5%, writer queue HWM at most 2048, writer CPU at most 10% of one core, RSS increase at most 256 MiB, writer p99 at most 250 ms, and zero maker-thread filesystem calls. Bounded spool roundtrip admission and rollback restart rehearsal are also required.

The pre-deploy process baseline binds PID 1798225 at 98692 seconds elapsed, 14.6% whole-process CPU, 334652 KiB RSS, and 992184 KiB VSZ. Candidate RSS is compared with that raw RSS baseline and must increase by no more than 256 MiB. Writer-thread CPU and whole-process CPU remain separate fields: the writer has the 10% of one-core hard gate; whole-process CPU is reported as transport diagnostics rather than being mislabeled as writer cost.

Quote-loop regression is anchored to the v9 one-hour telemetry window ending at `1785896266.426`, with 601 requote rows and `requote_total_us` p99 of `141384.284` microseconds. Candidate p99 must be no more than `148453.4982` microseconds. The evaluator recomputes the regression from the two raw p99 values and rejects any changed baseline-window identity; it never trusts a supplied percentage.

Evidence admission is content-addressed and atomic. It rejects duplicate stage records, release/identity hash drift, incomplete gates, symlinks, partial publication, and an existing destination.

## Current Permission

This implementation creates only a release plan and validation machinery. It does not grant research, action, deployment, or live-policy authority. Actual production mutation remains blocked until the exact prior-stage receipts are provided and the owner explicitly confirms that individual stage.

## Verification

- orchestrator tests: `24 passed`;
- Ruff: passed;
- Python compilation: passed;
- default dry-run plan SHA256: `ba5542f434da4c9651b39f0fbeb62c6d7f355731aead140446c3a256ca35607c`;
- default plan reports `source_payload_current=false`, `pyarrow_version_mismatch=true`, and `deployment_executed=false`;
- no SSH, rsync, deploy, restart, performance wait, admission, or rollback was executed.

The prior builder/preflight tests now fail at their frozen source hashes because F07 changed the journal schema/writer payload. This is the intended fail-closed signal for rebuilding the release, not a reason to alter the old manifest or reinterpret the uploaded staging tree.

## Added Files

- `scripts/orchestrate_prospective_lifecycle_remote_release.py`
- `tests/test_orchestrate_prospective_lifecycle_remote_release.py`
- `prospective_lifecycle_remote_release_orchestrator_v1_contract_20260805.json`
- `prospective_lifecycle_remote_runtime_evidence_20260805.json`
- `prospective_lifecycle_remote_isolated_stage_evidence_20260805.json`
- `prospective_lifecycle_v9_quote_loop_baseline_telemetry_20260805.json`
- this implementation report
