# Volatility-Time Add Rearm C++ q90 Parity v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

Development mechanics parity passes. F09 may now register, but has not yet created or authorized, a new lineage-randomized variance-time replay identity. This result does not authorize reward/PnL evaluation, Validation, sealed holdout, shadow, or live deployment.

The authoritative report is [`report.json`](${NARROWGATE_RETIRED_DATA_ROOT}/reports/volatility_time_add_rearm_cpp_q90_parity_v1_20260729/development/report.json), with SHA256 `e3e792f5fdb5b534615fa11cbe1ed8ccb986eb5948c7c9c698637897d872fc15`. The independent machine-readable audit is [`volatility_time_add_rearm_cpp_q90_parity_v1_postrun_audit_20260729.json`](volatility_time_add_rearm_cpp_q90_parity_v1_postrun_audit_20260729.json).

## What Was Tested

The Python full-path replay remained authoritative. A strict lockstep C++ kernel consumed every raw snapshot/delta event and independently maintained:

- sequence validity and native deep-book state;
- exact-level activation, queue-ahead, cancel and refill path;
- the frozen BUY q90 model and policy artifact;
- cancel request, ACK-before-fill race, recovery, queue reset, and re-entry.

The explicit 85-second-per-fill-unit control and variance-time candidate were both replayed on 2026-04-17 and 2026-06-21. These days were selected from the already frozen Development checkpoints for mechanism coverage only. Neither economic outcomes nor later panels were used for selection or evaluation.

## Parity Result

| Day | Arm | Book events | Evaluations | Activations | Lifecycle calls | Cancel/ACK | Recovery | Re-entry | Mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-04-17 | variance time | 3,000,262 | 1,932,180 | 8,396 | 16,318 | 1/1 | 0 | 1 | 0 |
| 2026-04-17 | wall time | 3,000,262 | 1,858,447 | 8,077 | 15,960 | 1/1 | 0 | 1 | 0 |
| 2026-06-21 | variance time | 4,545,351 | 1,600,686 | 5,860 | 11,865 | 9/9 | 6 | 9 | 0 |
| 2026-06-21 | wall time | 4,545,351 | 1,763,534 | 6,400 | 12,887 | 3/3 | 2 | 3 | 0 |

Totals were 15,091,226 raw book-message consumptions, 7,154,847 q90 evaluation calls, 28,733 activations, and 57,030 lifecycle calls. All event, state, probability, action, counter, and sequence comparisons matched.

The actual source manifest contains 104 hash-bound files totaling 841,092,709 bytes: 96 native snapshot/delta files, four normalized BBO files, and four normalized L2 files. Its canonical identity is `d0f25c9b0f51668d8f5d80417a0f7e1b10b2b887fbd864a8c648e8bac9586a70`.

## Boundary And Coverage

This is native-book and BUY-q90 kernel parity inside Python-authoritative replay. It is not full C++ tick-replay authority. Every output row and the report retain `full_cpp_tick_replay_authority=false`.

The 40 frozen predecessor days contained no real q90 fill between cancel request and ACK. That branch is therefore covered by the frozen synthetic contract test `test_q90_cancel_pending_fill_recovery_ack_and_queue_reset`, not claimed as historical market-path evidence. The 41-test frozen contract suite passed, and the complete Python suite completed with 1,002 passes and four skips.

Two implementation defects were found and fixed before the formal run:

- each native level change now retains its source message receive timestamp;
- C++ rejects BBO state whose receive timestamp is not yet causally visible.

## Permissions

The formal artifacts explicitly retain:

- `reward_or_pnl_read=false`;
- `markout_read=false`;
- `validation_read=false`;
- `sealed_holdout_read=false`;
- `randomized_action_identity_created=false`;
- `action_experiment_authorized=false`;
- `live_deployment_authorized=false`;
- `aws_receive_time_transport_supported=false`.

## Next Boundary

The two pre-registration blockers are now removed: the candidate regenerates its own full Python path, and native-book BUY q90 has strict event-level C++ parity. The next permissible identity is `volatility_time_add_rearm_randomized_replay_v1`, randomized once per same-side cooldown lineage with known propensity. Its Development economics must remain separate from this mechanics identity. AWS receive-time remains a later transport and live-authorization gate.
