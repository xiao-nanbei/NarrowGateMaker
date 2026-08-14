# F05 Offline Formal-V13 Interruption And Shared-Prefix Replay Amendment

Last materially modified: 2026-08-14

Status: `formal_v13_one_day_passed_nested_oof_interrupted_no_evidence_admitted`.

Evidence availability: this report and its repository-relative JSON receipt are public. The formal execution manifest, preflight, one-day receipt, interrupted progress records, replay cache, mechanics tables, source receipts, owner artifacts, and market data remain in the owner-private evidence store and are not distributed with the public repository; their SHA256 values are public integrity metadata only.

## Formal-V13 Result

Formal-v13 was bound from the clean fill-trace v4 tag and passed schema/provenance preflight with status `formal_offline_replay_mechanics_ready`. Its fixed one-worker `2026-06-27` mechanics gate passed all 81 opportunities: 81 exact-owner no-op comparisons matched, all 81 paths reached complete washout, no path was right-censored, and the frozen owner actions were 54 `CONTROL_85N`, 8 `FIXED_1748S`, and 19 `FIXED_211S`. The gate computed path values only as an implementation necessity; it persisted no economic values and used none for selection.

The subsequent nested chronological OOF run was started and then deliberately stopped after the old one-shot adapter was shown to replay every opportunity-duration arm from the beginning of its day. Ten BUY outer-train day jobs had only `state=running` progress records when stopped. Formal-v13 admitted zero cache entries, labels, candidate policies, outer-test results, scorecards, or economic evidence. No interrupted value was exposed to candidate selection or research interpretation.

## Why The Executor Changed

The old adapter was semantically conservative but computationally unusable: it repeated the identical exact-owner prefix for every opportunity and every duration arm. The replacement executes one exact-owner parent path per day, forks from the identical in-memory state at each frozen target fill, and runs only the divergent duration continuation. Per-opportunity shards are admitted atomically, completed shards are resumable, and stale staging is quarantined before recomputation.

The first resumable implementation was not used for formal evidence because its day finalizer counted only arms newly completed by the current process and could reject a valid mixture of resumed and new shards. That run was stopped after four mechanics days, before labels or economics. The corrected v2 finalizer requires `resumed + newly dispatched = frozen target count` and separately validates newly completed arm accounting and the full admitted manifest.

## Evidence Boundary

This amendment does not reinterpret formal-v13 as a completed OOF run. It preserves the clean preflight and one-day mechanics evidence, records the interrupted nested run as non-admitted computation, and moves the next formal identity to v14. Validation and sealed holdout remain unread; action and live authority remain false. The active owner policy, private live configuration, EC2 runtime, quote prices, sizes, BER, P3, q90 action, inventory limits, and lifecycle behavior are unchanged.

No F05 companion, observer, writer, journal, feature dump, candidate telemetry, deployment, restart, or live configuration change was created. The Unknown ACK lifecycle amendment remains a separate `implemented_local_predeploy_blocked` correctness line.

## Next Gate

The shared-prefix implementation must first reproduce and admit the immutable 30-day, 3,516-opportunity mechanics denominator. A new clean admission tag may then bind formal-v14. Formal-v14 must independently pass preflight and the fixed one-day gate before it may run nested chronological OOF; no formal-v13 cache or progress record may be promoted or reused as economic evidence.
