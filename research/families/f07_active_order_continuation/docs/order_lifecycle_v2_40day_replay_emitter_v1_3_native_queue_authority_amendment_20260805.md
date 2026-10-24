# F07 Lifecycle-v2 40-Day Replay Emitter v1.3

Last materially modified: 2026-08-05

Status: native queue-authority split implemented; formal 40-day replay not yet executed.

The v1.2 worker reached authoritative input admission and stopped before tick replay because the composed v13 window was required to carry `exact_queue_policy_eligible=true`. That window is the market-context node: it provides trades, rolling state, and the operational ML overlay. It is not the queue-truth node. No lifecycle row or economic outcome was admitted.

v1.3 keeps formal lifecycle eligibility on the market-context window and binds exact queue authority exclusively to the separate native snapshot/delta tape. Every day must resolve exactly 24 D-1 warmup files and 24 target-day files in the frozen order, with path, byte size, and SHA256 matching the plan. Queue mode must remain `strict`, and inferred L2 cancel-ahead remains disabled. Missing, duplicated, reordered, or mutated native inputs fail closed.

This is a DAG authority correction, not a relaxation of queue evidence. The 40-day denominator, q90 action-OFF baseline, lifecycle schema, and mechanics-only economic firewall remain unchanged. A new execution plan is required because the runner implementation hash changed.
