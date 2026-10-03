# Formal Execution Attempt And Evidence Freeze Contract V1

Last materially modified: 2026-09-28

Status: current project governance contract.

Question: which semantic changes define a new research identity, and which checks establish that an execution attempt is usable?

Result: executor stabilization precedes evidence freeze. Ordinary implementation failures create new execution attempts under the same research identity; they do not create research `vXX` identities. A new research identity is required only when the frozen sample, baseline or candidate ladder, folds, estimand, or statistical contract changes.

Evidence boundary: experiment-specific producers and readers validate input support, schema, units, clocks, feature order, accounting and runtime capabilities. Source and result provenance remains available without a generic hash-bound authorization receipt. Original results and failure records retain their actual experiment scope.

## Development Before Freeze

Executor fixes and performance work remain on a development branch until representative single-day output, an all-fold zero-economic walk, concurrency and cache durability, regression and parity checks, and a complete output-shape smoke run all pass. These gates must exercise the intended worker topology, mmap and resource lifetime, interruption and resume, atomic cache replacement, result aggregation, scorecard generation, and receipt serialization without exposing intermediate economic values.

The pre-run manifest names the execution attempt and records its research identity, actual parameters, input support, cache versions, output schema and permissions. Source commit and build information are provenance. They do not replace semantic checks or grant action or live authority.

## Identity Layers

The research identity names the scientific question: sample, baseline and candidate ladder, folds, estimand and statistical method. The execution attempt names a run under that contract. Checksums protect irreplaceable bytes at transfer or archival ingestion boundaries; local reconstructible caches and checkpoints use atomic publication, schema and semantic validation.

A result receipt records the completed outputs without changing the pre-run experiment conditions. Comments, source layout and same-ABI recompilation do not invalidate compatible artifacts or require retraining. Semantic compatibility, not byte identity, determines reuse.

## Failed Attempts

An unexpected implementation bug, crash, cache mismatch, concurrency race, serialization error, or ordinary performance repair produces an immutable failed-attempt receipt. That attempt is ineligible for economic inference. The repair returns to the development line, repeats every stability gate, and receives a new `attempt-*` identity only after it passes. Partial strategy-dependent caches and partial economic outputs are not imported unless a separately validated cache contract proves exact semantic identity.

Historical failed tags and receipts remain provenance. They are not deleted, renamed, or rewritten. Their names do not define research versions.
