# Formal Execution Attempt And Evidence Freeze Contract V1

Last materially modified: 2026-08-21

Status: current project governance contract.

Question: when may a research executor receive a clean SHA-bound formal identity, and when does a failed run justify a new research version?

Result: executor stabilization precedes evidence freeze. Ordinary implementation failures create new execution attempts under the same research identity; they do not create research `vXX` identities. A new research identity is required only when the frozen sample, baseline or candidate ladder, folds, estimand, or statistical contract changes.

Evidence boundary: this contract governs future NarrowGate formal research and normalizes current F05 terminology without rewriting historical commits, tags, manifests, or failure receipts. The executable validator is `models/audit/formal_evidence_governance.py`, with regression coverage in `tests/test_formal_evidence_governance.py`.

## Development Before Freeze

Executor fixes and performance work remain on a development branch until representative single-day output, an all-fold zero-economic walk, concurrency and cache durability, regression and parity checks, and a complete output-shape smoke run all pass. These gates must exercise the intended worker topology, mmap and resource lifetime, interruption and resume, atomic cache replacement, result aggregation, scorecard generation, and receipt serialization without exposing intermediate economic values.

Only the exact tested clean source may receive an annotated execution tag and pre-run manifest. The manifest binds one execution attempt to the unchanged research identity, research-contract hashes, clean Git commit and tree, tag, source artifacts, runtime configuration, cache namespace, output schema, and permissions. It cannot grant action or live authority.

## Identity Layers

The research identity names the scientific question. Its immutable contract is the sample, baseline and candidate ladder, folds, estimand, and statistical method. The execution attempt names one admitted implementation run. Git commit and tree identify tracked source; an annotated tag names admitted source; the pre-run manifest binds source, inputs, runtime, and permissions; artifact SHA256 values identify external and private bytes; the final receipt binds completed result SHA256 values back to the pre-run manifest.

A result receipt never mutates its pre-run manifest. A source change requires a new attempt manifest because the executed bytes changed, but it does not change the research identity when the research contract is byte-identical.

## Failed Attempts

An unexpected implementation bug, crash, cache mismatch, concurrency race, serialization error, or ordinary performance repair produces an immutable failed-attempt receipt. That attempt is ineligible for economic inference. The repair returns to the development line, repeats every stability gate, and receives a new `attempt-*` identity only after it passes. Partial strategy-dependent caches and partial economic outputs are not imported unless a separately validated cache contract proves exact semantic identity.

Historical failed tags and receipts remain provenance. They are not deleted, renamed, or rewritten. Their names do not define research versions.
