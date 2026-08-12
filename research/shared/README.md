# Shared Research Infrastructure

Last materially modified: 2026-07-28

Shared code stays in its runtime-owned package. These directories are ownership indexes, not duplicate implementations. A research family may depend on a shared layer but must not copy it into the family directory.

- `data_identity/`: market-data identity and good-day admission.
- `replay_lifecycle/`: authoritative replay, queue, and lifecycle.
- `strategy_semantics/`: live/replay quote and policy semantics.
- `experiment_governance/`: manifests, splits, scorecards, OPE, and promotion.
