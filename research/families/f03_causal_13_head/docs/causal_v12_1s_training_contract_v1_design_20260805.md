# Causal-v12 Canonical 1s Training Contract v1

Last materially modified: 2026-08-05

Status: `training_mechanics_frozen_execution_blocked`

This contract advances only the canonical 1-second successor. It does not pre-commit 2-second or 5-second candidates, does not change any of the 13 label estimands, and does not read prediction or economic outcomes.

## Frozen Membership

The successor inherits the exact hashed v12 train-only membership:

- 52 chronological fit days;
- 1 embargo day (`2025-11-23`);
- 13 chronological inner-selection days;
- all 66 days for the post-selection refit.

Previously read 2026 native panels remain historical diagnostics. They cannot be renamed as independent confirmation evidence for this successor.

## Overlapping Labels

At a 1-second decision cadence the future intervals overlap. For each head and UTC day, a valid row receives the average reciprocal concurrency over:

```text
[decision_ts, decision_ts + maximum_future_dependency_s)
```

That uniqueness factor multiplies the inherited base sample weight and is normalized within the UTC-day/head cell to preserve the valid cell's total base-weight scale. The inherited base remains `exp(-0.1 * days_ago / 30.44)` with the v12 manifest's frozen reference date `2026-07-23`; changing that reference date would be a second intervention. Invalid or censored labels receive zero weight. Evaluation and uncertainty remain clustered chronologically by UTC day; 1-second rows are not treated as independent evidence.

## Execution Blockers

The generic source manifest, 1-second label identity, and synthetic Python/C++ parity contract are now hash-bound. Training remains fail-closed until a full physical 1-second daily panel manifest passes real-day Python/C++ fingerprint parity and the training-code and model-output identities are bound. The two-row real-source probe is not a substitute for that daily artifact. This design does not authorize model training, action changes, or live deployment.
