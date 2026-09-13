---
name: mm-as-glft-ml-spread
description: Audit or change NarrowGate quote units, empirical P3 touch semantics, ML-to-quote adapters and order-action propagation. Use for quote-controller research or semantic parity; not routine downloads, repository cleanup or deployment.
metadata:
  short-description: Audit NarrowGate quote semantics and action effects
---

# NarrowGate quote semantics

Last materially modified: 2026-09-19

This skill applies to the source tree containing it. Read the root [AGENTS.md](../../../AGENTS.md) and actual experiment contract; a different remote or historical skill is not a second current rule set. Scope/source facts live in [dataset_scope.json](../../../data/dataset_scope.json), not here. Policy is not acceptance evidence.

## Workflow

1. Identify whether the request is a unit audit, behavior-preserving repair or behavior-changing experiment. Identify execution/reference markets and the exact consumer/configuration.
2. Trace the implemented signal, quote and order paths. Read [quote semantics](references/quote-semantics.md) for units, P3 and proxy terminology.
3. For learning or economics, select the applicable method in [research methods](references/research-methods.md). Do not impose OPE requirements on supervised learning or direct paired simulation.
4. Validate at the layer changed. Preserve safety behavior for an engineering repair; measure actual intervention propagation for a candidate.
5. Report only task-relevant evidence, assumptions, changes, validation and limitations. Implementation parity, predictive skill and economic value are different conclusions.

## Action-effect acceptance

For behavior-changing candidates trace:

signal → decision → raw target quote/action → tick rounding/post-only/risk-filtered action → submitted/kept/cancelled order → fill/inventory path → net PnL/risk.

Report eligible decisions, scored/excluded/missing decisions, changed decisions, changes overridden downstream, actual order requests and terminal executions separately. No-change is a valid result; do not increase coefficients after reading results merely to force an effect. A cancel request is not an accepted or completed cancellation.

Pure refactors, performance work and semantic-equivalence repairs instead prove unchanged outputs/state for the stated inputs; they do not have to manufacture action differences.

## Conditional references

- Input or quality issues: [data workflow](references/data-workflow.md).
- Actual deployment or same-epoch live reproduction: [live workflow](references/live-workflow.md).
- Publication only: [publication protocol](references/publication.md).
- Historical question only: [retired instruction snapshot](references/historical-context.md). It preserves earlier instructions and findings, not current commands, authority or defaults.
