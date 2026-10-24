# SELL First-Fill Conditional Value v1 Invalidation

The v1 identity was stopped before completing the 40-day native Development artifact and before running its evaluator. It has no prediction result.

The producer exposed two registered model features as constants: `local_toxicity=0.5` because ML was disabled, and the opener-only taker-flow field as zero because its causal helper was activated only by the old first-add trace. Its feature-ready timestamps were also submit-time assertions rather than checks against BBO, L2, individual-trade, and queue source clocks.

Nine atomic day checkpoints remain under the v1 output directory as invalid diagnostics. They must not be resumed, pooled into v2, or admitted by an evaluator. A partial three-day target summary was accidentally inspected after the v1 Spec was frozen; no gate or implementation was changed in response, but v1 is invalid on the independent feature-lineage failures in any case.

The successor must use a new identity, trace schema, producer, source-clock contract, and output directory. Validation, holdout, action, and live permissions remain false.
