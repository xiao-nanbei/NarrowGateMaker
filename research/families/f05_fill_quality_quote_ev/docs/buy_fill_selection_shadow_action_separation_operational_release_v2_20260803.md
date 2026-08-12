# BUY Fill-Selection Shadow/Action Separation Operational Release v2

Last materially modified: 2026-08-03

Status: deployed. BUY fill-selection action is OFF; its scorer and shadow log remain ON. causal-v12 remains ON, and q90 remains shadow-ON/action-OFF.

## Evidence Boundary

The scientific conclusion is `unsupported_negative_point_estimate`. The frozen 40-day current-stack comparison estimates `-16.7946 USDC` for selector ON minus OFF, while fills increase only `0.573%`, loss magnitude worsens `10.55%`, and multi-level SHORT value worsens `-13.5910 USDC`. The day-clustered interval `[-1.3041, +0.2950] USDC/day` crosses zero, so it does not establish that the average treatment effect is negative.

Even an interval wholly below zero would not prove harm in every market state; that would require conditional-effect or statewise-bound evidence. The project must not claim that BUY fill-selection has been proven universally harmful.

## Operational Decision

Operational suspension does not require that stronger claim. An execution action bears the burden of positive economic evidence, and this action is reversible while the scorer remains observable. The active contract is:

```text
buy_fill_selection_shadow_enabled = true
buy_fill_selection_live_enabled = false
```

Shadow hits may update scorer diagnostics and append the shadow journal. They cannot change the policy reason mask, spread multiplier, or quote coordinates. The health stream reports shadow evaluations/hits separately from applied actions.

## Re-enable Rule

The action cannot be re-enabled merely because the old estimate was not significantly negative. Re-enable consideration requires a new frozen identity with positive economic evidence against the then-current operational baseline. Automatic re-enable remains forbidden.

The deployment is config-and-runtime-code reversible. Its machine record is [`buy_fill_selection_shadow_action_separation_operational_release_v2_20260803.json`](buy_fill_selection_shadow_action_separation_operational_release_v2_20260803.json).
