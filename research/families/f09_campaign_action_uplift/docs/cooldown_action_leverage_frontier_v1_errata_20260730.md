# Cooldown Action Leverage Frontier v1 - Provenance Errata

Last materially modified: 2026-07-30

## Correction

The frozen Spec, evaluator, repository report, and machine report compare the one-cycle MDE-per-affected-fill scale with an "observed first-add loss scale." The frontier identity did not bind, hash, or read the F10 first-add report.

That comparison is therefore an **unbound contextual comparison**. It is not an evidence-bound economic gate and must not be cited as an input to the frontier closure decision.

The contextual file is [`first_add_decision_to_terminal_loss_diagnostic_v1_development_20260729.md`](../../f10_live_replay_attribution/docs/first_add_decision_to_terminal_loss_diagnostic_v1_development_20260729.md), but this errata deliberately does not retroactively attach its SHA256 to the frozen frontier identity.

## Unchanged Evidence

The following calculations remain valid because they use only the bound one-cycle mechanics report:

| Side | Design MDE | Selected-cycle fill rate | MDE / fill rate |
|---|---:|---:|---:|
| BUY | 0.014364963 USDC | 0.0715772 | 0.200691900 USDC/affected fill |
| SELL | 0.015456289 USDC | 0.0626065 | 0.246880049 USDC/affected fill |

The frozen 10% near-noop rule also remains unchanged: both selected-cycle fill-rate 95% upper bounds are below 10%.

## Decision Impact

There is no decision impact. The machine-readable close rule is based on:

- all included action identities remaining closed;
- no tested row combining acceptable activity, a positive reward lower bound, and supported lifecycle evidence;
- no cross-source pooled estimate;
- the bounded claim that only the tested temporal-permission subspace is exhausted.

None of those conditions reads or depends on the F10 first-add report. Accordingly:

- `tested_cooldown_temporal_permission_action_subspace_exhausted` remains valid;
- `f09_family_closed=false` remains valid;
- Validation, sealed holdout, action, and live permissions remain false.

The frozen Spec and result artifacts are intentionally not modified. The machine-readable correction is recorded in [`cooldown_action_leverage_frontier_v1_errata_20260730.json`](cooldown_action_leverage_frontier_v1_errata_20260730.json).
