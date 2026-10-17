# Cooldown Action Leverage Frontier v1 - Provenance Errata

Last materially modified: 2026-07-30

## Correction

The frozen Spec, evaluator, repository report, and machine report compare the one-cycle MDE-per-affected-fill scale with an "observed first-add loss scale." The frontier identity did not bind, hash, or read the F10 first-add report.

That comparison is therefore an **unbound contextual comparison**. It is not an evidence-bound economic gate and must not be cited as an input to the frontier closure decision.

The contextual file is [`first_add_decision_to_terminal_loss_diagnostic_v1_development_20260729.md`](../../f10_live_replay_attribution/docs/first_add_decision_to_terminal_loss_diagnostic_v1_development_20260729.md), but this errata deliberately does not retroactively attach its SHA256 to the frozen frontier identity.

## Current interpretation

The old MDE/fill-rate table and tested-subspace exhaustion verdict are no longer current conclusions. Temporal permission remains [F09-TEMPORAL-PERMISSION](../research_questions.json), pending validation. Any new MDE must use the declared current opportunity denominator rather than a post-selected losing-lot subset. This erratum still identifies the original unbound comparison; it does not retroactively bind that source or convert an old close decision into a new result.

The frozen original Spec and result are unchanged. The historical machine correction remains in [the original errata record](cooldown_action_leverage_frontier_v1_errata_20260730.json).
