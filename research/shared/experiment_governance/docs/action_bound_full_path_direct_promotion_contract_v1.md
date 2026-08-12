# Action-Bound Full-Path Direct Promotion Contract v1

Last materially modified: 2026-08-04

## Rule

Observation-only shadow is no longer a mandatory strategy-promotion stage. The governed sequence is:

```text
freeze one concrete action
-> authoritative full-path replay
-> promotion controller
-> active live with preflight and automatic rollback
```

Both evidence routes remain valid. A normal hard-gate successor may receive `research_supported_promotion`; an explicit owner continuation may receive `owner_risk_accepted_promotion`. The owner label is permanent. Neither route may skip full-path economics, execution parity, safety gates, or rollback.

## P3 Boundary

Conditional P3 leverage is embedded in a named action and reports only:

\[
p_0,\quad p_1,\quad \Delta R=p_1-p_0,\quad
CI_{\mathrm{simultaneous}}(\Delta R).
\]

It does not create a standalone research identity, generate a quote, mutate an order, or grant action/live authority. Its interval family is frozen with the action candidate family. Reach is not fill probability or economic value.

The active identity is named after its independent action:

```text
<candidate_source>_conditional_p3_reach_gate_v1
```

The action proposes side, role, direction, and executable price. P3 may only accept or reject that proposal through a frozen reach-mechanics gate. Missing context, unsupported distance, or invalid paired uncertainty falls back to the baseline quote for that decision.

The embedded interface binds baseline/candidate policy hashes, snapshot market and depth generations, effective prices and tick delta, candidate-universe and simultaneous-band hashes, and paired probabilities. `relative_reach_ratio` is null below the frozen denominator epsilon. `price_action_noop` and `reach_near_noop` are separate; neither implies economic no-op.

## Cadence And Ownership

The current v4.1 surface may be queried once at each canonical 10-second epoch. It may not be repeatedly queried by the 100ms loop. Each order retains the epoch and action identity under which it was submitted.

An action using fixed 10-second ownership must either apply the same ownership rule to both replay arms or declare ownership/TTL as part of the treatment. If only the candidate arm changes ownership, the result is a joint price-plus- lifecycle policy effect and cannot be attributed solely to P3. Preserving the current dynamic requote lifecycle instead requires a separately frozen multi-horizon P3 identity.

## Full Path

Baseline and candidate use the same immutable `QuoteDecisionSnapshot` and the same market/random path. Prices are compared only after tick rounding, GTX, post-only checks, and spread caps. Replay must regenerate activation, queue, partial fills, cancel/ACK races, cooldown, inventory, and campaign state.

Promotion requires positive assignment-to-terminal PnL lower bound; no material worsening in campaign q10/CVaR, MAE, maximum inventory, or inventory time; and fill/activity within limits frozen before outcomes. Candidate rate, side scope, and rate limits deployed to live must exactly match replay.

## Shadow Budget

Shadow remains permitted for bounded execution parity, transport collection, or incident diagnosis, with an explicit owner, byte/rate budget, and expiry. It is not strategy evidence and cannot substitute for full-path replay.

Current rationalization is:

- retire BUY fill-selection and closed fair-center shadow writers;
- retire continuous inventory what-if and depth diagnostics once their frozen denominators are sufficient, while retaining the promoted imbalance action;
- keep q90 shadow only while fresh-recovery and transport work is active;
- move external receive-time collection to the independent bounded recorder before removing those inputs from the live process.

This contract grants no action, Validation, holdout, or live permission.
