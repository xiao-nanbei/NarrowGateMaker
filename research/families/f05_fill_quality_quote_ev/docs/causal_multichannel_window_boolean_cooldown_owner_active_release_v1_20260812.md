# Causal Multichannel Boolean Cooldown Owner Active Release

Last materially modified: 2026-08-12

Evidence availability: this public report and the referenced public identities are available in the repository. Exact policy bytes, deployment package, preflight output, rollback bundle, process state, machine locators, and prospective epoch records are retained in the private evidence store and are not distributed with the public repository. SHA256 values identify named bytes; they are not download links.

## Decision

The owner has explicitly accepted the unresolved statistical and transport risk and authorized the frozen SELL Boolean cooldown policy for active live use. This is an `owner_risk_accepted_promotion`, not a research-supported promotion.

```text
research hard gates passed: false
research-supported action authority: false
owner risk accepted: true
owner active-live authority: true
live policy enabled: true
Validation read: false
sealed holdout read: false
```

The historical scorecard remains unchanged. Its terminal and closed-campaign confidence intervals cross zero, the positive-day gate failed, strict historical queue authority is unavailable, and prospective receive-time transport evidence is incomplete.

## Evidence Used

The owner decision used the already frozen, outcome-informed policy and did not retune its rules or durations after reading economics.

| Panel | Terminal increment | Closed-campaign increment | Fill retention |
| --- | ---: | ---: | ---: |
| Repeated-policy 50-day Development | +11.372165 USDC | +9.747065 USDC | 96.04% |
| Restart-aware continuous 71-day Development | +16.877254 USDC | +16.895254 USDC | 97.66% |

On the 71-day panel, campaign q10, CVaR10, and maximum absolute inventory improved. The terminal increment was `+0.237708 USDC/day`, with paired day-bootstrap 95% interval `[-0.173707,+0.766862]`; only 26/71 dates improved. These limitations are why the deployment retains the permanent owner-risk label.

## Active Policy

The active identity is `causal_multichannel_window_boolean_cooldown_owner_policy_v1`.

- BUY remains `CONTROL_85N`.
- SELL is evaluated only after an exposure-increasing fill.
- Reducing quotes are unchanged.
- The ordered SELL policy can select total cooldown durations of `166s`, `211s`, or `1748s`; unsupported, stale, unobserved, or warming state falls back to `CONTROL_85N`.
- The policy uses only the selected decision-visible mid-EMA cross-age predicates and campaign age. No trade/depth predicate, external venue input, P3 mapping, q90 action, BUY selector, BER change, quote-price change, order-size change, or inventory-limit change was introduced.
- The runtime uses receive-time 100ms state updates and requires 2048 seconds of causal warmup after a cold start. Warmup never blocks trading; it preserves the predecessor cooldown behavior.

Policy SHA256: `877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29`

Predicate-bundle SHA256: `ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37`

## Deployment Verification

The deployed configuration SHA256 is `800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85`. A fail-closed preflight verified the configuration, model, P3, Feature DAG, policy, predicate bundle, owner authorization, and unchanged q90/BUY-selector action states before deployment. Future starts and restarts now run the same preflight before changing process state and atomically persist its result.

Post-start verification confirmed:

```text
single maker process: pass
owner-risk authority loaded: pass
policy enabled health field: pass
BUY selector action/shadow: off
q90 action: off
deep-book and quote loop: active
startup severe errors: none observed
```

The first health windows correctly reported policy warmup and predecessor fallback. A non-control cooldown is not expected until warmup is complete and a future eligible SELL exposure-increasing fill occurs.

The exact deployment package, preflight output, rollback bundle, process receipt, and machine locators are retained by the F05 private owner. The complete rollback bundle SHA256 is `f09c97d1679f8fae8b1df3d8e6e7ca12f6ef71dfa83ca1f30817b635087d220e`.

## Operating Boundary

This release does not retroactively pass the frozen research scorecard and does not authorize tuning on the consumed 50/71-day evidence. Any rule, duration, side, feature, warmup, or fallback change is a new policy identity. Runtime artifact mismatch fails startup; per-decision unsupported state falls back to `CONTROL_85N`.

The successor operational baseline identity is recorded in `research/families/f10_live_replay_attribution/docs/operational_baseline_identity_20260812_v11.json`.
