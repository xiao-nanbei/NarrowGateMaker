# Multi-Short Reducing-BUY Aggression v1.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Decision

`close_multi_short_reducing_buy_aggression_on_development`

The exact maker-only repair action is closed. Validation and the sealed holdout remain unread; no action or live authority was created. The result is not a near pass and must not be rescued by tuning aggressiveness, trigger inventory, release inventory, or the scorecard after observing Development.

The initial v1 implementation failed before outcome access because floating price arithmetic could emit an invalid maker tick. v1.1 replaced that boundary with integer tick arithmetic while preserving the frozen action. The failed v1 Spec and implementation-failure record remain unchanged.

## Action

At the first campaign transition to `inventory <= -0.002 BTC`, the campaign was assigned once at exact 0.5/0.5 propensity:

- control: current reducing-BUY logic;
- candidate: keep reducing BUY open at the most aggressive valid maker price, `min(ask1 - tick, max(baseline, bid1))`;
- release: inventory recovers to `>= -0.001 BTC`;
- exposure-increasing SELL, opener, cooldown, inventory limit and all other policy mechanics remain shared;
- BUY q90 is OFF in both arms;
- GTX maker only, with no action-generated IOC or taker order.

The primary outcome starts at assignment and ends at campaign terminal under the lineage outcome v2 accounting contract.

## Support

The frozen 40-day Development identity contains 24 Grade-A primary days and 16 Grade-B sensitivity days. Grade B is not pooled into the primary decision.

Grade A contains 627 assignments. Candidate assignment rate is 50.88%, and the final-action change rate is 99.37% with a 95% lower bound of 98.42%. Total fill retention is 103.00%, reducing-BUY fill ratio is 102.23%, and SELL-exposure fill retention is 105.05%. The action was neither a no-op nor a participation shutdown.

Across all 40 days there were 1,015 candidate quotes, zero maker violations, zero action-generated IOC/taker orders, and zero effective defense-pause overrides. The latter means the observed mechanism came from a more aggressive maker repair price, not from frequently bypassing defense pause.

## Primary Economics

Grade-A assignment-to-terminal reward was worse under treatment:

| Metric | Estimate | 95% interval / comparison |
|---|---:|---:|
| Reward uplift | -0.004435 USDC/assignment | [-0.030210, +0.021736] |
| UTC-day positive rate | 45.83% | gate requires 55% |
| Policy value | -0.151531 USDC/day | [-0.849884, +0.525528] |
| Multi-level SHORT loss protection | -0.004435 USDC/assignment | [-0.030786, +0.022624] |
| Candidate q10 | -0.231140 USDC | control -0.211660 |
| Candidate CVaR10 | -0.451453 USDC | control -0.448855 |

Grade B points in the same direction: reward uplift is -0.023007 USDC/assignment with interval `[-0.062423, +0.024066]`, and only 5 of 16 days are positive.

## Mechanism

The treatment did achieve its immediate mechanical objective:

- inventory-time avoidance: +0.277339 BTC-seconds, 95% lower bound +0.100789;
- repair-time avoidance: +84.82 seconds, 95% lower bound +18.54 seconds;
- repair probability uplift: +0.00336, with interval crossing zero.

Faster repair did not transport into terminal value or tail improvement. Maximum-inventory and campaign-MAE intervals also crossed zero, while q10 and CVaR10 were slightly worse. The economic conclusion is therefore:

> More aggressive passive reducing BUY shortens multi-level SHORT exposure, but under the current lifecycle it does not improve terminal PnL and does not protect the left tail.

## Gates

The canonical scorecard has `ranking_score=null` and `promotion_status=development_failed_family_closed`. Hard failures include non-positive value lower bounds, insufficient daily sign stability, and failure to exclude max-inventory, MAE and tail worsening.

This closes the exact aggressive-maker repair action. A bounded IOC emergency repair would be a genuinely different action with taker fees, ACK/fill races and its own safety contract. It was not created or tested by this result.

## Provenance

- Frozen Spec SHA256: `a8fa44e1a333da9e12b34ce8695e94a650d6c152d4e7e8da2c9a120ccf511c79`
- Authoritative report SHA256: `929038b18c3f95e163899385fc3f09b1ed04a67d06ef1b774e6a3514f8f3b5b6`
- Scorecard SHA256: `362e8f9811f2d236b56d003d7f8dc344e7964a66806ffece3971c50af905c40b`
- Frozen contract tests: 63 passed, 0 failed/errors.
- Machine-readable post-run audit: `multi_short_reducing_buy_aggression_v1_1_postrun_audit_20260801.json`.

The reusable replay-cache DAG was implemented after this run. It does not alter this frozen implementation identity or retroactively change the result.
