# Causal-v12 Operational Baseline Promotion

Last materially modified: 2026-08-02

Status: active operational and backtest baseline; research confirmation remains unresolved.

## Decision

The owner promoted the running causal-v12 semantics-v6 configuration from a reversible live canary to the rolling NarrowGate operational baseline on 2026-08-02. The same identity is now the default control for new backtests.

This was an identity-only promotion. The EC2 process, config bytes, model bundle, empirical P3 artifact, q90 policy, queue mechanics, latency profile, cooldown, size, inventory limits, and safety gates did not change. No process restart was required, so open orders and inventory state were not disturbed.

The immutable machine identity is [`operational_baseline_identity_20260802_v4.json`](../../f10_live_replay_attribution/docs/operational_baseline_identity_20260802_v4.json). The mutable current pointer is [`operational_baseline_current.json`](../../f10_live_replay_attribution/docs/operational_baseline_current.json). The preceding [`operational_baseline_identity_20260802.json`](../../f10_live_replay_attribution/docs/operational_baseline_identity_20260802.json) remains the historical live-canary snapshot and is not rewritten.

## Economic Basis

The owner-amended v2 rescore combines the previously read historical late 22 days with five post-fit Grade-A days. It reports:

- 19/27 positive ML-ON minus ML-OFF PnL days;
- closed-campaign value uplift of `+1.1050 USDC/day`, with a day-clustered 95% interval of `[+0.4801,+1.7306]`;
- total terminal-MTM uplift of `+1.2126 USDC/day`, with a 95% interval of `[+0.5755,+1.8437]`;
- fill retention of `85.58%`, inside the owner-amended 80%-120% band;
- loss-reduction/fill-reduction selectivity of `2.925`, with a 95% interval of `[1.574,4.384]`.

The day-end open-MTM contribution is only `2.9045 USDC`, or 8.87% of the total uplift. Closed campaign value is therefore the primary operational economic measure; UTC day remains an inference cluster rather than a forced strategy reset boundary.

## Evidence Boundary

Campaign q10 remains unresolved: the mean daily delta is `-0.01269 USDC` and its 95% interval is `[-0.03880,+0.01419]`. The source panels and their outcomes were already read, and the amended fill gate was specified after observing the earlier result. Accordingly:

- `baseline_promotion_authorized=true`;
- `research_prediction_authority=false`;
- `research_live_authority=false`;
- `ranking_score=null` remains the historical scorecard result;
- no closed action family or frozen historical Spec is reopened.

Future experiments must compare candidates with the v12 ML-ON operational control. A confirmatory economic replay must preserve inventory, cash, cooldown, campaign, and model state across UTC midnight. A replay claiming current q90-ON live equivalence also remains blocked until cancel-ACK terminal risk-set and post-cancel recovery semantics are repaired.
