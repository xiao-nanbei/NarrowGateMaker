# BUY Fill-Selection Live Shadow Retirement Operational Release v3

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: deployed and verified. The legacy BUY fill-selection action and its live shadow scorer are both OFF. The scorer artifact and research/replay code remain available only as historical F05 evidence.

## Operational change

At `2026-08-03T23:01:19Z`, the AWS Tokyo maker restarted under Python 3.12.13 with this exact permission state:

```text
buy_fill_selection_shadow_enabled = false
buy_fill_selection_live_enabled = false
buy_fill_selection_live_model_path = ""
```

No q90, causal-v12, empirical-P3, quote-core, queue, latency, cooldown, inventory, order-size or safety parameter changed. The old remote config is preserved at `deploy_backups/live_config_pre_buy_fill_shadow_retirement_20260803T230008Z.yaml` with SHA256 `94425652b01e67f9285bc91e514413eea9c8b01ab53cbdf740c8802b952151e2`. The active config SHA256 is `889f605dc6a057874a8070fd86cbd21a0c8eb050156315c1dc6f48ec9acb48f5`.

Remote and local preflight passed. PID `1792979` stopped cleanly and PID `1798225` started with all 13 causal-v12 heads loaded. The first post-restart health row reported zero BUY fill-selection evaluations, hits and actions. The shadow CSV remained at 240,563 lines after restart, confirming that the writer had stopped rather than merely changing the action flag.

## Why the old selector failed

This is not primarily a raw sample-count problem.

- The deployed 2026-07-06 artifact contains 111 distinct days. Its five blocked-day folds train on 87–92 days and test on 19–24 days each.
- The repaired causal-v6/v7 Validation panel contained 50,460 order opportunities and 1,148 fills. All four repaired targets still failed the joint fill-quality, campaign-value and tail gate.
- The exact current-stack 40-day A/B evaluated 396,657 selector decisions and 19,534 baseline fills. Enabling the action changed terminal MTM by `-16.7946 USDC`, increased fills by 0.573%, increased inventory time by 3.21%, and worsened multi-level SHORT terminal value by `-13.5910 USDC`.

The dominant problems are semantic and causal:

1. **Wrong estimand for the action.** The legacy label is defined only after an order fills. It estimates a version of `P(good outcome | filled, state, baseline policy)`. The live action instead tightens a BUY quote by capping a soft widen back to `spread_mult=1.0`, so it changes which marginal orders fill. A high conditional-quality score on baseline fills does not identify the incremental value of the newly induced fills.
2. **Training/live feature mismatch.** The artifact declares 42 features, including order/lifecycle fields such as queue state. In the latest 360-hour live window every one of 118,832 shadow rows was missing 28–30 features and used only 13–15. The live gate allowed up to 99 missing features. The score distribution was correspondingly narrow: mean `0.434256`, standard deviation `0.003660`, range `0.424900–0.453414`, against a fixed threshold of `0.44`.
3. **Non-chronological legacy artifact.** The deployed fold ensemble is a blocked-day diagnostic in which the same 111-day universe appears in train folds and test folds across the ensemble. It is not a past-only production mapping.
4. **Campaign spillover is outside the label.** A local BUY quote change alters later inventory, SELL repairs, cooldown and multi-level campaign paths. The observed 40-day deterioration was concentrated in those downstream paths, while the binary fill label never represented their direct USDC value.

More rows can reduce variance, but cannot repair these four identification errors. In particular, the 2025 provider-normalized data lack the exact native order lifecycle needed to turn this old classifier into an action-value model.

## Research-only successor boundary

The following are retained:

- `strategy/fill_selection_model.py` as a historical scorer/replay adapter;
- F05 order-level, toxicity and fill-selection audit code;
- the 2026-07-06 artifact and the repaired causal-v6/v7 frozen evidence;
- the 40-day action-OFF/ON report.

They are no longer an operational shadow contract. A future successor must not retrain the same filled-only classifier with more dates. It must freeze one exact action and estimate its marginal value directly:

\[
\Delta Q(x)=
E[Y_{\mathrm{terminal}}\mid x,\text{cap soft widen to }1.0]
-E[Y_{\mathrm{terminal}}\mid x,\text{keep baseline quote}].
\]

All features must be decision-visible and live/replay-identical; no post-activation queue or outcome field may enter the feature row. Evaluation must use chronological outer OOF days, carryover-safe action ownership, exact activation/queue/cancel/partial-fill paths, and assignment-to-terminal USDC. Only independent positive full-path evidence may create a new active action. The legacy shadow log is not a prerequisite and cannot grant authority.
