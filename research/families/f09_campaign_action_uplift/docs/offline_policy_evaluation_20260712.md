# Offline Policy Learning and Counterfactual Evaluation

Last materially modified: 2026-07-27

## Status

`models.audit.offline_policy_evaluation` is a research/audit layer. It does not change replay parameters, live quotes, order size, inventory limits, or routing.

This document describes the estimator contract. Experiment ranking is governed by `models.audit.experiment_scorecard`, and access to Validation or family-specific sealed evidence is governed independently by `models.audit.panel_promotion_controller`. An OPE estimate cannot grant itself access to another panel or permission to change live behavior.

The first implementation is action-level/contextual OPE. It evaluates one independent decision unit at a time. A future multi-step campaign implementation must use sequential density ratios or another MDP estimator; the current module must not be described as a complete implementation of Kallus-Uehara double reinforcement learning.

The design follows two methodological references:

- [Bennett and Kallus, *Efficient Policy Learning from Surrogate-Loss Classification Reductions*](https://proceedings.mlr.press/v119/bennett20a.html);
- [Kallus and Uehara, *Double Reinforcement Learning for Efficient Off-Policy Evaluation in Markov Decision Processes*](https://jmlr.org/papers/v21/19-827.html).

The second paper defines the direction for sequential campaign OPE. The current v1 estimator is deliberately narrower because adjacent maker decisions share orders, queue state, inventory, and campaign outcomes.

The first paper is also a warning, not a label to attach to any DR score: a surrogate-loss policy-learning reduction need not be efficient for policy parameters. This v1 module does not implement Bennett-Kallus's efficient GMM policy-parameter estimator. Its optional supported-`Q` argmax is discovery only; the formal output is cross-fitted contextual policy evaluation for a pre-registered candidate.

## Why the Existing Score Is Not Enough

The existing fill-selection score estimates a relation close to:

\[
P(\text{good fill}\mid x,\text{baseline placed and filled}).
\]

It can rank baseline fills, but it cannot by itself identify:

\[
V(x,a_{candidate})-V(x,a_{baseline}).
\]

Three missing quantities matter:

1. the behavior propensity `e(a|x)`;
2. action-specific outcomes `Q(x,a)`;
3. support/overlap showing that the candidate action was actually observable under the behavior policy.

A placed-order table is also insufficient for actions such as `pause` or `skip`, because those decisions never enter the placed denominator.

## Action Panel Contract

One row must represent one independent decision unit. Required columns are:

| Column | Meaning |
|---|---|
| `day` | UTC decision day used for grouped splits |
| `decision_id` | Stable unique decision identifier |
| `action` | Action actually taken by the behavior policy |
| `reward` | Reward observed after that action |
| `candidate_action` | Pre-registered deterministic candidate action |

Instead of `reward`, the panel may provide:

\[
reward = fill\_value - campaign\_cost - queue\_cost.
\]

The decomposition must be defined before inspecting the candidate result. It must not count maker-signed markout and the same spread capture twice.

Optional `candidate_prob_<action>` columns define a stochastic candidate policy. They must be non-negative and sum to one on every row; the evaluator does not silently normalize malformed policy probabilities. For controlled replay or live randomization, a complete `behavior_prob_<action>` vector is preferred. It must include every registered action, be finite, and sum to one on every row. The selected action must have positive probability. When both the vector and `behavior_propensity` are present, the scalar must exactly match the selected action's vector entry. The evaluator then uses the logged vector directly and does not fit a propensity model. If no complete vector is present, optional `behavior_propensity` provides the known probability of the logged action while the remaining behavior policy is estimated out of fold; if neither is present, the full behavior policy is estimated out of fold.

Missing actions are rejected rather than converted to a string category. A candidate action that was never logged remains unsupported even when an outcome regression can extrapolate a number for it.

The replay decision trace now emits `decision_id`, but trace rows do not yet have a valid action reward. A reward label must be produced by a non-overlapping shadow probe, a parity-qualified replay counterfactual, or controlled randomized exposure. Do not join one terminal campaign PnL value to every decision in that campaign; that duplicates the outcome and violates the independent-unit contract.

## Causal Feature Registry

Formal mode uses an allowlist of decision/submit-time state. Examples include:

- side and inventory role;
- inventory/campaign state known so far;
- queue, depth, microprice, toxicity, and markout EMA state;
- cooldown occupancy;
- causal multi-market state available before the action.

Terminal campaign fields, future markout, fill outcome, and post-action state are rejected as features. Custom fields require a JSON registry entry with `available_at` equal to `decision` or `submit`.

For event-time or cross-venue fields, the registry can also specify `source_timestamp_col` and `max_age_ms`. The evaluator then requires:

\[
source\_timestamp \le decision\_timestamp
\]

and fails the run if any row exceeds its pre-registered age budget. Ambiguous fields such as final `campaign_max_abs_qty` or realized `*_markout_30s` are not part of the built-in allowlist. A genuinely decision-time model prediction must be registered under a name and provenance that distinguish it from its label.

## Estimators and Gates

For each chronological or blocked-day fold, the module fits:

\[
\hat e(a\mid x), \qquad \hat Q(x,a).
\]

It reports direct method, clipped IPS, clipped SNIPS, and clipped doubly robust candidate values. Candidate-minus-behavior uncertainty uses a day-clustered bootstrap.

The formal gate also requires:

- sufficient prediction coverage;
- candidate unsupported mass below budget;
- effective sample size above threshold;
- every candidate action to have minimum logged support;
- bounded importance weights.

The summary additionally reports day-level positive/negative/zero uplift counts. These are diagnostics alongside the day-clustered interval, not a replacement for overlap, ESS, campaign-tail support, or an untouched later panel.

If an action was never attempted by the behavior policy, a regression prediction does not repair the missing overlap. The report status becomes `diagnostic_only_overlap_failed`.

Passing these numerical gates is conditional on identification assumptions. In particular, action meanings must be consistent; all joint causes of action and reward must be observed; candidate actions need positivity; decision rows must not have unmodeled interference; and campaign reward cannot be duplicated across multiple decisions. These assumptions are written into every JSON/Markdown report. The evaluator cannot prove them from the panel.

Accordingly, a passing report uses the status `overlap_gates_passed_assumptions_required` and always records `causal_identification_proven=false`. The compatibility field `formal_estimate_valid` means only that the implemented numerical OPE gates passed; it is not a claim that exchangeability or no-interference was proven.

## Unified Scorecard Boundary

Each maintained action-family result must also emit the canonical scorecard from `models.audit.experiment_scorecard`. The family specification freezes one versioned profile before outcomes are read:

- `action_alpha_v1` for conditional net-value actions;
- `action_defense_v1` for downside-protection actions;
- `action_execution_v1` for queue/lifecycle/latency actions;
- `paired_screen_v1` for parameter screening only, never promotion.

Identity, causality, propensity, overlap/ESS, reward-lower-bound, tail, candidate-rate and fill-retention failures are non-compensable hard gates. When any hard gate fails, `ranking_score` is null even if a soft weighted score would be positive. The scorecard classifies evidence; the separate promotion controller checks the frozen panel identity and decides whether Validation or sealed evidence may be read.

Blocked-day cross-fitting is not chronological walk-forward: its training set may include dates after a test day. New action families should first freeze Development, embargo, Validation and family-specific sealed evidence from the existing retained good-day universe. Waiting for future days is required only after that family's evidence is consumed, its specification changes after holdout access, support is exhausted, or production drift requires fresh confirmation.

## CLI

```bash
.venv/bin/python -m models.audit.offline_policy_evaluation \
  --panel-csv <action-panel.csv> \
  --out-prefix <output-prefix> \
  --feature side \
  --feature inventory_role \
  --feature inventory_ratio \
  --feature campaign_age_s \
  --feature queue_local_rank \
  --feature microprice_shift_bps \
  --split-mode chronological \
  --min-train-days 30 \
  --embargo-days 1 \
  --test-days 10
```

For discovery only, `--learn-supported-policy` chooses the highest predicted `Q(x,a)` among actions whose out-of-fold behavior propensity exceeds the overlap floor. It is still subject to the same DR/ESS/unsupported-mass report and must be evaluated on a later panel not used to select features or hyperparameters.

Outputs:

- `*.ope_rows.csv`: out-of-fold propensities, Q values, weights, and pseudo-outcomes;
- `*.ope_folds.csv`: chronological/blocked train-test identities and fold gates;
- `*.ope_actions.csv`: logged support and candidate action mass;
- `*.ope_summary.json`: estimators, overlap, ESS, bootstrap, and formal status;
- `*.ope_report.md`: compact human-readable report.

For a genuinely later panel, use `evaluate_fixed_holdout_policy()`. It fits the outcome models on the frozen development panel and evaluates only disjoint later days. It rejects overlapping train/holdout days and records `holdout_used_for_fit=false`. Running ordinary cross-fitting inside the later panel is not an acceptable substitute.

## Promotion Boundary

An OPE pass is evidence for a specified candidate policy under the logged data distribution. It is not permission to bypass replay/live parity, campaign-tail gates, latency stress, or a later holdout. Candidate actions that change queue priority or future inventory must still be replayed through the complete order lifecycle before any live consideration.

See `experiment_scorecard_v1_20260722.md` for the metric/gate authority and `paired_screen_v2_architecture_20260727.md` for the separation between evidence building, screening/ranking, and panel promotion.

## Implemented Validation Sequence

The original plumbing validation assigned `block/rearm` labels after an observational shadow probe and was explicitly marked `strategy_evidence=false`. It has been superseded by `models.audit.safe_add_rearm_randomized`. The current runner assigns one actual replay action per eligible campaign with a logged probability vector: `r0_block`, `r1_rearm`, or `r2_rearm_widen_1tick`. R0 preserves the baseline fill cooldown; R1 submits the baseline add quote once; R2 submits it one tick farther from the market. R1/R2 traverse actual replay new-order latency, queue, fill, cancel, inventory, and campaign paths. `models.audit.safe_add_rearm_ope_panel` now validates these action-bearing rows and rejects post-hoc shadow-probe input.

Eligibility is restricted to the first quote decision in a campaign after the pre-registered elapsed time where the exposure-increasing side is blocked only by fill cooldown and has no active or pending same-side order. Reducing quotes, size, and inventory limits are unchanged. The reward is the actual decision-to-campaign-terminal MTM change; 30-second maker-signed fill value is only an attribution component, and campaign cost is the accounting residual in `reward = fill_value - campaign_cost - queue_cost`.

The first action-bearing replay family is `models.audit.local_action_uplift`. It uses local exact L2/queue/flow state only, chooses at most one intervention per campaign, and supports four actions: `baseline`, `prevent_over_widen`, `widen_1tick`, and `recenter_1tick`. The v1 eligibility surface is deliberately restricted to a newly placed exposure-increasing `add` quote with no active or pending same-side order. This makes queue reset cost zero by construction while the selected order and all subsequent state still traverse replay latency, queue, pending cancel, fills, cooldown, and inventory mechanics. `opener` and `reducing` remain unsupported in v1 and must not be inferred from the `add` result.

`models.audit.local_action_ope_report` evaluates the frozen panel separately for BUY, SELL, and pooled scopes and reports primary reward, terminal campaign MTM, intervention fill, and a separately named extreme-terminal-tail diagnostic. A candidate with too few logged tail events is rejected for tail support even if its observed tail count is zero.
