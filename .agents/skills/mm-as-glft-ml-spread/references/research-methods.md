# Research method selection

Last materially modified: 2026-10-18

Read for research, not routine file maintenance. Preserve the user's question and selected experiment identity; do not substitute a convenient older study or demand every method at once. General evidence sufficiency and change-scoped verification belong to the checkout's `AGENTS.md`.

## Match checks to the method

| Task | Required evidence | Not a universal prerequisite |
| --- | --- | --- |
| Supervised prediction | Label estimand/units, causal visibility, actual outcome endpoints, split leakage/purge, train-only fitting/selection and support | Randomized actions, propensity, DR or live deployment |
| Direct paired simulation | Same frozen data, executable code/runtime, initial state, fees/funding, latency/tie/fill assumptions and account boundaries; action effects, net PnL and inventory/tail risk | Invented behavior propensities or OPE |
| Logged-policy OPE | Reliable behavior propensity, consistency, overlap/positivity, effective sample support, estimator assumptions, temporal fitting and uncertainty; DR when selected by contract | Fictional propensity for unsupported deterministic actions |
| Randomized intervention | Actual assignment mechanism/probabilities, intervention unit, contamination checks, accounting and prespecified estimand | One intervention per inventory lifecycle unless its exact contract requires it |
| Engineering equivalence | Fixed-input state/output parity, causality, performance and regressions | Positive PnL or changed actions |
| Historical live matching | Same runtime/config/epoch, observed visibility and verified initial account/order state for the claimed interval | Live account restoration for unrelated new experiments |

Simulation proves differences under its assumptions, not online causal gains. Prediction quality does not prove economic improvement. Missing native queue/transport evidence limits native/live-parity claims, not all modeled offline research.

## Contracts and baselines

### Historical claims and discoverable open questions

Under the owner's October17 instruction, treat historical claims encountered in maintenance as questions for current validation rather than importing their old rankings, losses, gates or promotion decisions as current conclusions. Use the relevant family in `research/registry.json`; its `research_questions` field points to one family-owned `research_questions.json`. Scan all family links for `status=pending_validation` only for an explicit inventory/prioritization request; a local task reads its relevant family/question. Record directly relevant unsupported historical claims there without automatically starting validation. A missing link means not inventoried, not an empty backlog. Deduplicate by stable question ID and cross-reference the same question from blogs, family guides and private owner records rather than creating separate task lists.

Each record identifies the question, historical source references, evidence status and specific missing pieces, next validation and any existing owner artifact. Keep the hypothesis status separate from evidence availability: distinguish a located old report, a method needing reconstruction, unverifiable provenance and explicitly retired source material. If the source or execution identity cannot be established, say so; do not fabricate a baseline, sample or result. Do not erase an accepted historical observation merely because current applicability is pending. Preserve frozen records and previous-use; the current-facing guide presents the question without isolated old performance numbers. Creating a question neither executes an experiment nor changes a running frozen study.

Resolve a question only against its bound new experiment and retained report, recording supported, unsupported or inconclusive for that scope; missing data and unperformed tests are not negative results. Historical close/exhausted labels do not forbid necessary owner-authorized validation. This is not a new universal significance threshold or backtest quota.

- `operational_baseline`: actual deployed behavior at a verified runtime epoch, resolved only for operational/live-matching work.
- `research_baseline`: B0 frozen for one experiment; it does not move when live changes. Include a control only when the experiment calls for one.
- `demo_baseline`: synthetic/template behavior, not operational evidence.

A running live process or public release is not a prerequisite for offline data repair/training. Identify source with an exact local/private commit/tree or immutable snapshot plus separately bound authorized overlay. Never label dirty bytes as unchanged HEAD. Bind one run root to data/calibration/model/runtime and result identities.

The active experiment defines calendar, split and previous-use rights, model candidates, calibration/selection, account boundaries and stop conditions. Do not substitute generic three-month rules, old selected-day panels, continuous accounts or inventory lifecycle propensities for an approved protocol. Keep all declared dates and report missing support/uncertainty; repair never resets previous-use or unlocks sealed results.

There is no owner-imposed backtest run-count quota. Perform necessary evaluations and engineering/recovery checks for the requested question rather than stopping when a historical numerical allowance is exhausted. Declared panel size is a completeness denominator, not an execution cap. Keep resource protection, declared identities, no accidental duplicate dispatch, causal validity and honest rerun provenance; removing quotas does not change a model or create a new scientific question by itself.

For work on the owner-authorized recomputation, resolve `research/recompute_407.json` and the selected frozen plan, not as a prerequisite for unrelated maintenance or every new experiment. Rebuild market derivatives, calibration, labels, models and economics only where incompatible with that plan; reuse compatible verified artifacts rather than restarting because documentation changed. Resolve B0 and any ML-OFF arm from the actual experiment contract, not a stale stage description here. Funding/auxiliary exceptions need actual authorization and source/visibility verification. Preserve previous-use; current runtime progress belongs to its owner, not this reference.

## Causality and accounting

Features consume only inputs ready by decision time. Purge labels by actual outcome end across held-out boundaries; fit transformations/calibration only on declared training support. Do not use final outcomes to alter dates, thresholds, features or candidates. Distinguish explanatory Development, model selection and independent/family-specific confirmation.

Reference markets obey causal as-of visibility, not exchange-time joins alone. Separate exchange-observed state from immutable strategy-visible state. Preserve packet/count observability and disclose modeled latency, order ties and fill qualification.

Account independence follows the slice contract, not file boundaries. Within a continuous account retain inventory/orders/pending requests/rolling state/risk through midnight and gaps. Independent slices finish terminal MTM, fees and funding before resetting; MTM is not free liquidation. Missing funding or reliable terminal valuation means incomplete all-in economics, not zero. Do not double count costs.

Behavior-changing candidates follow the quote skill's action-effect chain with side/role support, uncertainty and inventory risk. OPE uses its chosen scorecard/gates; historical probabilities, intervention counts and closed-family thresholds stay in their exact contracts. A closed action identity neither invalidates its entire research family nor permits renaming it to retune consumed evidence.

Negative/no-effect results can complete a task. Never manufacture profitability or repeatedly tune an opened final set to satisfy a generic stopping slogan.

## Training support before economic evaluation

For a new fit, changed training support or expanded economic evaluation, compare the proposed model with the actual baseline's training calendar, eligible rows after label/outcome censoring, class/condition support, temporal coverage, weighting, selection and refit procedure. Reuse located verification when those identities and the intended claim remain unchanged; a comment edit or behavior-preserving repair does not trigger retraining or reconstruction of old labels. A transport/engineering pilot must not silently become a full-study model merely because inference works. If support is demonstrably inadequate or materially narrower than the intended comparison without justification, explain the mismatch and propose an appropriately supported design before substantial compute. Do not impose universal row/day minima or infer adequacy from raw counts alone. An explicitly authorized, labelled narrow pilot is valid; do not use its PnL to screen dates or silently expand the question/data. For requested baseline-method training, inherit verified support/method and change only authorized inputs/targets, with new provenance and no evaluation-outcome tuning. Preserve pilot evidence and disclose outcome-informed owner stopping.
