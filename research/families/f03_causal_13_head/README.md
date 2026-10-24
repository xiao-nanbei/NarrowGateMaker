# F03: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-17

Last materially synchronized: 2026-10-17

See the [dimensional review map](units_review.md) for producer-to-consumer checks. It is an audit checklist, not a claim that all unit conversions are already verified.

## Questions and entry points

This family owns packages 5 in the [unified plan](../../RECOMPUTE_407.md); see the [machine-readable work list](../../recompute_407.json). A plan is not an execution receipt.

## Completed five-candidate Final107 comparison

As of 2026-10-16 UTC, all five candidates have full economic acceptance for 54/54 independent accounts and 107/107 dates: 270 new candidate accounts, reused original same-contract B0 results, no new controls or continuous accounts. The common calendar is UTC [2026-05-28, 2026-09-12), with 53 two-day accounts and one one-day account per candidate. These are sums over reset accounts, not continuous-account returns. Unit PnL divides each arm's total net PnL by its own total traded volume/notional, not the mean of account ratios. Exact aggregates are in the [machine-readable record](../../final107_comparison.json).

| Configuration | Net PnL USDC | Delta vs B0 USDC | USDC per 0.001 BTC | USDC per 10,000 USDC notional | Better/worse accounts |
| --- | ---: | ---: | ---: | ---: | ---: |
| B0 — DEFENSE_ON__URGENCY_OFF | -157.501448 | 0 | -0.003636691 | -0.530644 | — |
| A — Original100, joint direction; twelve original B0 heads | -159.670159 | -2.168711 | -0.003719747 | -0.542696 | 26/28 |
| B — Development300, daily weights, single market | -180.979750 | -23.478302 | -0.004422014 | -0.644098 | 23/31 |
| C — Development300, equal valid labels, single market | -168.924291 | -11.422843 | -0.004314136 | -0.628495 | 25/29 |
| D — Original100, all-head equal labels, joint direction | -177.477944 | -19.976497 | -0.004298328 | -0.626426 | 23/31 |
| E — Original100, all-head equal labels, single market | -198.350727 | -40.849279 | -0.004811535 | -0.700320 | 21/33 |

Every candidate is worse than B0 in total and on both unit-PnL measures; none is promoted or activated live. Passing a training contract does not guarantee better trading, and this result does not prove the old weighting universally superior. B/C jointly change date and class adjustment, not an isolated date effect; D/E differ only in direction inputs/model, with identical other-twelve-head bytes and P3; C/D also change training support. Every 100-day candidate uses the original B0's exact dates, not the most recent consecutive 100 days. B/C use all Development dates for training. Candidate additions followed partial Final inspection, so Final is not fresh holdout. No significance, equivalence, future-profitability guarantee or new time-decay experiment is established.

All five original archive manifests reached `verified_minimal`, with economic status `closed_complete`, an independent final check and deletion of the inspection automation. Formal results, accounting, original task/exit receipts, source/model/input/B0 bindings, unique failures and shared/pinned recovery dependencies remain retained. Only redundant working copies passed the retained-copy, consumer, recovery and open-file gates before removal. This later archival closure does not rewrite the frozen economic reports or rerun economic acceptance, and does not certify other families or historical archives. Detailed ledgers, models, purchased data and acceptance receipts remain in the private evidence store, not distributed with the repository; this aggregate is not a complete reproducibility bundle.

## Authorized Development300 extension

The owner separately authorized retraining all thirteen current-B0 BTCUSDC heads on the complete Development300 calendar, UTC [2025-08-01, 2026-05-28). This is one single-market 29-feature bundle, not a 300-day joint-market direction model. [development300.py](development300.py) preserves the original label/P3 and per-head effective-row weighting method: each supported day has equal total weight before classification imbalance adjustment, H=inf has no age decay, and early stopping is unweighted. The chronological split expands to 239 fit days, one embargo day and 60 selection days, followed by all-300 refit at each head's selected iteration. There is no new half-life search, P3 recalibration, Final training or strategy change.

All 300 label days, thirteen selection fits and thirteen full refits have passed model/support acceptance; all 54 Final accounts have completed full economic acceptance after real save/reload, input and immutable-runtime checks. The original 100 label days were reused. The bundle is compared alongside the unchanged [F04 100-day joint-direction candidate](../f04_external_market_alpha/README.md), reusing accepted same-contract urgency-off B0 controls. No extra continuous or duplicate control path is authorized. The new candidate was added after viewing partial Final outcomes; Final is not fresh holdout, and all former Development T/A/B/C dates are now training for this bundle. Historical four-bundle selection and results below retain their original identity; they do not select or validate this extension.

## Authorized valid-label-uniform comparison

The owner authorized one additional complete 300-day thirteen-head bundle, implemented in [label_equal_training.py](label_equal_training.py). The actual B0 and the preceding 300-day bundle both normalize each supported day's total loss weight before classification adjustment. A head/day with only one eligible label can therefore assign that observation disproportionate weight. This is a choice between day-average and label-average objectives, not evidence that changing weights will improve trading.

The additional candidate gives every eligible label within each head weight 1 and disables classification imbalance reweighting. It inherits the exact accepted panel, per-head missing/outcome-end masks, 239/1/60 selection split, unweighted early stopping, full-300 refit, features, P3 and other hyperparameters. It changes both date and class reweighting, so it does not isolate the date-weight component. No new label production, half-life or weight search is authorized. All thirteen selection fits and full refits, support/model acceptance and the prespecified real save/recovery comparison have passed. All individually admitted accounts have completed through the existing PoolRun. [label_equal_final.py](label_equal_final.py) identifies the fixed 54 independent Final accounts; all 54 accounts and 107 dates have passed full economic acceptance. No new continuous or control accounts are added. Existing candidates and results are unchanged. The owner confirmed this design after partial Final inspection; Final remains previously used, not fresh holdout.

The method distinguishes the estimand before choosing weights, following the general distinction between cluster-average and individual-average targets in [Benitez et al.](https://arxiv.org/abs/2110.09633); this is an analogy, not a claim that financial dates satisfy a randomized-trial design. [Cui et al.'s class-balanced loss](https://openaccess.thecvf.com/content_CVPR_2019/html/Cui_Class-Balanced_Loss_Based_on_Effective_Number_of_Samples_CVPR_2019_paper.html) addresses class imbalance, not equal weighting of sparse dates. [LightGBM's parameter documentation](https://lightgbm.readthedocs.io/en/latest/Parameters.html#is_unbalance) warns that class reweighting can impair probability estimates. Neither source guarantees economic gains; label dependence, calibration, actual actions and complete account results still require evaluation. Exact plans, weight/support diagnostics, models and process receipts remain private evidence, not distributed with this repository.

## Original-B0 100-day valid-label-uniform comparison

The owner also authorized a BTCUSDC-only thirteen-head bundle using the exact original B0's 100 training dates, not the most recent 100 consecutive days. [label_equal100_training.py](label_equal100_training.py) retains the original 79/1/20 fit/embargo/selection split, per-head eligibility masks, unweighted early stopping and full-100 refit. Every eligible label has weight 1; date normalization, classification rebalance and time decay are disabled. All thirteen heads use BTCUSDC29, with the original P3, strategy and economic consumers unchanged.

Twelve already accepted equal-label heads from the [100-day all-head joint-direction candidate](../f04_external_market_alpha/README.md) have exactly matching dates, masks and parameters and are reused with their original bytes and provenance. Only the missing BTCUSDC-only 10-second direction head requires a new selection fit and refit. These are not the old B0's weighted twelve heads. All thirteen model/support checks and the real saved-recovery comparison have passed. All 54 admitted accounts have completed through the existing PoolRun with formal/durable acceptance. [label_equal100_final.py](label_equal100_final.py) binds the same 54 independent Final accounts, with individual input admission and no predecessor-model completion wait; full economic results and minimal archival closure are complete. No additional control or continuous paths are created.

Against the 100-day all-head joint-direction candidate, only the direction head's inputs and fitted model differ. Against B0, both date and class weighting change; against the 300-day equal-label candidate, training support also differs. This outcome-informed addition does not restore Final to fresh holdout. Exact source/model/reuse bindings and results are private evidence, not distributed with this repository.

## Historical preface: why rebuild?

The v4 test/economic branch was withdrawn after future-metrics leakage: invalid evidence differs from a valid negative result. Corrected v5 and v9 retained predictive diagnostics but did not establish stable maker gains. Source-aware v12 and its one-second successor showed why clearer features or faster inference do not automatically improve order lifecycles: replacements can lose queue priority, reduce fills and prolong inventory exposure. These findings belong to their original samples and assumptions, not all future machine learning.

The reusable lesson is to distinguish prediction loss, actual actions and complete account equity. New data requires new calibration, labels and model identity; renaming an old directory is not migration. Historical evidence informs this preface but is not a current model default.

## New feature and label contract

This F03 uses BTCUSDC only over the declared 407-date source calendar. BTCUSDT processing and mirroring are separate work. Models consume 29 named predictors from the shared 30-field execution protocol, excluding unavailable native packet count. Unknown inputs and validity masks are preserved. Historical individual trade counts require individual trade input in live, not invented children reconstructed from aggregate packets.

Source time, modeled delivery and feature readiness are separate. Decisions use ready observations; independently produced outcome Bars are offline label inputs, never future feature information. Source-time proxies and sampled delays remain modeling assumptions, not native timing or queue parity.

The thirteen heads are direction, return and volatility at 10/30/60 seconds, plus bid and ask toxicity at 5/10 seconds. Their inherited quote/touch-conditioned meaning is not an unconditional future return or guaranteed queue fill. Each head is purged using its actual outcome end, not one fixed-horizon mask. Missing future markouts remain unknown rather than safe/non-toxic zero labels.

P3 was fitted once from all 100 declared training days and frozen before labels and training. It models touch probability using absolute price distance, not queue-adjusted fill probability. Four complete bundles, H=inf/240/120/60, contain 52 trained heads with source, label, calibration, split and training identity bindings. Frozen model bytes remain unchanged.

## Selection, control and accounting

Only the 100 B days' net PnL selected one whole bundle from the four H candidates. A/C/T amounts did not rank models; ties favor inf and then the longer half-life. ML-OFF is a new-chain control, not a fifth candidate or old B0. Development comprises 150 independent two-day accounts per arm. Final accepts only the frozen winner and ML-OFF, plus a separate one-day tail.

Within each shard the account is continuous; between shards it resets to the same declared initial state. Feature warmup is not account carryover. Common input, P3, delays, execution assumptions and accounting remain fixed. ML-OFF loads no model and uses market-Bar rolling variance. The replay bridge exports five prediction fields: 10-second direction, volatility, return and bid/ask toxicity. Training thirteen heads does not prove thirteen separate economic contributions.

Net equity includes trade cash flows, fees, signed real funding and terminal inventory MTM. Marking inventory does not assume free liquidation. Missing funding or valuation cannot be reported as complete all-in PnL.

## Implementation and new results

All 858/858 strategy-shards are complete: 750 Development executions (four models plus ML-OFF) and 108 Final executions. Each Final arm contains 53 independent two-day accounts and one one-day account, covering the same 107 dates from 2026-05-28 through 2026-09-11. Every accounting receipt's economic_complete and daily-audit hash binding was checked; daily equity changes reconcile to shard net PnL within 0.000001 USDC. See the [machine-readable aggregate](results_20260922.json).

| Final metric (USDC) | H=inf | ML-OFF |
| --- | ---: | ---: |
| All-in net PnL | −189.212148 | −269.042233 |
| Fees paid (already in net PnL) | 12.188537 | 10.601817 |
| Signed funding cash flow (already in net PnL) | +0.041388 | +0.021085 |
| Last-day net PnL | −6.636594 | −7.103006 |

The paired sum difference is +79.830084 USDC. Both arms lost money; relative loss reduction is not profitability validation. No significance or annualized return is claimed. These are sums of independently reset accounts, each starting with 10,000 USDC, not a continuous-account return. B alone selected H=inf under the existing rule; Development totals include training-period diagnostics and cannot replace B ranking.

The missing boundary funding was resolved: Binance's actual record is at 2026-09-12 00:00:00.001 UTC, not exact midnight. Original millisecond timestamps are preserved. Under the frozen start < settlement <= end rule this settlement is after the tail account end and is not moved into it. The boundary-day file supports coverage validation; neither zero substitution nor timestamp rounding is used.

After viewing partial Final outcomes, the owner paused and then authorized real funding completion and both tail arms. Previous-use, the addition of ML-OFF after viewing one Development shard, and inspection of interim Final remain disclosed; completion does not make F a fresh holdout. Raw ledgers, models, purchased data and execution receipts are private evidence, not distributed. F03 completion does not mean the 407-day BTCUSDT content/cross-day acceptance or full mirror is complete, nor does it complete other families or establish live activation.

## Inputs, units and evaluation

Use the shared 407-day source and strategy-visible clocks. Freeze feature/label/action units, declared candidates, parent artifacts and splits. Backtest counts are not capped; complete the evaluations needed by the declared question. Purge by actual outcome ends; training and selection do not read final data, and previous-use remains. Fix initial accounts, delays, fill eligibility, fees, real funding and terminal inventory MTM. Missing is not zero; intents are not fills.

## Reproduction and limitations

Choose actual APIs through the [new-input guide](../../INPUT_MIGRATION.md), without defaulting to old models, dates or rankings. Distinguish not run, missing input, valid negative and valid positive results. Incomplete fork state is not checkpoint equivalence; simulation does not prove native/live fill parity.

## Historical context

Earlier work explored these mechanisms under its original data and execution identities. One private snapshot preserves the pre-rewrite tree; existing historical docs are traceability only, not current closure states or permission to reopen final data.

### Retired causal-v3 feature bundle: documented time leakage

The old causal-v3 feature bundle exposed five-minute futures metrics at interval start on three July dates rather than interval end. Its affected prediction/economic conclusions are not current evidence. This remains a specific engineering defect, not proof that all old data or the new Tardis models fail; see the [source-alignment record](../../../docs/marketdata_good_day_alignment_20260720.md).

## Open questions and accepted evidence

Historical claims are registered by question in the [family question index](research_questions.json). Follow `research_questions` from the root family registry to discover pending work. Registration does not execute a study or withdraw accepted current-source results or engineering checks; the results below retain their own source, baseline and coverage.
