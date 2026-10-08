# F07: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-08

Last materially synchronized: 2026-10-08

Current research entry: [B0_RESPONSE action-value closure report](../f08_side_taker_lifecycle/response_baseline.md). Economics and local identity acceptance cover 204 independent accounts / 407 days, with net PnL -1603.031182 USDC, still a loss; this is neither a continuous account nor wholly out-of-sample validation. The report separates the frozen 47-feature model, 39-feature information ablation, one-time Final reuse and live admission gaps. Old-order action choice is not U1 evaluation timing, and does not rewrite historical hazard findings.

## Questions and entry points

This family owns packages 11 in the [unified plan](../../RECOMPUTE_407.md); see the [machine-readable work list](../../recompute_407.json). A plan is not an execution receipt.

## Implementation and new results

This family studies when existing orders are reevaluated, retained, canceled or continued. Existing rules are not trained queue-value models; integration or restoration checks for a particular mechanism do not validate every cancel/ACK/reentry combination. [F05 E/C](../f05_fill_quality_quote_ev/README.md) completed its specified batch; C compares KEEP−CANCEL, not full REENTER. [F01](../f01_fixed_parameter_racing/README.md) owns fixed and dynamic price-threshold comparisons.

## U0/U1: event-driven quote evaluation

U0 is the original B0 strategy in the local quote-compute-time scenario. U1 requests reevaluation after visible-state changes and existing order-lifecycle events, retaining original price thresholds, ordinary/reducing time conditions, pending, risk rules and quote formulas. It coalesces unexecuted quote-computation requests rather than dropping market, fill or order events; in-flight computation neither reenters nor resamples. It is not busy polling with requote_interval set to zero, nor does every market event directly cancel and replace orders.

Complete economic comparisons cover eight matched independent Final accounts. U1 reduced aggregate loss, improved most accounts and had a positive paired median, but both aggregate paths remained negative. Turnover fell substantially while net PnL per turnover worsened; quote evaluations and actual request burden increased, as did absolute inventory-time exposure and peak inventory. Better total PnL alone therefore does not establish better fill selection, lower risk or costless responsiveness.

One account in the batch used an earlier implementation; the others used subsequent scheduling fixes and recording optimizations. This is a completed-batch comparison, not proof of all-account equivalence under one implementation. Final had prior use and is not a new blind test. Available counters distinguish evaluation, coalescing, threshold blocks and actual requests, but request counts are not economically effective cancellations or queue resets and cannot attribute return differences to particular queue changes.

Evaluation timing can materially change the complete path. This batch shows a loss-reduction signal accompanied by substantial request and exposure costs, not stable profitability, universal superiority or promotion to B0/live. U1 trains no new value model and does not complete the original queue-value, REENTER or entire F07 program. Scheduling and performance repairs are supporting engineering, not substitutes for economic conclusions.

## Inputs, units and evaluation

Use the shared 407-day source and strategy-visible clocks. Freeze feature/label/action units, candidate budgets, parent artifacts and splits. Purge by actual outcome ends; training and selection do not read final data, and previous-use remains. Fix initial accounts, delays, fill eligibility, fees, real funding and terminal inventory MTM. Missing is not zero; intents are not fills.

## Reproduction and limitations

Choose actual APIs through the [new-input guide](../../INPUT_MIGRATION.md), without defaulting to old models, dates or rankings. Distinguish not run, missing input, valid negative and valid positive results. Incomplete fork state is not checkpoint equivalence; simulation does not prove native/live fill parity.

## Historical context

Earlier work explored these mechanisms under its original data and execution identities. One private snapshot preserves the pre-rewrite tree; existing historical docs are traceability only, not current closure states or permission to reopen final data.
