# F02: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-17

Last materially synchronized: 2026-10-17

## Questions and entry points

This family owns packages 3, 6 in the [unified plan](../../RECOMPUTE_407.md); see the [machine-readable work list](../../recompute_407.json). A plan is not an execution receipt.

## Open questions

The [family question register](research_questions.json), linked from the [family registry](../../registry.json), exposes `status=pending_validation` records: `F02-SCALAR-ADAPTER-CURRENT-SOURCE`. Old numbers remain historical observations in their original reports, not current model rankings. Unverifiable provenance and methods requiring reconstruction are explicit evidence gaps, not negative findings. This register does not start a replay or change an active experiment.

## Implementation and new results

Static P3 has a new train-only input adapter; conditional and reach-time surfaces still need integration.

Full new-study results are not delivered here; private evidence is not distributed. Interface tests cannot replace coverage, sample/censoring denominators or prediction-to-action-to-order-to-fill-to-inventory-to-net-PnL evidence.

## Inputs, units and evaluation

Use the shared 407-day source and strategy-visible clocks. Freeze feature/label/action units, declared candidates, parent artifacts and splits. Backtest counts are not capped; complete the evaluations needed by the declared question. Purge by actual outcome ends; training and selection do not read final data, and previous-use remains. Fix initial accounts, delays, fill eligibility, fees, real funding and terminal inventory MTM. Missing is not zero; intents are not fills.

## Reproduction and limitations

P3 distance is measured outward from the same-side BBO, not as half the total quote spread. With best bid 99 and best ask 101, quotes 98 / 102 have a total spread of 4 but a same-side distance of only 1 on each side; distances of 2 require boundary quotes 97 / 103. This synthetic example distinguishes constraints, not economic performance or implementation acceptance.

![Synthetic comparison of same-side BBO distance and total quote spread; Chinese labels](../../../docs/images/orderbook/orderbook_p3_same_side_bbo_distance.png)

*Mechanism illustration / synthetic values / not live or backtest results. A total-spread floor does not establish a per-side BBO-distance floor.*

Choose actual APIs through the [new-input guide](../../INPUT_MIGRATION.md), without defaulting to old models, dates or rankings. Distinguish not run, missing input, valid negative and valid positive results. Incomplete fork state is not checkpoint equivalence; simulation does not prove native/live fill parity.

## Authorized scalar-adapter redo: method audit

The October 17 owner request reopens the August scalar-adapter question on the current source and B0; it does not reuse the old economic result as a current finding. The original 24-day panel combined nine previously inspected Validation-OOF days with fifteen later diagnostic days, and reused existing conditional models rather than fitting a new current-source bundle. Its symmetric side aggregation and the mapping from a ten-second touch probability to quote-floor/intensity controls are substantive modeling assumptions: touch is not queue-conditioned fill intensity or net value. A negative result alone does not prove an implementation defect.

The redo must bind current inputs, train-only feature/calibration support, actual outcome-end purging and the current B0 before fitting or replay. First reproduce the declared scalar mapping under that new identity; do not silently replace it with a side-specific strategy or tune on the old loss. Report signal, quote, fill, inventory and net-accounting changes separately. The historical audit is complete; new input/model binding and new economic evaluation are not. Retire the old experiment payload only after the new accepted result and its retained dependencies are verified, as requested by the owner.

## Historical context

Earlier work explored these mechanisms under its original data and execution identities. One private snapshot preserves the pre-rewrite tree; existing historical docs are traceability only, not current closure states or permission to reopen final data.

## Open questions and accepted evidence

Historical claims are registered by question in the [family question index](research_questions.json). Follow `research_questions` from the root family registry to discover pending work. Registration does not execute a study or withdraw accepted current-source results or engineering checks; the results below retain their own source, baseline and coverage.
