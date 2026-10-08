# F05: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-04

Last materially synchronized: 2026-10-04

## Questions and entry points

This family owns packages 7, 8, 11, 12 in the [unified plan](../../RECOMPUTE_407.md); see the [machine-readable work list](../../recompute_407.json). A plan is not an execution receipt.

## Implementation and new results

F05 includes distinct label and action contracts: opportunity-level fill quality, risk widening, paired POST/WAIT and KEEP/CANCEL selection, and Full-Multiscale cooldown experiments. Each experiment's labels, actions, samples, execution scope and economic conclusions are recorded separately; a cooldown result such as `supported_sides=[]` does not summarize the whole family. Opportunity panels, five heads per side and strict model loading are interfaces, not evidence of deployment. `training_binding.py` creates training views within each parent account's actual outcome-end and time support while leaving engineering artifacts unchanged. `economic_candidate.py` connects a training-frozen risk score to the existing quote-widen action for research replay only; it is neither a new net-value estimator nor a live strategy. E/C interfaces do not imply adoption into B0 or live.

`logged_outcomes.produce_logged_outcomes()` consumes a closed producer-audited replay journal and exact creation links. It preserves all declared decision callbacks, non-quote actions, observed partial fills, lifecycle censoring and unknown conditional markouts. WAIT or an unsubmitted quote does not receive a hypothetical no-fill/zero-Quote-EV label. The engineering adapter retains the existing per-fill first-replay-price-at-or-after-target method (including delayed/forward-filled-clock-row limitations), aggregates with BTC quantity weights, and separately names USDC/BTC, bps and gross USDC markout; none is net strategy PnL.

`build_opportunity_panel(..., support_role="engineering_observation")` can check a legal observation interval without assigning it to training. Its output is rejected by `train_opportunity_models()`. Actual F05 label admission, features, training intervals and budgets must be bound before fitting; an F03 development account does not supply those permissions. Raw denominators and filtered engineering panels are separate artifacts. A panel is not an economic evaluation or evidence of profitability.

One real multi-day support batch completed ten two-sided head fits, independent loading and one full development-account candidate comparison. The candidate changed the actual order and fill path, but its all-in net PnL was worse than the matched F03 reference on that account. This negative result is retained without threshold retuning. The batch validates a first bounded economic consumer, not the full calendar, the entire F05 family or the F01–F10 combination. Private identities and ledgers are not distributed; interface tests cannot replace coverage, sample/censoring denominators or prediction-to-action-to-order-to-fill-to-inventory-to-net-PnL evidence.

## Inputs, units and evaluation

The prefill E/C calendar revision `f03-407-t100-final107-v2` uses the maintained F03 calendar through [prefill_calendar.py](prefill_calendar.py), replacing the earlier contiguous 92/14/28-day allocation. [prefill_labels.py](prefill_labels.py) selects the first eligible opportunity per declared UTC-day/surface/slot; the full 30-second outcome must remain strictly inside that day and parent account. T100 alone supplies fitting labels and transforms; A50/B100/C50 supply fixed, interleaved-date diagnostics, not past-only validation. Final107 supplies no supervised labels and retains its previous-use history. The calendar mode in [risk_selection_training.py](risk_selection_training.py) validates actual decision dates and slot identities while retaining the original chronological API.

This v2 batch is complete; see the sanitized completion record in [package 7, prefill_ec](../../recompute_407.json). The 2,048 training and 256 diagnostic pairs are quota ceilings; actual completion is 2,288 valid label pairs, 16 legal empty slots, 132 label-parent accounts and four frozen fits. Empty slots are not zero labels. Ten paths completed 2,040 economic units and 4,070 strategy-days on the same 204 independent accounts; 2,040 is not an account count. Final was evaluated but supplied no supervised labels; full-period totals include training dates, while A/B/C are interleaved-date diagnostics. The initial contiguous plan and its original budget remain historical, not retroactively rewritten as this batch's coverage. Private completion receipts, models and ledgers are not distributed.

E labels POST−WAIT only at baseline-permitted flat opening opportunities. C labels KEEP−CANCEL only for baseline-KEEP, still-valid orders with a visible increasing-risk role. Thirty seconds is the common settlement window after one intervention, not an added cooldown; remaining inventory is valued under the contract. The full policy repeatedly applies selectors, so sums of short labels cannot replace account net PnL. Random controls match rejection probability on the training opportunity stream, not evaluation turnover, inventory exposure or realized rejection rates; two seeds provide limited evidence. EC remains the predeclared main candidate and cannot retrospectively be renamed E. Completion, reduced loss or improved PnL per turnover alone establishes neither stable selectivity nor profitability. E has not replaced B0/live, and no economic admission or automatic follow-up execution is implied.

Use the shared 407-day source and strategy-visible clocks. Freeze feature/label/action units, candidate budgets, parent artifacts and splits. Purge by actual outcome ends; training and selection do not read final data, and previous-use remains. Fix initial accounts, delays, fill eligibility, fees, real funding and terminal inventory MTM. Missing is not zero; intents are not fills.

## Economic conclusions from E/C

All paths remained negative in both full-period and Final aggregates. Model E and EC reduced total loss relative to B0, but C alone and the addition of C to E changed direction across evaluation scopes, providing no consistent incremental benefit. EC retains its predeclared main-candidate identity rather than being replaced by the more favorable E result.

Random E/EC controls lost less in total while trading substantially less; this is not a turnover-matched comparison. Model E had slightly better net PnL per turnover than the two random E controls, yet its Final per-turnover result remained worse than B0. Reduced total loss therefore cannot be attributed directly to selection quality, nor can the model be reduced to random trade suppression. Complete paths were evaluated, but stable selectivity and profitability were not established; two random seeds do not characterize the control distribution. Training dates in full-period totals, interleaved diagnostics and previously viewed Final remain limitations.

F05 owns the E/C labels, fits and policy comparison. C's existing-order lifecycle semantics are cross-referenced from [F07](../f07_active_order_continuation/README.md), without expanding CANCEL into completed REENTER research.

## Reproduction and limitations

Choose actual APIs through the [new-input guide](../../INPUT_MIGRATION.md), without defaulting to old models, dates or rankings. Distinguish not run, missing input, valid negative and valid positive results. Incomplete fork state is not checkpoint equivalence; simulation does not prove native/live fill parity.

## Historical context

Earlier work explored these mechanisms under its original data and execution identities. One private snapshot preserves the pre-rewrite tree; existing historical docs are traceability only, not current closure states or permission to reopen final data.
