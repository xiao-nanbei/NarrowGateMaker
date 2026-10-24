# Fill Inventory Lifecycle Audit

> Current status (2026-07-27): the lifecycle estimand, censoring treatment and distinction between short-horizon markout and inventory lifecycle value remain valid. The Retained111 counts, FIFO/LIFO durations, survival percentages and exact PnL below are superseded because their order/fill denominator used the former mixed L2 and operational queue path. They must not be quoted as current calibration or promotion evidence. Current work must rebuild the denominator under normalized 100ms L2, repaired trade-side data, the merged clock and an explicitly bound queue identity.

## Why this audit exists

A fixed 30-second maker-signed markout measures early adverse selection. It does not measure how long the inventory created by that fill remains on the book. The research denominator must separate:

1. micro markout at short, pre-declared horizons;
2. the lifetime of the inventory lot created by an opener/add fill;
3. the time until the containing inventory lifecycle returns to flat; and
4. censored open inventory at the replay boundary.

Inventory is fungible, so a reducing fill does not identify which historical add fill it closes. The audit therefore reports both FIFO and LIFO attribution. The difference is model uncertainty, not exchange-observed truth.

## Pending current-source lifecycle question

The old counts, duration and survival table have been removed from current presentation. `F05-MARKOUT-HOLDING-LIFECYCLE` in the [F05 question register](../../f05_fill_quality_quote_ev/research_questions.json) asks how markout horizons should relate to causally estimable holding lifetimes. [F10 attribution](../research_questions.json) owns the lot/cycle mapping. Neither a fixed 30 seconds nor a fixed percentage of an eventual realized lifetime is established as optimal. Use declared horizon curves and censoring-aware lifecycle outcomes; do not feed a future realized close time into a decision-time feature.

## Consequence for labels

`markout_30s` remains useful, but only as an early toxicity label. It must not be called fill PnL or lifecycle value. A maker action should be evaluated with a vector of outcomes:

```text
micro:       1s / 5s / 20s / 30s maker-signed markout
lot:         MTM or realized value at attributed lot close
inventory lifecycle:    terminal MTM/PnL, MAE, repair, duration, tail
censoring:   open lot/inventory_lifecycle at replay boundary
execution:   fill probability and queue/reset cost
```

For prediction, the economically aligned target should use survival or competing-risk methods rather than selecting one global second value. For action uplift, the reward must retain the terminal inventory lifecycle component and must not duplicate one inventory lifecycle outcome across multiple intervention rows.

For closed lots, the audit also pairs the passive opening fill with the attributed passive reducing fill and records realized `lot_pnl`. For censored lots it deliberately leaves `lot_pnl` missing; a separate terminal MTM label is required, and no hypothetical taker close fee is deducted.

Artifacts:

- `backtest_results_btcusdc/fill_inventory_lifecycle_retained111_20260713.lots.csv`
- `backtest_results_btcusdc/fill_inventory_lifecycle_retained111_20260713.summary.csv`
- audit entry point: `models/audit/fill_inventory_lifecycle.py`
