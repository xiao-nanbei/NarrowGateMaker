# F01: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-23

Last materially synchronized: 2026-09-23

## Questions and entry points

This family owns packages 9 in the [unified plan](../../RECOMPUTE_407.md). The [work list](../../recompute_407.json) defines questions and dependencies; it is an implementation plan, not a completion report.

## Implementation and results

The first bounded, real 48-hour development comparison is complete. The F01 wrapper now requires an explicitly loaded frozen F03/inf 13-head engine and its calibrated P3 identity, creates a separate stateful engine for each arm, and rejects parameter aliases that cannot change the effective quote coefficients. On the same Tardis BTCUSDC input, the newly run wrapper B0 matched the earlier direct F03/inf account in all 997 ordered fill economics, UTC marks and full accounting. Fill-linked predictions, order IDs/times and final quotes also matched. Twelve intermediate floating-point columns differed by no more than approximately 1.02e-10 USDC/BTC; no final quote or account difference was observed.

The predeclared existing gamma-axis point was 0.05 versus B0 gamma 0.046. Both used fresh independent accounts on development shard 073, 2025-12-25 through 27 UTC, with the same model, P3, timing, funding, fees, execution and terminal MTM. All monetary figures are USDC, calculated from full-precision accounts before display:

| Arm | All-in net PnL | Fills | Fees | Real funding | Terminal unrealized PnL | Peak absolute BTC inventory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Frozen F03/inf B0 | -7.7600 | 997 | 0.3460 | -0.0075 | -0.00495 | 0.005 |
| F01 gamma 0.05 | -2.3102 | 1,058 | 0.2831 | -0.0100 | 0 | 0.006 |

The candidate-minus-B0 net difference was +5.4498 USDC: a smaller loss, **not profitability**. Its greater peak absolute inventory failed the risk non-inferiority gate frozen before this account was read. B0 is therefore retained in this first batch; the predeclared conditional shard 074 evaluation was not opened. This account and the F03 base model were previously used in development, so this is not a fresh whole-strategy holdout. The first ordered fill already differs between arms, demonstrating different executed paths, but fill-only traces cannot prove a complete unsubmitted-opportunity or order denominator. Private account and trace evidence is retained in the private evidence store and not distributed here. This single bounded comparison does not close the F01 family or choose a Phase 1 combined strategy.

## Inputs, units and evaluation

Use the shared 407-day source and actual strategy-visible clocks; freeze feature/label/action units in each experiment. Purge by actual outcome ends, keep training/selection out of final evaluation and retain previous-use. Fix common accounts, delays, fill eligibility, fees, funding and terminal MTM. Missing is not zero and an intent is not a fill. Reuse artifacts only when their model/action contracts remain compatible.

## Reproduction and remaining work

Read the [new-input interface guide](../../INPUT_MIGRATION.md). Further F01 points require a separately predeclared candidate/support budget and unchanged common account assumptions; the observed result above cannot be used to pick a favorable date or tune a new point. Intraday maximum drawdown and complete order-opportunity changes were not measured by this fill-linked receipt. Distributed shards preserve account boundaries; forks require complete-state equivalence checks.

## Historical context

Earlier studies explored this family's mechanisms under their original inputs and execution identities. One private source snapshot preserves pre-rewrite documents; existing historical docs are traceability only, not current statuses or permission to reopen final data.
