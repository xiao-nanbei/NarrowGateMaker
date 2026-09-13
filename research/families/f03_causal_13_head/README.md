# F03: new 407-day research

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

See the [dimensional review map](units_review.md) for producer-to-consumer checks. It is an audit checklist, not a claim that all unit conversions are already verified.

## Questions and entry points

This family owns packages 5 in the [unified plan](../../RECOMPUTE_407.md); see the [machine-readable work list](../../recompute_407.json). A plan is not an execution receipt.

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

Use the shared 407-day source and strategy-visible clocks. Freeze feature/label/action units, candidate budgets, parent artifacts and splits. Purge by actual outcome ends; training and selection do not read final data, and previous-use remains. Fix initial accounts, delays, fill eligibility, fees, real funding and terminal inventory MTM. Missing is not zero; intents are not fills.

## Reproduction and limitations

Choose actual APIs through the [new-input guide](../../INPUT_MIGRATION.md), without defaulting to old models, dates or rankings. Distinguish not run, missing input, valid negative and valid positive results. Incomplete fork state is not checkpoint equivalence; simulation does not prove native/live fill parity.

## Historical context

Earlier work explored these mechanisms under its original data and execution identities. One private snapshot preserves the pre-rewrite tree; existing historical docs are traceability only, not current closure states or permission to reopen final data.
