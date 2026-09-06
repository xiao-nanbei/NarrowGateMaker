# F01 Fixed Parameter Racing

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially synchronized: 2026-09-06

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: closed as an alpha family. The retained runner has screening authority only and uses the canonical paired scorecard path.

Root Python files contain the racing and campaign replay entrypoints; `audit/` contains the paired screening implementation; `docs/` contains the closure and architecture record. Shared dependencies: D, R, S, G.

## Continuous baseline and funding accounting

The existing `campaign_outcome_replay_audit` runner accepts `--continuous` for
contiguous UTC `--days`, with `--engine python --workers 1`. It merges only market
inputs and calls the existing simulator once per arm. The first day retains
causal pre-roll; later days do not reset orders, queue state, cooldown, campaign
or risk state. Non-contiguous dates cannot be treated as a continuous account.
This path does not claim full C++ scheduler qualification or exact historical
live initial-state recovery. External reference/repair window concatenation is
not implemented; those inputs must not be silently dropped.

`--funding-history <frozen-fundingRate-json>` books linear-contract settlement on
each arm's physical exchange-time inventory. The file contains ordered Binance
`symbol`, `fundingTime`, `fundingRate` and settlement `markPrice` records, downloaded
for the complete research interval before execution. The runner never downloads
rates during replay. Positive rates debit longs and credit shorts; settlement is
not a fill. Equal-millisecond settlement precedes fills by explicit model
convention; the output counts ties rather than claiming exchange-exact ordering.

Funding contributes to campaign value and `replay_net_pnl`; legacy `replay_pnl`
remains the simulator's trading PnL after transaction fees. The funding CSV
records each settlement, position and cashflow. This preserves the current live
risk/cooldown basis (trading PnL), rather than changing policy through an
accounting repair. Funding-aware margin or equity-based risk feedback is not
modeled. Missing funding input is reported as `unmodeled`, not verified zero cost.

In continuous mode, each row of the legacy `daily.csv` file represents an entire
segment: `accounting_window=continuous_segment`, `window_end_day` and
`window_day_count` make that scope explicit. It is not a daily observation for
bootstrap or selection. Do not combine its statistics with daily fresh-start
results or reuse an earlier latency environment's baseline.
