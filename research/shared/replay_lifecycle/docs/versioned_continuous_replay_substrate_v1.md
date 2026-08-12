# Versioned Continuous Replay Substrate v1

Last materially modified: 2026-08-03

## Purpose

This shared substrate gives F03, F05, F09, and F10 one versioned definition of calendar time, restart boundaries, state carry, and continuous PnL accounting. It does not define a strategy action or promote data quality.

The frozen calendar envelope is 2026-04-17 through 2026-07-30. It contains the existing 40-day F09 Development panel as immutable anchor days. The dedicated anchor comparison spans the first through last anchor day (2026-04-17 through 2026-06-26); expanded modes retain the full calendar envelope.

## State Contract

The following state survives UTC midnight and planned process restart:

- cash and position quantity;
- average entry price and cumulative fees;
- cumulative realized and marked PnL;
- economic campaign identity and peak inventory.

Before an offline interval, new quotes stop and all live orders must reach an observable terminal event. The following process-local state is then cleared:

- active and cancel-pending orders;
- queue position, order age, and order cursors;
- q90 active-order hazard cursor;
- runtime campaign, cooldown, EMA, and signal state.

Restart requires a fresh book snapshot and past-only feature warmup before quoting can resume. Inventory remains economically exposed while the strategy is offline and is marked through the gap.

## Replay Modes

| Mode | Use | Authority |
|---|---|---|
| `anchor_panel_continuous` | Comparable continuous path over the frozen 40 anchor days | Continuity sensitivity only |
| `native_strict_continuous` | Grade-A native L2 intervals | Exact lifecycle/queue only after authoritative runner binding |
| `restart_aware_calendar` | A/B/C intervals with planned offline gaps | Continuous PnL, inventory, and campaign sensitivity |
| `provider_normalized_continuous` | Tardis provider-local normalized intervals | Prediction/source sensitivity; never exact queue |

Provider-normalized Tardis artifacts preserve provider-local visibility and do not claim AWS Tokyo receive time or native Binance sequence authority.

## Accounting Identity

For a state that is carried across midnight:

\[
\mathrm{PnL}_d=
\mathrm{equity}_{d,\mathrm{end}}-
\mathrm{equity}_{d,\mathrm{start}},
\qquad
\mathrm{equity}=\mathrm{cash}+q\,m.
\]

Daily slices must add to the continuous equity change. The final open inventory is marked; it is never dropped because a campaign has not closed.

## Identification Boundary

Comparing continuous replay with daily fresh starts identifies the effect of state continuity, planned restarts, and accounting semantics. It does not prove that tail-inventory governance works.

Tail-governance evidence requires governance ON and OFF on the same continuous timeline, restart schedule, warmup, and market tape:

\[
\Delta_{\mathrm{governance}}=
\mathrm{PnL}_{\mathrm{continuous,on}}-
\mathrm{PnL}_{\mathrm{continuous,off}}.
\]

Statistical support still requires the family-specific clustered lower bound and tail gates. This substrate cannot grant action or live authority.

## Current Status

The state, restart, accounting, calendar, and Tardis coverage contracts have unit coverage. The authoritative multi-day `backtest_tick.py` binding remains fail-closed, so no continuous-path PnL result has yet been produced by v1.
